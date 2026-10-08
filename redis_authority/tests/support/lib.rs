//! A valkey server per test: its own port, its own directory, killed when
//! dropped. The binary comes from Bazel's runfiles, so a test needs no
//! service on the host.

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

const SERVER_PATH: Option<&str> = option_env!("VALKEY_SERVER");
const START_TIMEOUT: Duration = Duration::from_secs(5);
const ADMIN_TIMEOUT: Duration = Duration::from_secs(5);

pub enum ServerMode {
    Standalone,
    Cluster,
    /// A standalone server where `user` may run only the adapter's commands
    /// on keys matching `key_pattern`, as the adapter's `url` connects.
    StandaloneWithAcl {
        user: String,
        password: String,
        key_pattern: String,
    },
}

pub struct ValkeyServer {
    port: u16,
    dir: tempfile::TempDir,
    mode: ServerMode,
    assign_slots: bool,
    process: Mutex<Option<Child>>,
}

impl ValkeyServer {
    pub fn start(mode: ServerMode) -> Self {
        Self::launch_new(mode, true)
    }

    pub fn url(&self) -> String {
        match &self.mode {
            ServerMode::StandaloneWithAcl { user, password, .. } => {
                format!("redis://{user}:{password}@127.0.0.1:{}/", self.port)
            }
            _ => format!("redis://127.0.0.1:{}/", self.port),
        }
    }

    pub fn flushall(&self) {
        self.admin(&["FLUSHALL"]);
    }

    /// Saves the data to disk and stops; `restart` brings it back with that data.
    pub fn shutdown_save(&self) {
        // A server that shuts down closes the connection without a reply.
        let reply = redis::cmd("SHUTDOWN")
            .arg("SAVE")
            .exec(&mut self.admin_connection());
        let mut process = self.process.lock().unwrap();
        let child = process.as_mut().expect("running");
        let started = Instant::now();
        let stopped = loop {
            if reply.is_ok() {
                break false;
            }
            if child.try_wait().expect("valkey status").is_some() {
                break true;
            }
            if started.elapsed() >= START_TIMEOUT {
                break false;
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        if !stopped {
            let _ = child.kill();
            let _ = child.wait();
            *process = None;
            panic!(
                "SHUTDOWN SAVE did not stop the server (reply {reply:?}); see {}",
                self.log().display()
            );
        }
        *process = None;
    }

    pub fn restart(&self) {
        self.launch();
    }

    /// Stops answering every client for `duration`.
    pub fn pause_clients(&self, duration: Duration) {
        self.admin(&["CLIENT", "PAUSE", &duration.as_millis().to_string(), "ALL"]);
    }

    /// A second, empty node joined to this single-node cluster.
    pub fn add_cluster_node(&self) -> ValkeyServer {
        let node = Self::launch_new(ServerMode::Cluster, false);
        node.admin(&["CLUSTER", "MEET", "127.0.0.1", &self.port.to_string()]);
        self.wait_until("the cluster to hold two nodes", || {
            [self, &node].iter().all(|n| {
                let info = n.cluster_info();
                info.contains("cluster_known_nodes:2") && info.contains("cluster_state:ok")
            })
        });
        node
    }

    /// Moves the slot of `key`, keys and all, from this node to `to`.
    pub fn move_slot_of(&self, key: &str, to: &ValkeyServer) {
        let slot: u16 = self.query(&["CLUSTER", "KEYSLOT", key]);
        let slot = slot.to_string();
        let from_id: String = self.query(&["CLUSTER", "MYID"]);
        let to_id: String = to.query(&["CLUSTER", "MYID"]);
        to.admin(&["CLUSTER", "SETSLOT", &slot, "IMPORTING", &from_id]);
        self.admin(&["CLUSTER", "SETSLOT", &slot, "MIGRATING", &to_id]);
        let keys: Vec<String> = self.query(&["CLUSTER", "GETKEYSINSLOT", &slot, "1000"]);
        if !keys.is_empty() {
            let port = to.port.to_string();
            let mut migrate = vec!["MIGRATE", "127.0.0.1", &port, "", "0", "5000", "KEYS"];
            migrate.extend(keys.iter().map(String::as_str));
            self.admin(&migrate);
        }
        for node in [to, self] {
            node.admin(&["CLUSTER", "SETSLOT", &slot, "NODE", &to_id]);
        }
    }

    /// A port picked here may be taken before the server binds it (another
    /// test's server, a client's own ephemeral port, a cluster bus port), in
    /// which case the server exits at once: start again on another port.
    fn launch_new(mode: ServerMode, assign_slots: bool) -> Self {
        let mut last = String::new();
        for _ in 0..8 {
            let port = free_port(matches!(mode, ServerMode::Cluster));
            let base = std::env::var_os("TEST_TMPDIR")
                .map(PathBuf::from)
                .unwrap_or_else(std::env::temp_dir);
            let dir = tempfile::Builder::new()
                .prefix("valkey-")
                .tempdir_in(base)
                .expect("a directory for the server");
            let server = Self {
                port,
                dir,
                mode: match &mode {
                    ServerMode::Standalone => ServerMode::Standalone,
                    ServerMode::Cluster => ServerMode::Cluster,
                    ServerMode::StandaloneWithAcl {
                        user,
                        password,
                        key_pattern,
                    } => ServerMode::StandaloneWithAcl {
                        user: user.clone(),
                        password: password.clone(),
                        key_pattern: key_pattern.clone(),
                    },
                },
                assign_slots,
                process: Mutex::new(None),
            };
            match server.start_process() {
                Ok(()) => {
                    server.configure();
                    return server;
                }
                Err(error) => last = error,
            }
        }
        panic!("no valkey server could start: {last}");
    }

    fn log(&self) -> PathBuf {
        self.dir.path().join("valkey.log")
    }

    fn launch(&self) {
        self.start_process().unwrap_or_else(|error| panic!("{error}"));
        self.configure();
    }

    /// Starts the server process and returns once that very process answers.
    fn start_process(&self) -> Result<(), String> {
        let relative = SERVER_PATH.expect("the valkey binary is known only to Bazel builds: run through Bazel");
        let runfiles = std::env::var_os("TEST_SRCDIR").expect("TEST_SRCDIR: run through Bazel");
        let binary = PathBuf::from(runfiles).join(relative);
        let mut command = Command::new(&binary);
        command
            .args(["--port", &self.port.to_string(), "--bind", "127.0.0.1"])
            .arg("--dir")
            .arg(self.dir.path())
            .args(["--save", "", "--appendonly", "no", "--protected-mode", "no"])
            .arg("--logfile")
            .arg(self.log());
        if matches!(self.mode, ServerMode::Cluster) {
            command.args([
                "--cluster-enabled",
                "yes",
                "--cluster-config-file",
                "nodes.conf",
                "--cluster-node-timeout",
                "1000",
            ]);
        }
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|error| panic!("cannot start {}: {error}", binary.display()));
        let pid = child.id();
        *self.process.lock().unwrap() = Some(child);
        let started = Instant::now();
        loop {
            if let Some(status) = self.process.lock().unwrap().as_mut().and_then(|c| c.try_wait().ok().flatten()) {
                return Err(format!("valkey exited at start ({status}); see {}", self.log().display()));
            }
            // A foreign server on the port also answers PING: the process id says whose it is.
            let ours = self.try_admin_connection().is_some_and(|mut connection| {
                redis::cmd("INFO")
                    .arg("server")
                    .query::<String>(&mut connection)
                    .is_ok_and(|info| info.contains(&format!("process_id:{pid}\r\n")))
            });
            if ours {
                return Ok(());
            }
            if started.elapsed() >= START_TIMEOUT {
                return Err(format!("valkey did not answer in time; see {}", self.log().display()));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Per-mode setup once the process answers.
    fn configure(&self) {
        match &self.mode {
            ServerMode::Standalone => {}
            ServerMode::Cluster => {
                if self.assign_slots && self.cluster_info().contains("cluster_slots_assigned:0") {
                    self.admin(&["CLUSTER", "ADDSLOTSRANGE", "0", "16383"]);
                }
                if self.assign_slots {
                    self.wait_until("the cluster to be ok", || {
                        self.cluster_info().contains("cluster_state:ok")
                    });
                }
            }
            ServerMode::StandaloneWithAcl {
                user,
                password,
                key_pattern,
            } => {
                let mut rule = vec![
                    "ACL".to_string(),
                    "SETUSER".into(),
                    user.clone(),
                    "on".into(),
                    format!(">{password}"),
                    "resetkeys".into(),
                    format!("~{key_pattern}"),
                    "resetchannels".into(),
                    "-@all".into(),
                ];
                rule.extend(
                    kabudachi_redis_authority::COMMANDS
                        .iter()
                        .map(|command| format!("+{command}")),
                );
                self.admin(&rule.iter().map(String::as_str).collect::<Vec<_>>());
            }
        }
    }

    fn wait_until(&self, what: &str, mut ready: impl FnMut() -> bool) {
        let started = Instant::now();
        while !ready() {
            assert!(
                started.elapsed() < START_TIMEOUT,
                "timed out waiting for {what}; see {}",
                self.log().display()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn try_admin_connection(&self) -> Option<redis::Connection> {
        let client = redis::Client::open(format!("redis://127.0.0.1:{}/", self.port)).ok()?;
        let connection = client.get_connection_with_timeout(ADMIN_TIMEOUT).ok()?;
        connection.set_read_timeout(Some(ADMIN_TIMEOUT)).ok()?;
        connection.set_write_timeout(Some(ADMIN_TIMEOUT)).ok()?;
        Some(connection)
    }

    fn admin_connection(&self) -> redis::Connection {
        self.try_admin_connection()
            .unwrap_or_else(|| panic!("cannot reach the server; see {}", self.log().display()))
    }

    fn admin(&self, args: &[&str]) {
        let mut command = redis::cmd(args[0]);
        for arg in &args[1..] {
            command.arg(*arg);
        }
        if let Err(error) = command.exec(&mut self.admin_connection()) {
            panic!("{args:?} failed: {error}; see {}", self.log().display());
        }
    }

    fn query<T: redis::FromRedisValue>(&self, args: &[&str]) -> T {
        let mut command = redis::cmd(args[0]);
        for arg in &args[1..] {
            command.arg(*arg);
        }
        command
            .query(&mut self.admin_connection())
            .unwrap_or_else(|error| {
                panic!("{args:?} failed: {error}; see {}", self.log().display())
            })
    }

    fn cluster_info(&self) -> String {
        self.try_admin_connection()
            .and_then(|mut connection| {
                redis::cmd("CLUSTER").arg("INFO").query(&mut connection).ok()
            })
            .unwrap_or_default()
    }
}

impl Drop for ValkeyServer {
    fn drop(&mut self) {
        if let Some(mut child) = self.process.lock().unwrap().take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// A port free now; for a cluster node, one whose bus port (+10000) is free too.
fn free_port(cluster: bool) -> u16 {
    loop {
        let port = TcpListener::bind("127.0.0.1:0")
            .expect("a free port")
            .local_addr()
            .expect("a local address")
            .port();
        if !cluster || (port <= 55_535 && TcpListener::bind(("127.0.0.1", port + 10_000)).is_ok()) {
            return port;
        }
    }
}
