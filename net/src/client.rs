//! A caller's transport and leader discovery. A caller never joins the
//! worker roster, registers with the authority, votes, or claims work.

use std::sync::Arc;

use kabudachi_core::coordination_authority::RecoveryEpoch;
use kabudachi_core::election::JoinFloor;
use kabudachi_core::protocol::messages::{JoinResponse, JoinResponseIds};
use libp2p::Multiaddr;
use tokio::task::JoinHandle;

use crate::join::{LeaderSearch, ask_for_leader};
use crate::messenger::{ListenRejected, Net};
use crate::worker::{WorkerConfig, listen};

/// A listening caller, ready to discover the existing shard's leader.
pub struct Client {
    net: Arc<Net>,
    config: WorkerConfig,
}

impl Client {
    /// Opens the result transport using `config`'s addresses and limits.
    /// No worker node or executor is started. A caller needs seeds or an
    /// authority; unlike a worker it never founds a shard.
    pub async fn start(config: WorkerConfig) -> Result<Self, ListenRejected> {
        let net = listen(&config, true).await?;
        let (in_flight, store) = crate::payload_memory::payload_shares(config.memory_limits);
        net.payload_memory().set_limits(in_flight);
        net.held_records().set_blob_store_limit(store);
        Ok(Self { net, config })
    }

    /// The transport used for task submission and certified results.
    pub fn net(&self) -> Arc<Net> {
        Arc::clone(&self.net)
    }

    /// Keeps discovering the shard's leader through configured seeds and
    /// authority listings. `observe` receives the reachable pointer, or
    /// `None` while an election or loss of connectivity hides the leader.
    /// Dropping this future stops discovery without changing membership.
    pub async fn run(self, mut observe: impl FnMut(Option<JoinResponse>)) {
        let mut floor = JoinFloor::none();
        let mut known_address = None;
        let mut listed = Vec::new();
        let mut listing: Option<JoinHandle<Vec<Multiaddr>>> = None;
        loop {
            if listing.as_ref().is_some_and(JoinHandle::is_finished)
                && let Some(done) = listing.take()
            {
                listed = done.await.unwrap_or_default();
            }
            if listing.is_none()
                && let Some(authority) = &self.config.authority
            {
                let authority = Arc::clone(authority.authority());
                let name = self.config.shard_name.clone();
                listing = Some(tokio::task::spawn_blocking(move || {
                    let mut addresses = Vec::new();
                    if let Ok(Some(hint)) = authority.read_leader_hint(&name) {
                        addresses.extend(hint.address.parse::<Multiaddr>());
                    }
                    if let Ok(Some(record)) = authority.read_shard(&name)
                        && let Ok(peers) = authority.live_registrations(&name, &record.shard_id)
                    {
                        addresses.extend(peers.addresses().values().filter_map(|a| a.parse().ok()));
                    }
                    addresses
                }));
            }
            let addresses: Vec<_> = self
                .config
                .seeds
                .iter()
                .chain(known_address.iter())
                .chain(listed.iter())
                .cloned()
                .collect();
            let found = ask_for_leader(
                &self.net,
                &addresses,
                floor,
                self.config.join_peer_timeout,
                std::time::Duration::from_millis(
                    self.config.election_timings.heartbeat_interval.as_ticks(),
                ),
            )
            .await;
            match found {
                LeaderSearch::Found(pointer)
                    if pointer
                        .shard_id()
                        .is_some_and(|id| id.name() == self.config.shard_name) =>
                {
                    floor = JoinFloor::at(RecoveryEpoch::new(
                        pointer.recovery_epoch,
                        pointer.recovery_epoch_lineage,
                    ));
                    known_address = pointer.leader_multiaddr.parse().ok();
                    self.net.name_leader(pointer.leader_id());
                    self.net.refresh_peer_routing();
                    observe(Some(pointer));
                }
                _ => {
                    self.net.name_leader(None);
                    observe(None);
                }
            }
            // Election traffic may arrive when the leader first sees this
            // connection. A caller never replies with a worker heartbeat.
            self.net.take_inputs();
            self.net.poll_join_requests();
            self.net.poll_task_requests();
            tokio::time::sleep(self.config.retry_interval).await;
        }
    }
}
