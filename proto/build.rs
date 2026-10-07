const PROTO_FILES: [&str; 7] = [
    "claim.proto",
    "election.proto",
    "ids.proto",
    "join.proto",
    "task.proto",
    "task_exchange.proto",
    "task_record.proto",
];

fn main() {
    println!("cargo:rerun-if-env-changed=PROTOC");
    for proto in PROTO_FILES {
        println!("cargo:rerun-if-changed={proto}");
    }
    let mut config = prost_build::Config::new();

    // With `PROTOC` set, use that `protoc` binary like any other prost
    // project. Otherwise use `protox`, a pure-Rust protobuf compiler, so the
    // default build needs no `protoc` installed.
    if std::env::var_os("PROTOC").is_some() {
        config
            .compile_protos(&PROTO_FILES, &["."])
            .expect("protoc failed to compile protos");
    } else {
        let file_descriptors =
            protox::compile(PROTO_FILES, ["."]).expect("protox failed to compile protos");
        config
            .compile_fds(file_descriptors)
            .expect("failed to generate Rust from protos");
    }
}
