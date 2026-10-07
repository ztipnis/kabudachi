//! Helpers shared by the unit tests in this crate. Integration tests are
//! separate crates and keep their own in `tests/support`.

use std::time::Duration;

use kabudachi_core::election::Input;

use crate::messenger::Net;

/// How long a unit test waits for anything that should happen at once.
pub(crate) const TEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Takes `net`'s queued inputs until one is `expected`.
pub(crate) async fn wait_for_input(net: &Net, expected: &Input) {
    tokio::time::timeout(TEST_TIMEOUT, async {
        while !net.take_inputs().contains(expected) {
            net.wait_for_arrival().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{expected:?} arrived within the timeout"));
}
