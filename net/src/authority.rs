//! A worker's one way to call its coordination authority from net: the
//! [`AuthorityClient`]. Both the bootstrap cascade and the driver use it, so
//! at most one call of each kind is ever in flight, whoever asked.

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use kabudachi_core::coordination_authority::CoordinationAuthority;
use kabudachi_core::election::{
    AuthorityCall, AuthorityPerformer, AuthorityReply, AuthorityRequest, CallKind, Issuer,
    ReplyToken, ReplyTokens,
};
use kabudachi_core::protocol::ids::{ShardId, WorkerId};
use kabudachi_core::time::Instant;
use libp2p::Multiaddr;
use tokio::sync::{mpsc, watch};

use crate::messenger::Net;

/// A coordination authority shared by every task that calls it: the
/// client's, and the blocking-pool tasks that perform its calls.
pub type SharedAuthority = Arc<dyn CoordinationAuthority + Send + Sync>;

/// A worker's one way to call its coordination authority from net. It
/// performs each call on Tokio's blocking pool, answers a call that panics as
/// `Unavailable`, and keeps at most one call of each kind in flight, whoever
/// asked. `Worker::run` makes one, lends it to the bootstrap cascade, then
/// moves it into the driver, whose rejoin uses it too. It holds no `Net`: it
/// reads the worker's id once and watches its own address.
///
/// A slow authority holds at most one blocking thread per kind, rather than
/// one more at every renewal. A dropped call is asked again on its asker's
/// own schedule: the node's renewals come round, and a fenced node reads the
/// epoch again at its next registration.
pub struct AuthorityClient {
    authority: SharedAuthority,
    shard_id: ShardId,
    my_id: WorkerId,
    /// Whose address a registration names.
    own_address: watch::Receiver<Option<Multiaddr>>,
    /// Where a call performed on the blocking pool sends its reply.
    sender: mpsc::UnboundedSender<AuthorityReply>,
    replies: mpsc::UnboundedReceiver<AuthorityReply>,
    /// The token of the call of each kind performed and not yet answered.
    in_flight: BTreeMap<CallKind, ReplyToken>,
    /// Mints the token of every call net asks for itself. Its issuer is
    /// [`Issuer::Cascade`], so no token of it equals a node's.
    tokens: ReplyTokens,
}

impl AuthorityClient {
    pub fn new(net: &Net, shard_id: ShardId, authority: SharedAuthority) -> Self {
        Self::from_parts(
            authority,
            shard_id,
            net.local_worker_id(),
            net.own_address_watch(),
        )
    }

    /// For tests with no `Net`.
    #[cfg(test)]
    pub(crate) fn with_parts(
        authority: SharedAuthority,
        shard_id: ShardId,
        my_id: WorkerId,
        own_address: watch::Receiver<Option<Multiaddr>>,
    ) -> Self {
        Self::from_parts(authority, shard_id, my_id, own_address)
    }

    fn from_parts(
        authority: SharedAuthority,
        shard_id: ShardId,
        my_id: WorkerId,
        own_address: watch::Receiver<Option<Multiaddr>>,
    ) -> Self {
        let (sender, replies) = mpsc::unbounded_channel();
        AuthorityClient {
            authority,
            shard_id,
            my_id,
            own_address,
            sender,
            replies,
            in_flight: BTreeMap::new(),
            tokens: ReplyTokens::new(Issuer::Cascade),
        }
    }

    /// Asks for `request`, stamped `sent_at`, under the next token of this
    /// client's `Issuer::Cascade` mint: a call net asks for itself (the
    /// cascade's, a rejoin's). `None`, and nothing asked or minted, while a
    /// call of its kind is unanswered.
    pub(crate) fn ask(&mut self, request: AuthorityRequest, sent_at: Instant) -> Option<ReplyToken> {
        if self.in_flight.contains_key(&CallKind::of(&request)) {
            self.log_dropped(&request);
            return None;
        }
        let call = AuthorityCall::new(request, &mut self.tokens, sent_at);
        self.start(call);
        Some(call.token)
    }

    /// The next reply, waiting at most `within` (for ever when `None`); `None`
    /// if none came in time. Taking a reply frees its kind. Cancel-safe, for
    /// use in `select!`.
    pub(crate) async fn next_reply(&mut self, within: Option<StdDuration>) -> Option<AuthorityReply> {
        // `sender` lives as long as `replies`, so the channel never closes.
        let reply = match within {
            Some(within) => tokio::time::timeout(within, self.replies.recv())
                .await
                .ok()??,
            None => self.replies.recv().await?,
        };
        Some(self.freed_by(reply))
    }

    /// A reply that has already arrived, if any, taken as [`Self::next_reply`]
    /// takes one.
    pub(crate) fn try_reply(&mut self) -> Option<AuthorityReply> {
        let reply = self.replies.try_recv().ok()?;
        Some(self.freed_by(reply))
    }

    /// Starts `call` on the blocking pool, unless a call of its kind is
    /// unanswered.
    fn start(&mut self, call: AuthorityCall) {
        if self.in_flight.contains_key(&call.token.kind) {
            self.log_dropped(&call.request);
            return;
        }
        self.in_flight.insert(call.token.kind, call.token);
        let authority = Arc::clone(&self.authority);
        let shard_id = self.shard_id.clone();
        let my_id = self.my_id.clone();
        let address = self
            .own_address
            .borrow()
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let replies = self.sender.clone();
        tokio::task::spawn_blocking(move || {
            let performed = std::panic::catch_unwind(AssertUnwindSafe(|| {
                call.perform(&*authority, &shard_id, &my_id, &address)
            }));
            let reply = performed.unwrap_or_else(|_| {
                tracing::error!(
                    request = ?call.request,
                    "a coordination authority call panicked; answering it as unavailable"
                );
                call.unavailable()
            });
            // Whoever asked has stopped: no one is left to hand it to.
            let _ = replies.send(reply);
        });
    }

    fn log_dropped(&self, request: &AuthorityRequest) {
        tracing::debug!(
            request = ?request,
            "not asking the coordination authority again while the same kind of call is \
             unanswered"
        );
    }

    /// Frees the kind `reply` answers, and returns it.
    fn freed_by(&mut self, reply: AuthorityReply) -> AuthorityReply {
        let recorded = self.in_flight.remove(&reply.token().kind);
        debug_assert_eq!(
            recorded,
            Some(reply.token()),
            "a reply answers the one call of its kind in flight"
        );
        reply
    }
}

/// The node's calls, with the node's own `Issuer::Node` tokens: each starts on
/// the blocking pool unless its kind is in flight, when it is dropped, as if
/// the authority had not answered it. Always `None`: the reply comes through
/// `next_reply`.
impl AuthorityPerformer for AuthorityClient {
    fn perform(&mut self, call: AuthorityCall) -> Option<AuthorityReply> {
        self.start(call);
        None
    }
}

#[cfg(test)]
mod tests {
    use kabudachi_core::election::{
        AuthorityCall, AuthorityPerformer, AuthorityReply, AuthorityRequest, CallKind, Issuer,
        ReplyTokens,
    };
    use kabudachi_core::coordination_authority::AuthorityError;
    use kabudachi_core::protocol::ids::{ShardId, WorkerId};
    use kabudachi_core::time::{Duration as TickDuration, Instant, RealClock};
    use kabudachi_testkit::FaultingAuthority;
    use tokio::sync::watch;

    use super::*;
    use crate::test_support::TEST_TIMEOUT;

    fn client_over(authority: &FaultingAuthority<RealClock>) -> AuthorityClient {
        AuthorityClient::with_parts(
            std::sync::Arc::new(authority.clone()),
            ShardId::new("shard-1"),
            WorkerId::new("me"),
            watch::channel(None).1,
        )
    }

    fn faulting() -> FaultingAuthority<RealClock> {
        FaultingAuthority::new(RealClock::new(), TickDuration::from_secs(30))
    }

    async fn wait_until_holding(authority: &FaultingAuthority<RealClock>, kind: CallKind) {
        tokio::time::timeout(TEST_TIMEOUT, async {
            while !authority.is_holding(kind) {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("a {kind:?} call was held within the timeout"));
    }

    async fn next(client: &mut AuthorityClient) -> AuthorityReply {
        client
            .next_reply(Some(TEST_TIMEOUT))
            .await
            .expect("a reply arrived within the timeout")
    }

    #[tokio::test]
    async fn a_second_call_of_a_kind_is_dropped_while_the_first_is_held() {
        let authority = faulting();
        let mut client = client_over(&authority);
        authority.hold_next(CallKind::Register);

        let first = client
            .ask(AuthorityRequest::Register, Instant::at(0))
            .expect("the first call is asked");
        assert_eq!(first.issuer, Issuer::Cascade);
        wait_until_holding(&authority, CallKind::Register).await;
        let second = client.ask(AuthorityRequest::Register, Instant::at(0));

        let other = client.ask(AuthorityRequest::ReadRecoveryEpoch, Instant::at(0));
        let other_reply = if other.is_some() {
            Some(next(&mut client).await)
        } else {
            None
        };
        authority.release(CallKind::Register);
        assert_eq!(second, None, "a call of a held kind is dropped");
        assert!(
            matches!(other_reply, Some(AuthorityReply::RecoveryEpoch { .. })),
            "another kind is not held up: {other_reply:?}"
        );
        let reply = next(&mut client).await;
        assert!(matches!(reply, AuthorityReply::Registered { .. }));
        assert_eq!(reply.token(), first);
    }

    #[tokio::test]
    async fn a_reply_frees_its_kind() {
        let authority = faulting();
        let mut client = client_over(&authority);

        let first = client.ask(AuthorityRequest::Register, Instant::at(0)).unwrap();
        next(&mut client).await;
        let again = client
            .ask(AuthorityRequest::Register, Instant::at(0))
            .expect("the kind was freed");

        assert!(again.number > first.number);
    }

    #[tokio::test]
    async fn a_node_call_that_panics_answers_unavailable_and_frees_its_kind() {
        let authority = faulting();
        let mut client = client_over(&authority);
        let mut node_tokens = ReplyTokens::new(Issuer::Node);
        authority.panic_next(CallKind::Register);

        let call = AuthorityCall::new(AuthorityRequest::Register, &mut node_tokens, Instant::at(0));
        assert!(client.perform(call).is_none());
        let reply = next(&mut client).await;
        assert!(matches!(
            reply,
            AuthorityReply::Registered {
                result: Err(AuthorityError::Unavailable),
                ..
            }
        ));
        assert_eq!(reply.token(), call.token, "the node's own token comes back");

        let call = AuthorityCall::new(AuthorityRequest::Register, &mut node_tokens, Instant::at(0));
        assert!(client.perform(call).is_none());
        assert!(matches!(
            next(&mut client).await,
            AuthorityReply::Registered { result: Ok(_), .. }
        ));
    }
}
