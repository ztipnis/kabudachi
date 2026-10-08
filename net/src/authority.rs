//! A worker's one way to call its coordination authority from net: the
//! [`AuthorityClient`]. Both the bootstrap cascade and the driver use it, so
//! at most one call of each kind is ever in flight, whoever asked.

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use kabudachi_core::coordination_authority::CoordinationAuthority;
use kabudachi_core::election::{
    AuthorityCall, AuthorityTimings, AuthorityPerformer, AuthorityReply, AuthorityRequest, CallKind, Issuer,
    ReplyToken, ReplyTokens,
};
use kabudachi_core::protocol::ids::{ShardId, ShardName, WorkerId};
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
/// A call unanswered after the TTL of the worker's [`AuthorityTimings`], the
/// time the node counts a lost call by, is answered as `Unavailable`, which
/// frees its kind. The blocking thread it was on may still be running; if it
/// ever returns, its result is discarded, so a call is answered once.
///
/// A slow authority holds at most one blocking thread per kind, rather than
/// one more at every renewal. A dropped call is asked again on its asker's
/// own schedule: the node's renewals come round, and a fenced node reads the
/// epoch again at its next registration.
pub struct AuthorityClient {
    authority: SharedAuthority,
    name: ShardName,
    shard_id: ShardId,
    my_id: WorkerId,
    /// Whose address a registration names.
    own_address: watch::Receiver<Option<Multiaddr>>,
    /// Where a call performed on the blocking pool sends its reply.
    sender: mpsc::UnboundedSender<AuthorityReply>,
    replies: mpsc::UnboundedReceiver<AuthorityReply>,
    /// The token of the call of each kind performed and not yet answered.
    in_flight: BTreeMap<CallKind, ReplyToken>,
    /// How long a call may go unanswered before it is answered as lost.
    call_timeout: StdDuration,
    /// Mints the token of every call net asks for itself. Its issuer is
    /// [`Issuer::Cascade`], so no token of it equals a node's.
    tokens: ReplyTokens,
}

impl AuthorityClient {
    pub fn new(
        net: &Net,
        name: ShardName,
        shard_id: ShardId,
        authority: SharedAuthority,
        timings: AuthorityTimings,
    ) -> Self {
        Self::from_parts(
            authority,
            name,
            shard_id,
            timings,
            net.local_worker_id(),
            net.own_address_watch(),
        )
    }

    fn from_parts(
        authority: SharedAuthority,
        name: ShardName,
        shard_id: ShardId,
        timings: AuthorityTimings,
        my_id: WorkerId,
        own_address: watch::Receiver<Option<Multiaddr>>,
    ) -> Self {
        let (sender, replies) = mpsc::unbounded_channel();
        AuthorityClient {
            authority,
            name,
            shard_id,
            my_id,
            own_address,
            sender,
            replies,
            in_flight: BTreeMap::new(),
            call_timeout: StdDuration::from_millis(timings.ttl.as_ticks()),
            tokens: ReplyTokens::new(Issuer::Cascade),
        }
    }

    /// The incarnation every call started from now on names.
    pub(crate) fn shard_id(&self) -> &ShardId {
        &self.shard_id
    }

    /// Serves `shard_id` for calls started after this: the incarnation every
    /// later call names. The cascade serves the one it is reading or
    /// founding, `Worker::run` the one it entered. A call already in flight
    /// keeps the id it started with.
    pub(crate) fn serve(&mut self, shard_id: ShardId) {
        self.shard_id = shard_id;
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
        let name = self.name.clone();
        let shard_id = self.shard_id.clone();
        let my_id = self.my_id.clone();
        let address = self
            .own_address
            .borrow()
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let replies = self.sender.clone();
        let call_timeout = self.call_timeout;
        tokio::spawn(async move {
            let performing = tokio::task::spawn_blocking(move || {
                std::panic::catch_unwind(AssertUnwindSafe(|| {
                    call.perform(&*authority, &name, &shard_id, &my_id, &address)
                }))
            });
            // On a timeout `performing` is dropped, not aborted: a running
            // blocking task cannot be stopped, and its result goes nowhere.
            let reply = match tokio::time::timeout(call_timeout, performing).await {
                Ok(Ok(Ok(reply))) => reply,
                Ok(_) => {
                    tracing::error!(
                        request = ?call.request,
                        "a coordination authority call panicked; answering it as unavailable"
                    );
                    call.unavailable()
                }
                Err(_) => {
                    tracing::warn!(
                        request = ?call.request,
                        "a coordination authority call went unanswered; answering it as unavailable"
                    );
                    call.unavailable()
                }
            };
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
