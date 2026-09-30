//! The swarm task's half of a correlated request/response protocol: the asks
//! it has made and not yet had answered, and what each `request_response`
//! event settles. Join (`crate::join_codec`) and claim (`crate::claim::codec`)
//! are two instances of it; `crate::messenger`'s `Correlated` says how `Net`
//! reaches each one.

use std::collections::HashMap;

use libp2p::PeerId;
use libp2p::request_response::{self, OutboundRequestId, ResponseChannel};
use tokio::sync::oneshot;

/// An inbound request not yet answered, and the channel its answer goes back on.
pub(crate) struct Asked<C: request_response::Codec> {
    pub(crate) from: PeerId,
    pub(crate) request: C::Request,
    pub(crate) channel: ResponseChannel<C::Response>,
}

/// The swarm task's half of one correlated protocol: its asks awaiting an answer.
pub(crate) struct Exchange<C: request_response::Codec> {
    awaiting: HashMap<OutboundRequestId, oneshot::Sender<Option<C::Response>>>,
}

impl<C: request_response::Codec> Default for Exchange<C> {
    fn default() -> Self {
        Self {
            awaiting: HashMap::new(),
        }
    }
}

impl<C> Exchange<C>
where
    C: request_response::Codec + Clone + Send + 'static,
{
    /// Sends `request` to `to`; `respond_to` gets the answer, or `None` if the request fails.
    pub(crate) fn ask(
        &mut self,
        behaviour: &mut request_response::Behaviour<C>,
        to: &PeerId,
        request: C::Request,
        respond_to: oneshot::Sender<Option<C::Response>>,
    ) {
        let id = behaviour.send_request(to, request);
        self.awaiting.insert(id, respond_to);
    }

    /// Best-effort, like the election `Ack`: a closed channel means the asker stopped waiting.
    pub(crate) fn answer(
        behaviour: &mut request_response::Behaviour<C>,
        channel: ResponseChannel<C::Response>,
        response: C::Response,
    ) {
        let _ = behaviour.send_response(channel, response);
    }

    /// Settles what `event` settles:
    /// - a `Response` resolves its ask with `Some`;
    /// - an `OutboundFailure` resolves it with `None`;
    /// - an inbound `Request` comes back to be queued for the driver;
    /// - anything else is ignored.
    pub(crate) fn on_event(
        &mut self,
        event: request_response::Event<C::Request, C::Response>,
    ) -> Option<Asked<C>> {
        use request_response::{Event, Message};
        match event {
            Event::Message {
                peer,
                message:
                    Message::Request {
                        request, channel, ..
                    },
                ..
            } => Some(Asked {
                from: peer,
                request,
                channel,
            }),
            Event::Message {
                message:
                    Message::Response {
                        request_id,
                        response,
                    },
                ..
            } => {
                self.resolve(&request_id, Some(response));
                None
            }
            Event::OutboundFailure { request_id, .. } => {
                self.resolve(&request_id, None);
                None
            }
            _ => None,
        }
    }

    /// The asker may have stopped waiting, so a closed channel is no error.
    fn resolve(&mut self, id: &OutboundRequestId, outcome: Option<C::Response>) {
        if let Some(respond_to) = self.awaiting.remove(id) {
            let _ = respond_to.send(outcome);
        }
    }
}
