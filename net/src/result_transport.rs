//! The result transport: how a task's result reaches its client, the worker
//! that submitted the task and awaits it, and how the client hears how the
//! task ended.
//!
//! [`codec`] frames the `/kabudachi/result/1` messages. The worker that ran a
//! task delivers its result ([`Net::deliver_result`]) before it reports the
//! run to its leader, asking again until the client acknowledges or the
//! run's reconnect timeout passes. The leader, once the report that ended
//! the task is stored, tells the client the run is certified
//! ([`Net::send_certified`]) or how the task ended without a result
//! ([`Net::send_concluded`]), asking again for up to [`PUSH_WITHIN`]. A
//! client reads what arrived with [`Net::take_result_events`], which is
//! when each message is acknowledged, so a sender that heard the
//! acknowledgement knows the client's reader holds it. [`Net::result_arrived`]
//! waits for more. A message for this worker itself is queued here with no
//! network hop. A client that missed a message asks the leader with
//! `QueryOutcome` (see `crate::task_exchange`).

use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use kabudachi_core::protocol::digest::Digest;
use kabudachi_core::protocol::ids::{TaskId, TaskRunId, WorkerId};
use kabudachi_core::protocol::messages::{
    CertifiedAck, DeliverResult, OutcomeAnswer, ResultAck, ResultCertified, ResultMessage,
    result_message,
};
use libp2p::PeerId;

use crate::framing::MAX_MESSAGE_BYTES;
use crate::messenger::Net;
use crate::result_transport::codec::ResultCodec;

pub mod codec;

/// Room a delivery leaves in its frame for everything but the payload:
/// two ids, a digest and the message framing.
const RESULT_FRAMING_BYTES: u32 = 4096;

/// The largest result a delivery carries in one frame. A larger one is not
/// delivered: its run fails instead.
pub const MAX_RESULT_PAYLOAD_BYTES: usize = (MAX_MESSAGE_BYTES - RESULT_FRAMING_BYTES) as usize;

/// How long a leader keeps asking a client to take a certification or a
/// conclusion. A client that does not take it in time asks the next leader
/// itself.
pub const PUSH_WITHIN: Duration = Duration::from_secs(30);

/// A result message that arrived for this worker as a client.
#[derive(Debug, Clone, PartialEq)]
pub enum ResultEvent {
    /// A run's result, not certified yet.
    Delivered {
        task: TaskId,
        run: TaskRunId,
        digest: Digest,
        payload: Vec<u8>,
    },
    /// The leader certified this run's result as the task's.
    Certified {
        task: TaskId,
        run: TaskRunId,
        digest: Digest,
    },
    /// The task ended without a result, or the leader answered how it stands.
    Concluded {
        task: TaskId,
        outcome: OutcomeAnswer,
    },
}

/// A delivery the client never acknowledged within its time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryError {
    Unreached,
}

impl ResultEvent {
    /// `message` as an arrival, with the acknowledgement it is answered
    /// with; `None` for an acknowledgement, or a message missing an id or
    /// digest it needs, which is left unanswered.
    pub(crate) fn arrived(message: ResultMessage) -> Option<(ResultEvent, ResultMessage)> {
        use result_message::Message;
        match message.message? {
            Message::Deliver(delivery) => Some((
                delivered(delivery)?,
                wrapped(Message::Delivered(ResultAck {})),
            )),
            Message::Certified(certified) => Some((
                certified_event(certified)?,
                wrapped(Message::CertifiedAck(CertifiedAck {})),
            )),
            Message::Concluded(outcome) => Some((
                ResultEvent::Concluded {
                    task: outcome.task_id.clone()?.into(),
                    outcome,
                },
                wrapped(Message::CertifiedAck(CertifiedAck {})),
            )),
            Message::Delivered(_) | Message::CertifiedAck(_) => None,
        }
    }
}

fn delivered(delivery: DeliverResult) -> Option<ResultEvent> {
    Some(ResultEvent::Delivered {
        task: delivery.task_id?.into(),
        run: delivery.task_run_id?.into(),
        digest: Digest::try_from(&delivery.result_digest?).ok()?,
        payload: delivery.payload,
    })
}

fn certified_event(certified: ResultCertified) -> Option<ResultEvent> {
    Some(ResultEvent::Certified {
        task: certified.task_id?.into(),
        run: certified.task_run_id?.into(),
        digest: Digest::try_from(&certified.result_digest?).ok()?,
    })
}

fn wrapped(message: result_message::Message) -> ResultMessage {
    ResultMessage {
        message: Some(message),
    }
}

fn is_ack(answer: &ResultMessage) -> bool {
    matches!(
        answer.message,
        Some(result_message::Message::Delivered(_) | result_message::Message::CertifiedAck(_))
    )
}

impl Net {
    /// Delivers a run's result to its task's client, asking again until the
    /// client acknowledges it or `within` passes. Delivered to this worker
    /// itself when it is the client.
    pub async fn deliver_result(
        &self,
        client: WorkerId,
        delivery: DeliverResult,
        within: Duration,
    ) -> Result<(), DeliveryError> {
        let message = wrapped(result_message::Message::Deliver(delivery));
        if self.push(client, message, within).await {
            Ok(())
        } else {
            Err(DeliveryError::Unreached)
        }
    }

    /// Tells a task's client its run is certified, asking again for up to
    /// [`PUSH_WITHIN`]; whether the client took it. Spawn it: it owns all it
    /// needs.
    pub fn send_certified(
        &self,
        client: WorkerId,
        certified: ResultCertified,
    ) -> impl Future<Output = bool> + Send + use<> {
        let message = wrapped(result_message::Message::Certified(certified));
        self.push(client, message, PUSH_WITHIN)
    }

    /// Tells a task's client how its task ended without a result, as
    /// [`Self::send_certified`] does.
    pub fn send_concluded(
        &self,
        client: WorkerId,
        outcome: OutcomeAnswer,
    ) -> impl Future<Output = bool> + Send + use<> {
        let message = wrapped(result_message::Message::Concluded(outcome));
        self.push(client, message, PUSH_WITHIN)
    }

    /// Every result message that arrived for this worker since the last
    /// call, each acknowledged now; a malformed one is dropped unanswered.
    pub fn take_result_events(&self) -> Vec<ResultEvent> {
        let mut events = self.inbound.take_own_results();
        for asked in self.take_asked::<ResultCodec>() {
            if let Some((event, ack)) = ResultEvent::arrived(asked.request) {
                self.answer::<ResultCodec>(asked.channel, ack);
                events.push(event);
            }
        }
        events
    }

    /// Queues `event` as if it had arrived for this worker, so whoever takes
    /// the result events hands on every one of them in a single order.
    pub fn queue_result_event(&self, event: ResultEvent) {
        self.inbound.queue_own_result(event);
    }

    /// Waits until a result message has arrived since this last returned.
    /// An arrival while no one waits is kept. Wait from one task at a time.
    pub async fn result_arrived(&self) {
        self.inbound.results_arrived.notified().await;
    }

    /// Sends `message` to `client`, or queues it here when `client` is this
    /// worker; whether it was taken.
    fn push(
        &self,
        client: WorkerId,
        message: ResultMessage,
        within: Duration,
    ) -> impl Future<Output = bool> + Send + use<> {
        let own = (client == self.local_worker_id()).then(|| Arc::clone(&self.inbound));
        let remote = match &own {
            Some(_) => None,
            None => PeerId::from_str(client.as_str()).ok().map(|to| {
                self.ask_until_accepted::<ResultCodec>(to, message.clone(), is_ack, within)
            }),
        };
        async move {
            if let Some(inbound) = own {
                return match ResultEvent::arrived(message) {
                    Some((event, _)) => {
                        inbound.queue_own_result(event);
                        true
                    }
                    None => false,
                };
            }
            match remote {
                Some(asked) => asked.await,
                None => false,
            }
        }
    }
}
