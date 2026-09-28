//! The `/kabudachi/election/2` `request_response` protocol: a
//! length-prefixed, prost-encoded `ElectionMessage` as the request, and a
//! trivial zero-byte acknowledgement as the response.
//!
//! Version 2 is the gossip roll call's schema (ADR-0001). It gives several
//! of version 1's field numbers and payload tags, from the ring roll call,
//! new meanings, so a version 1 peer must never negotiate a stream with a
//! version 2 one: it would misread every message, or read one as another
//! kind. The new name keeps them apart.
//!
//! The response carries no information. `crate::messenger::Net::send`
//! documents delivery as unreliable and fire-and-forget — callers
//! never await a reply — so the ack exists only to let the request/response
//! substream close cleanly instead of every exchange ending in
//! `InboundFailure::ResponseOmission` on the sender's side.

use std::io;

use kabudachi_core::protocol::messages::ElectionMessage;
use libp2p::StreamProtocol;
use libp2p::futures::{AsyncRead, AsyncWrite, AsyncWriteExt};
use libp2p::request_response;

use crate::framing::{encode_length_prefixed, read_message};

/// The sole protocol this codec negotiates.
pub const PROTOCOL: StreamProtocol = StreamProtocol::new("/kabudachi/election/2");

/// The response half of the protocol. It carries no data — see the module
/// doc for why a real ack payload isn't needed here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Ack;

/// `request_response::Codec` for [`PROTOCOL`]. Stateless, so one instance
/// (or any number of clones) can be shared across connections.
#[derive(Debug, Clone, Copy, Default)]
pub struct ElectionCodec;

impl request_response::Codec for ElectionCodec {
    type Protocol = StreamProtocol;
    type Request = ElectionMessage;
    type Response = Ack;

    async fn read_request<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<ElectionMessage>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_message(io).await
    }

    async fn read_response<T>(&mut self, _: &Self::Protocol, _io: &mut T) -> io::Result<Ack>
    where
        T: AsyncRead + Unpin + Send,
    {
        // No bytes to read: the ack is empty by construction (see module doc).
        Ok(Ack)
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        io.write_all(&encode_length_prefixed(&req)).await
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        _io: &mut T,
        _res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        // Nothing to write: the ack is empty by construction (see module doc).
        Ok(())
    }
}



#[cfg(test)]
mod tests {
    use kabudachi_core::configuration::{Configuration, Generation, Single};
    use kabudachi_core::protocol::ids::{IncarnationId, ShardId, WorkerId};
    use kabudachi_core::protocol::messages::{
        AckEcho, LeaderHeartbeatAck, WorkerHeartbeat, election_message,
    };
    use libp2p::futures::io::Cursor;
    use libp2p::request_response::Codec as _;

    use super::*;
    use crate::framing::MAX_MESSAGE_BYTES;

    fn sample_heartbeat() -> WorkerHeartbeat {
        WorkerHeartbeat {
            worker_id: Some(WorkerId::new("worker-a").into()),
            incarnation_id: Some(IncarnationId::new("incarnation-1").into()),
            recovery_epoch_seen: 1,
            term_seen: 2,
            available_capacity: 3,
            active_task_runs_digest: vec![9, 9, 9],
            shard_id: Some(ShardId::new("shard-1").into()),
            newest_accepted_ack: Some(AckEcho {
                term: 2,
                send_token: 41,
            }),
            configuration_generation: Some(Generation::genesis(1).into()),
            send_token: 43,
        }
    }

    fn sample_message() -> ElectionMessage {
        ElectionMessage {
            payload: Some(election_message::Payload::Heartbeat(sample_heartbeat())),
        }
    }

    /// Writes `message` as a request and reads it back.
    async fn round_trip(message: ElectionMessage) -> ElectionMessage {
        let mut codec = ElectionCodec;
        let mut written = Cursor::new(Vec::new());
        codec
            .write_request(&PROTOCOL, &mut written, message)
            .await
            .expect("writing a request never fails against an in-memory buffer");

        let mut to_read = Cursor::new(written.into_inner());
        codec
            .read_request(&PROTOCOL, &mut to_read)
            .await
            .expect("reading back what was just written must succeed")
    }

    #[tokio::test]
    async fn write_request_then_read_request_round_trips_the_message() {
        let message = sample_message();

        assert_eq!(round_trip(message.clone()).await, message);
    }

    #[tokio::test]
    async fn a_heartbeat_that_has_accepted_no_ack_decodes_with_no_echo() {
        let no_echo = ElectionMessage {
            payload: Some(election_message::Payload::Heartbeat(WorkerHeartbeat {
                newest_accepted_ack: None,
                ..sample_heartbeat()
            })),
        };
        // A token of 0 is a real send instant, so it must stay distinct
        // from "no ack accepted yet".
        let echo_of_instant_zero = ElectionMessage {
            payload: Some(election_message::Payload::Heartbeat(WorkerHeartbeat {
                newest_accepted_ack: Some(AckEcho {
                    term: 0,
                    send_token: 0,
                }),
                ..sample_heartbeat()
            })),
        };

        assert_eq!(round_trip(no_echo.clone()).await, no_echo);
        assert_eq!(
            round_trip(echo_of_instant_zero.clone()).await,
            echo_of_instant_zero
        );
    }

    #[tokio::test]
    async fn a_leader_ack_round_trips_its_send_token() {
        let ack = ElectionMessage {
            payload: Some(election_message::Payload::HeartbeatAck(
                LeaderHeartbeatAck {
                    shard_id: Some(ShardId::new("shard-1").into()),
                    leader_id: Some(WorkerId::new("leader-1").into()),
                    recovery_epoch: 1,
                    term: 2,
                    configuration: Some((&Configuration::genesis(1)).into()),
                    recipient_admission: Some(Generation::genesis(1).into()),
                    send_token: 1_234,
                    recipient_prior_admission: Some(Generation::genesis(0).into()),
                    heartbeat_token: None,
                    recovery_epoch_lineage: None,
                },
            )),
        };

        assert_eq!(round_trip(ack.clone()).await, ack);
    }

    #[tokio::test]
    async fn read_request_rejects_a_length_prefix_over_the_maximum() {
        let mut codec = ElectionCodec;
        let mut oversized_prefix = Cursor::new((MAX_MESSAGE_BYTES + 1).to_be_bytes().to_vec());

        let result = codec.read_request(&PROTOCOL, &mut oversized_prefix).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn write_response_then_read_response_yields_an_ack() {
        let mut codec = ElectionCodec;

        let mut written = Cursor::new(Vec::new());
        codec
            .write_response(&PROTOCOL, &mut written, Ack)
            .await
            .expect("writing the empty ack never fails");
        assert!(
            written.get_ref().is_empty(),
            "the ack is documented as carrying no bytes on the wire"
        );

        let mut to_read = Cursor::new(written.into_inner());
        let ack = codec
            .read_response(&PROTOCOL, &mut to_read)
            .await
            .expect("reading the empty ack never fails");
        assert_eq!(ack, Ack);
    }

    /// The ack a follower must never adopt from: its configuration's term (9)
    /// is above the ack's (3). Adopted, it would later make a removal the
    /// follower leads refuse its own term.
    #[tokio::test]
    async fn read_request_rejects_an_ack_whose_configuration_outruns_its_term() {
        let outrun = Generation::new(0, 9, 1);
        let ack = ElectionMessage {
            payload: Some(election_message::Payload::HeartbeatAck(
                LeaderHeartbeatAck {
                    shard_id: Some(ShardId::new("shard-1").into()),
                    leader_id: Some(WorkerId::new("leader-1").into()),
                    recovery_epoch: 0,
                    term: 3,
                    configuration: Some(
                        (&Configuration::single(Single {
                            generation: outrun,
                            base: outrun,
                            voter_count: 2,
                        }))
                            .into(),
                    ),
                    recipient_admission: None,
                    send_token: 0,
                    recipient_prior_admission: None,
                    heartbeat_token: None,
                    recovery_epoch_lineage: None,
                },
            )),
        };

        let mut codec = ElectionCodec;
        let mut written = Cursor::new(Vec::new());
        codec
            .write_request(&PROTOCOL, &mut written, ack)
            .await
            .expect("writing a request never fails against an in-memory buffer");
        let mut to_read = Cursor::new(written.into_inner());
        let result = codec.read_request(&PROTOCOL, &mut to_read).await;

        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn read_request_rejects_a_message_missing_a_required_id() {
        let mut codec = ElectionCodec;
        let mut message = sample_message();
        if let Some(election_message::Payload::Heartbeat(heartbeat)) = &mut message.payload {
            heartbeat.worker_id = None;
        }

        let mut written = Cursor::new(Vec::new());
        codec
            .write_request(&PROTOCOL, &mut written, message)
            .await
            .expect("writing a request never fails against an in-memory buffer");
        let mut to_read = Cursor::new(written.into_inner());
        let result = codec.read_request(&PROTOCOL, &mut to_read).await;

        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }
}
