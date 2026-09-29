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
    use kabudachi_core::configuration::Generation;
    use kabudachi_core::protocol::ids::{IncarnationId, ShardId, WorkerId};
    use kabudachi_core::protocol::messages::{
        AckEcho, WorkerHeartbeat, election_message,
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

    #[tokio::test]
    async fn read_request_rejects_a_length_prefix_over_the_maximum() {
        let mut codec = ElectionCodec;
        let mut oversized_prefix = Cursor::new((MAX_MESSAGE_BYTES + 1).to_be_bytes().to_vec());

        let result = codec.read_request(&PROTOCOL, &mut oversized_prefix).await;

        assert!(result.is_err());
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
