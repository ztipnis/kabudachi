//! The `/kabudachi/election/1` `request_response` protocol: a
//! length-prefixed, prost-encoded `ElectionMessage` as the request, and a
//! trivial zero-byte acknowledgement as the response.
//!
//! The response carries no information. `core::transport::PeerMessenger`'s
//! `send` documents delivery as unreliable and fire-and-forget — callers
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
pub const PROTOCOL: StreamProtocol = StreamProtocol::new("/kabudachi/election/1");

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
    use kabudachi_core::protocol::ids::{IncarnationId, WorkerId};
    use kabudachi_core::protocol::messages::{WorkerHeartbeat, election_message};
    use libp2p::futures::io::Cursor;
    use libp2p::request_response::Codec as _;

    use super::*;
    use crate::framing::MAX_MESSAGE_BYTES;

    fn sample_message() -> ElectionMessage {
        ElectionMessage {
            payload: Some(election_message::Payload::Heartbeat(WorkerHeartbeat {
                worker_id: Some(WorkerId::new("worker-a").into()),
                incarnation_id: Some(IncarnationId::new("incarnation-1").into()),
                recovery_epoch_seen: 1,
                term_seen: 2,
                available_capacity: 3,
                active_task_runs_digest: vec![9, 9, 9],
            })),
        }
    }

    #[tokio::test]
    async fn write_request_then_read_request_round_trips_the_message() {
        let mut codec = ElectionCodec;
        let message = sample_message();

        let mut written = Cursor::new(Vec::new());
        codec
            .write_request(&PROTOCOL, &mut written, message.clone())
            .await
            .expect("writing a request never fails against an in-memory buffer");

        let mut to_read = Cursor::new(written.into_inner());
        let decoded = codec
            .read_request(&PROTOCOL, &mut to_read)
            .await
            .expect("reading back what was just written must succeed");

        assert_eq!(decoded, message);
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
