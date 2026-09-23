//! The `/kabudachi/join/1` `request_response` protocol (README §27 Phase 2
//! bootstrap join): a length-prefixed, prost-encoded `JoinRequest` as the
//! request, and a length-prefixed, prost-encoded `JoinResponse` (the
//! responder's known membership) as the response.
//!
//! A separate `request_response::Behaviour` from the election protocol's
//! (`crate::codec`) — see `net/src/swarm.rs`'s `Behaviour` — because a join
//! exchange is a real, correlated request/response (the caller needs the
//! specific reply to its own request, unlike `PeerMessenger::send`'s
//! fire-and-forget, uncorrelated delivery). `JoinRequest`/`JoinResponse` are
//! themselves the codec's `Request`/`Response` types directly: unlike
//! `ElectionMessage`, there's no wrapping oneof envelope, because there's
//! only ever exactly one request shape and one response shape.

use std::io;

use kabudachi_core::protocol::messages::{JoinRequest, JoinResponse};
use libp2p::StreamProtocol;
use libp2p::futures::{AsyncRead, AsyncWrite, AsyncWriteExt};
use libp2p::request_response;

use crate::framing::{encode_length_prefixed, read_message};

/// The sole protocol this codec negotiates.
pub const PROTOCOL: StreamProtocol = StreamProtocol::new("/kabudachi/join/1");

/// `request_response::Codec` for [`PROTOCOL`]. Stateless, so one instance
/// (or any number of clones) can be shared across connections.
#[derive(Debug, Clone, Copy, Default)]
pub struct JoinCodec;

impl request_response::Codec for JoinCodec {
    type Protocol = StreamProtocol;
    type Request = JoinRequest;
    type Response = JoinResponse;

    async fn read_request<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<JoinRequest>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_message(io).await
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<JoinResponse>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_message(io).await
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
        io: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        io.write_all(&encode_length_prefixed(&res)).await
    }
}



#[cfg(test)]
mod tests {
    use kabudachi_core::protocol::ids::WorkerId;
    use kabudachi_core::protocol::messages::JoinMember;
    use libp2p::futures::io::Cursor;
    use libp2p::request_response::Codec as _;

    use super::*;
    use crate::framing::MAX_MESSAGE_BYTES;

    #[tokio::test]
    async fn write_request_then_read_request_round_trips_the_message() {
        let mut codec = JoinCodec;

        let mut written = Cursor::new(Vec::new());
        codec
            .write_request(&PROTOCOL, &mut written, JoinRequest {})
            .await
            .expect("writing a request never fails against an in-memory buffer");

        let mut to_read = Cursor::new(written.into_inner());
        let decoded = codec
            .read_request(&PROTOCOL, &mut to_read)
            .await
            .expect("reading back what was just written must succeed");

        assert_eq!(decoded, JoinRequest {});
    }

    #[tokio::test]
    async fn write_response_then_read_response_round_trips_the_membership() {
        let mut codec = JoinCodec;
        let response = JoinResponse {
            members: vec![
                JoinMember {
                    worker_id: Some(WorkerId::new("worker-a").into()),
                    multiaddr: "/ip4/127.0.0.1/tcp/1".into(),
                },
                JoinMember {
                    worker_id: Some(WorkerId::new("worker-b").into()),
                    multiaddr: "/ip4/127.0.0.1/tcp/2".into(),
                },
            ],
        };

        let mut written = Cursor::new(Vec::new());
        codec
            .write_response(&PROTOCOL, &mut written, response.clone())
            .await
            .expect("writing a response never fails against an in-memory buffer");

        let mut to_read = Cursor::new(written.into_inner());
        let decoded = codec
            .read_response(&PROTOCOL, &mut to_read)
            .await
            .expect("reading back what was just written must succeed");

        assert_eq!(decoded, response);
    }

    #[tokio::test]
    async fn read_request_rejects_a_length_prefix_over_the_maximum() {
        let mut codec = JoinCodec;
        let mut oversized_prefix = Cursor::new((MAX_MESSAGE_BYTES + 1).to_be_bytes().to_vec());

        let result = codec.read_request(&PROTOCOL, &mut oversized_prefix).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn read_response_rejects_a_length_prefix_over_the_maximum() {
        let mut codec = JoinCodec;
        let mut oversized_prefix = Cursor::new((MAX_MESSAGE_BYTES + 1).to_be_bytes().to_vec());

        let result = codec.read_response(&PROTOCOL, &mut oversized_prefix).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn read_response_rejects_a_member_missing_its_worker_id() {
        let mut codec = JoinCodec;
        let response = JoinResponse {
            members: vec![JoinMember {
                worker_id: None,
                multiaddr: "/ip4/127.0.0.1/tcp/1".into(),
            }],
        };

        let mut written = Cursor::new(Vec::new());
        codec
            .write_response(&PROTOCOL, &mut written, response)
            .await
            .expect("writing a response never fails against an in-memory buffer");
        let mut to_read = Cursor::new(written.into_inner());
        let result = codec.read_response(&PROTOCOL, &mut to_read).await;

        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }
}
