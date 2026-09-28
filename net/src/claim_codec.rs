//! The `/kabudachi/claim/1` `request_response` protocol (README §8.2 claim
//! arbitration): a length-prefixed, prost-encoded `ClaimRequest`
//! (`REQUEST_CLAIM` for one task, or `CLAIM_OLDEST`) as the request, and a
//! length-prefixed, prost-encoded `ClaimResponse` (the accepted `Claim`, a
//! batch of them, or a `ClaimReject` naming one of
//! `core::scheduler::ClaimRejection`'s variants) as the response.
//!
//! A separate `request_response::Behaviour` from both the election
//! protocol's (`crate::codec`) and the bootstrap join protocol's
//! (`crate::join_codec`) — see `net/src/swarm.rs`'s `Behaviour` — for
//! exactly the reasons `join_codec`'s module doc already gives: this is a
//! genuine, correlated request/response (the caller needs the specific reply
//! to its own request), unlike `Net::send`'s fire-and-forget,
//! uncorrelated delivery. `ClaimRequest`/`ClaimResponse` are themselves the
//! codec's `Request`/`Response` types directly, mirroring
//! `JoinRequest`/`JoinResponse`.

use std::io;

use kabudachi_core::protocol::messages::{ClaimRequest, ClaimResponse};
use libp2p::StreamProtocol;
use libp2p::futures::{AsyncRead, AsyncWrite, AsyncWriteExt};
use libp2p::request_response;

use crate::framing::{encode_length_prefixed, read_message};

/// The sole protocol this codec negotiates.
pub const PROTOCOL: StreamProtocol = StreamProtocol::new("/kabudachi/claim/1");

/// `request_response::Codec` for [`PROTOCOL`]. Stateless, so one instance (or
/// any number of clones) can be shared across connections.
#[derive(Debug, Clone, Copy, Default)]
pub struct ClaimCodec;

impl request_response::Codec for ClaimCodec {
    type Protocol = StreamProtocol;
    type Request = ClaimRequest;
    type Response = ClaimResponse;

    async fn read_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<ClaimRequest>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_message(io).await
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<ClaimResponse>
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
    use kabudachi_core::protocol::messages::{Claim, claim_response};
    use libp2p::futures::io::Cursor;
    use libp2p::request_response::Codec as _;

    use super::*;
    use crate::framing::MAX_MESSAGE_BYTES;

    #[tokio::test]
    async fn read_request_rejects_a_length_prefix_over_the_maximum() {
        let mut codec = ClaimCodec;
        let mut oversized_prefix = Cursor::new((MAX_MESSAGE_BYTES + 1).to_be_bytes().to_vec());

        let result = codec.read_request(&PROTOCOL, &mut oversized_prefix).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn read_response_rejects_a_length_prefix_over_the_maximum() {
        let mut codec = ClaimCodec;
        let mut oversized_prefix = Cursor::new((MAX_MESSAGE_BYTES + 1).to_be_bytes().to_vec());

        let result = codec.read_response(&PROTOCOL, &mut oversized_prefix).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn read_request_rejects_a_request_that_asks_for_nothing() {
        let mut codec = ClaimCodec;

        let mut written = Cursor::new(Vec::new());
        codec
            .write_request(&PROTOCOL, &mut written, ClaimRequest { request: None })
            .await
            .expect("writing a request never fails against an in-memory buffer");
        let mut to_read = Cursor::new(written.into_inner());
        let result = codec.read_request(&PROTOCOL, &mut to_read).await;

        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn read_response_rejects_an_accept_missing_its_task_run_id() {
        let mut codec = ClaimCodec;
        let response = ClaimResponse {
            result: Some(claim_response::Result::Accept(Claim::default())),
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
