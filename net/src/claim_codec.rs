//! The `/kabudachi/claim/1` `request_response` protocol (README §27 Phase 2
//! claim arbitration, chunk C6): a length-prefixed, prost-encoded
//! `ClaimRequest` as the request, and a length-prefixed, prost-encoded
//! `ClaimResponse` (either the accepted `Claim` or a `ClaimReject` naming one
//! of `core::scheduler::ClaimRejection`'s seven variants) as the response.
//!
//! A separate `request_response::Behaviour` from both the election
//! protocol's (`crate::codec`) and the bootstrap join protocol's
//! (`crate::join_codec`) — see `net/src/swarm.rs`'s `Behaviour` — for
//! exactly the reasons `join_codec`'s module doc already gives: this is a
//! genuine, correlated request/response (the caller needs the specific reply
//! to its own request), unlike `PeerMessenger::send`'s fire-and-forget,
//! uncorrelated delivery. `ClaimRequest`/`ClaimResponse` are themselves the
//! codec's `Request`/`Response` types directly, mirroring
//! `JoinRequest`/`JoinResponse` — there's only ever exactly one request shape
//! (`REQUEST_CLAIM(task_id)`), so no wrapping oneof envelope is needed there;
//! the response needs one, since it's either an accepted `Claim` or a
//! `ClaimReject`.

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
    use kabudachi_core::protocol::ids::{TaskId, TaskRunId};
    use kabudachi_core::protocol::messages::{Claim, ClaimReject, ClaimRejectReason, claim_response};
    use libp2p::futures::io::Cursor;
    use libp2p::request_response::Codec as _;

    use super::*;
    use crate::framing::MAX_MESSAGE_BYTES;

    #[tokio::test]
    async fn write_request_then_read_request_round_trips_the_message() {
        let mut codec = ClaimCodec;
        let request = ClaimRequest {
            task_id: Some(TaskId::new("task-1").into()),
        };

        let mut written = Cursor::new(Vec::new());
        codec
            .write_request(&PROTOCOL, &mut written, request.clone())
            .await
            .expect("writing a request never fails against an in-memory buffer");

        let mut to_read = Cursor::new(written.into_inner());
        let decoded = codec
            .read_request(&PROTOCOL, &mut to_read)
            .await
            .expect("reading back what was just written must succeed");

        assert_eq!(decoded, request);
    }

    #[tokio::test]
    async fn write_response_then_read_response_round_trips_an_accept() {
        let mut codec = ClaimCodec;
        let response = ClaimResponse {
            result: Some(claim_response::Result::Accept(Claim {
                task: None,
                task_run_id: Some(TaskRunId::new("run-1").into()),
                attempt_number: 1,
                chain: vec![],
            })),
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
    async fn write_response_then_read_response_round_trips_a_reject() {
        let mut codec = ClaimCodec;
        let response = ClaimResponse {
            result: Some(claim_response::Result::Reject(ClaimReject {
                reason: ClaimRejectReason::ClaimRejectAlreadySelected as i32,
            })),
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
    async fn read_request_rejects_a_request_missing_its_task_id() {
        let mut codec = ClaimCodec;

        let mut written = Cursor::new(Vec::new());
        codec
            .write_request(&PROTOCOL, &mut written, ClaimRequest { task_id: None })
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
