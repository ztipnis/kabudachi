//! The `/kabudachi/claim/1` `request_response` protocol (claim
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
