//! The `/kabudachi/join/1` `request_response` protocol (bootstrap
//! join): a length-prefixed, prost-encoded `JoinRequest` as the
//! request, and a length-prefixed, prost-encoded `JoinResponse` (the
//! responder's pointer to the shard's leader) as the response.
//!
//! A separate `request_response::Behaviour` from the election protocol's
//! (`crate::codec`) — see `net/src/swarm.rs`'s `Behaviour` — because a join
//! exchange is a real, correlated request/response (the caller needs the
//! specific reply to its own request, unlike `Net::send`'s
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
