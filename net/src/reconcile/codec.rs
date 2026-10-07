//! The `/kabudachi/reconcile/1` `request_response` protocol: a
//! length-prefixed, prost-encoded `ReconcileRequest` (a new leader asking
//! one worker for a page of what it holds) as the request, and a
//! length-prefixed, prost-encoded `ReconcileReport` (that page) as the
//! response.
//!
//! A separate `request_response::Behaviour` from the others, for the same
//! reason the task protocol is: a correlated request/response whose caller
//! needs the reply to its own request. Neither message has a field whose
//! absence a reader cannot handle (a request without a cursor asks for the
//! first page, and a page's runs and keys are checked as they are turned
//! into `core::reconcile` types), so the codec only decodes.

use std::io;

use kabudachi_core::protocol::messages::{ReconcileReport, ReconcileRequest};
use libp2p::StreamProtocol;
use libp2p::futures::{AsyncRead, AsyncWrite, AsyncWriteExt};
use libp2p::request_response;
use prost::Message as _;

use crate::framing::{encode_length_prefixed, read_frame};

/// The sole protocol this codec negotiates.
pub const PROTOCOL: StreamProtocol = StreamProtocol::new("/kabudachi/reconcile/1");

/// `request_response::Codec` for [`PROTOCOL`]. Stateless, so one instance (or
/// any number of clones) can be shared across connections.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReconcileCodec;

impl request_response::Codec for ReconcileCodec {
    type Protocol = StreamProtocol;
    type Request = ReconcileRequest;
    type Response = ReconcileReport;

    async fn read_request<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<ReconcileRequest>
    where
        T: AsyncRead + Unpin + Send,
    {
        ReconcileRequest::decode(&read_frame(io).await?[..]).map_err(io::Error::other)
    }

    async fn read_response<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<ReconcileReport>
    where
        T: AsyncRead + Unpin + Send,
    {
        ReconcileReport::decode(&read_frame(io).await?[..]).map_err(io::Error::other)
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
