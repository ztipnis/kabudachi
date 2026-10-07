//! The `/kabudachi/task/1` `request_response` protocol: a length-prefixed,
//! prost-encoded `TaskRequest` (a submission, or a claimant's report that a
//! run started, completed, failed, or that a task is cancelled) as the
//! request, and a length-prefixed, prost-encoded `TaskResponse` as the
//! response.
//!
//! A separate `request_response::Behaviour` from the claim protocol's, for the
//! same reason that one is separate from the election and join protocols: a
//! genuine, correlated request/response, where the caller needs the specific
//! reply to its own request. `TaskRequest`/`TaskResponse` are themselves the
//! codec's `Request`/`Response` types.

use std::io;

use kabudachi_core::protocol::messages::{TaskRequest, TaskResponse};
use libp2p::StreamProtocol;
use libp2p::futures::{AsyncRead, AsyncWrite, AsyncWriteExt};
use libp2p::request_response;

use crate::framing::{encode_length_prefixed, read_message};

/// The sole protocol this codec negotiates.
pub const PROTOCOL: StreamProtocol = StreamProtocol::new("/kabudachi/task/1");

/// `request_response::Codec` for [`PROTOCOL`]. Stateless, so one instance (or
/// any number of clones) can be shared across connections.
#[derive(Debug, Clone, Copy, Default)]
pub struct TaskCodec;

impl request_response::Codec for TaskCodec {
    type Protocol = StreamProtocol;
    type Request = TaskRequest;
    type Response = TaskResponse;

    async fn read_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<TaskRequest>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_message(io).await
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<TaskResponse>
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
    use libp2p::futures::io::Cursor;
    use libp2p::request_response::Codec as _;

    use super::*;

    #[tokio::test]
    async fn read_request_rejects_a_request_that_asks_for_nothing() {
        let mut codec = TaskCodec;

        let mut written = Cursor::new(Vec::new());
        codec
            .write_request(&PROTOCOL, &mut written, TaskRequest { request: None })
            .await
            .expect("writing a request never fails against an in-memory buffer");
        let mut to_read = Cursor::new(written.into_inner());
        let result = codec.read_request(&PROTOCOL, &mut to_read).await;

        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }
}
