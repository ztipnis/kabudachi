//! The message-size limit and body decoding shared by every length-prefixed
//! `request_response::Codec` in this crate ([`crate::codec`],
//! [`crate::join_codec`], [`crate::claim::codec`]) — each frames its
//! messages the same way (a 4-byte big-endian length prefix followed by a
//! prost-encoded body). The framing lives here once, so every codec enforces
//! the same limit and error kinds (`STYLE_GUIDE.md`'s "defaults live in one
//! place"). A gossip message's body goes through the same decoding
//! ([`decode_election`]); gossipsub frames it itself.

use std::io;

use kabudachi_core::protocol::checked::{self, CheckedMessage};
use kabudachi_core::protocol::messages::{ElectionMessage, WellFormed};
use libp2p::futures::{AsyncRead, AsyncReadExt};

/// Requests/responses larger than this are rejected outright rather than
/// causing an allocation sized by an attacker- or bug-controlled length
/// prefix.
pub(crate) const MAX_MESSAGE_BYTES: u32 = 1024 * 1024;

// The scheduler refuses a task too big for one claim message by this size, so
// the two must agree.
const _: () = assert!(
    MAX_MESSAGE_BYTES as u64 == kabudachi_core::scheduler::MAX_CLAIM_FRAME_BYTES,
    "the scheduler's claim frame size is not this crate's message limit"
);

/// Decodes a frame body and rejects a malformed message: one missing a
/// required ID, or holding a field without the partner it needs
/// (`core::protocol::messages::WellFormed`). The required ID accessors panic
/// on an absent field, and any peer that completes the Noise handshake can
/// send one, so a malformed message stops here as `InvalidData` instead of
/// reaching `run_driver`.
pub(crate) fn decode_well_formed<M>(body: &[u8]) -> io::Result<M>
where
    M: prost::Message + Default + WellFormed,
{
    let message = M::decode(body).map_err(io::Error::other)?;
    if !message.is_well_formed() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} is not well formed", std::any::type_name::<M>()),
        ));
    }
    Ok(message)
}

/// Decodes a frame body as an election message and checks it
/// ([`checked::decode`]): a malformed one stops here as `InvalidData` instead
/// of reaching `run_driver`, for the same reason as [`decode_well_formed`].
/// Election messages have no [`WellFormed`] impl: they reach a node only as a
/// [`CheckedMessage`].
pub(crate) fn decode_election(body: &[u8]) -> io::Result<CheckedMessage> {
    let message = <ElectionMessage as prost::Message>::decode(body).map_err(io::Error::other)?;
    checked::decode(message).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

/// A 4-byte big-endian length prefix followed by the prost-encoded message.
pub(crate) fn encode_length_prefixed(message: &impl prost::Message) -> Vec<u8> {
    let body = message.encode_to_vec();
    let len = u32::try_from(body.len()).expect("a message never encodes to >4GiB");
    let mut framed = Vec::with_capacity(4 + body.len());
    framed.extend_from_slice(&len.to_be_bytes());
    framed.extend_from_slice(&body);
    framed
}

/// Reads one length-prefixed frame body. A length over [`MAX_MESSAGE_BYTES`]
/// is rejected before any body is allocated.
pub(crate) async fn read_frame<T>(io: &mut T) -> io::Result<Vec<u8>>
where
    T: AsyncRead + Unpin + Send,
{
    let mut len_bytes = [0u8; 4];
    io.read_exact(&mut len_bytes).await?;
    let len = u32::from_be_bytes(len_bytes);
    if len > MAX_MESSAGE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("message length {len} exceeds the {MAX_MESSAGE_BYTES}-byte maximum"),
        ));
    }
    let mut body = vec![0u8; len as usize];
    io.read_exact(&mut body).await?;
    Ok(body)
}

/// Reads one length-prefixed frame and decodes it with
/// [`decode_well_formed`].
pub(crate) async fn read_message<M, T>(io: &mut T) -> io::Result<M>
where
    M: prost::Message + Default + WellFormed,
    T: AsyncRead + Unpin + Send,
{
    decode_well_formed(&read_frame(io).await?)
}
