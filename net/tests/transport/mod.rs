//! The transport under a `Net`: the limits its codecs enforce and how it
//! connects to peers over real sockets.

mod addresses;
mod message_limit;
mod simultaneous_dial;
