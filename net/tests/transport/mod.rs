//! The transport under a `Net`: the limits its codecs and its input queue enforce and how it
//! connects to peers over real sockets.

mod addresses;
mod input_queue;
mod message_limit;
mod results;
mod simultaneous_dial;
