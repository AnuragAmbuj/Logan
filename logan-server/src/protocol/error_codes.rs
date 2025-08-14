//! Kafka protocol error codes

use bytes::BufMut;
use crate::protocol::Encodable;
use crate::ServerError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i16)]
pub enum ErrorCode {
    UnknownServerError = -1,
    None = 0,
    OffsetOutOfRange = 1,
    // Add other error codes as needed
}

impl Default for ErrorCode {
    fn default() -> Self {
        ErrorCode::None
    }
}

impl Encodable for ErrorCode {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        (*self as i16).encode(buf)
    }
}
