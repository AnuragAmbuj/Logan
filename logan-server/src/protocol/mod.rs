//! Kafka protocol definitions

pub mod api_keys;
pub mod codec;
pub mod messages;
pub mod primitives;
pub mod error_codes;

pub use api_keys::*;
pub use codec::*;
pub use messages::*;
pub use primitives::*;
pub use error_codes::*;

/*
/// Kafka protocol version
pub(crate) type ApiVersion = i16;

/// Correlation ID for matching requests and responses
pub(crate) type CorrelationId = i32;

/// Client ID string
pub(crate) type ClientId = String;

/// Error that can occur during protocol parsing
#[derive(Debug, thiserror::Error)]
pub(crate) enum ProtocolError {
    /// The message is too short
    #[error("Message too short")]
    MessageTooShort,
    /// The message is too long
    #[error("Message too long")]
    MessageTooLong,
    /// The API key is unknown
    #[error("Unknown API key: {0}")]
    UnknownApiKey(i16),
    /// The API version is not supported
    #[error("Unsupported API version: {0}")]
    UnsupportedVersion(i16),
    /// The message is malformed
    #[error("Malformed message: {0}")]
    MalformedMessage(String),
    /// An I/O error occurred
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
*/

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_request_parse() {
        // TODO: Add tests for request parsing
    }
}
