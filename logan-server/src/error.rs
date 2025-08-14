//! Error types for the Logan server

use std::io;
use thiserror::Error;
use anyhow::Error as AnyhowError;

/// Main error type for the Logan server
#[derive(Error, Debug)]
pub enum ServerError {
    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Anyhow error
    #[error(transparent)]
    Anyhow(#[from] AnyhowError),

    /// Protocol error
    #[error("Protocol error: {0}")]
    Protocol(String),

    /// Invalid configuration
    #[error("Configuration error: {0}")]
    Config(String),

    /// Storage error
    #[error("Storage error: {0}")]
    Storage(String),

    /// Authentication error
    #[error("Authentication failed: {0}")]
    Auth(String),

    /// Authorization error
    #[error("Authorization failed: {0}")]
    Authorization(String),

    /// Request timeout
    #[error("Request timed out")]
    Timeout,

    /// Internal server error
    #[error("Internal server error: {0}")]
    Internal(String),

    /// Decoding error
    #[error("Decoding error: {0}")]
    DecodingError(String),

    /// Frame too large
    #[error("Frame too large: {0}")]
    FrameTooLarge(usize),

    /// Connection reset by peer
    #[error("Connection reset by peer")]
    ConnectionResetByPeer,

    /// Unsupported API key
    #[error("Unsupported API key: {0}")]
    UnsupportedApiKey(i16),
}

impl From<std::str::Utf8Error> for ServerError {
    fn from(err: std::str::Utf8Error) -> Self {
        ServerError::DecodingError(err.to_string())
    }
}

impl From<ServerError> for io::Error {
    fn from(err: ServerError) -> Self {
        match err {
            ServerError::Io(e) => e,
            _ => io::Error::new(io::ErrorKind::Other, err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_server_error_display() {
        let io_error = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let server_error = ServerError::Io(io_error);
        assert!(server_error.to_string().contains("I/O error"));

        let protocol_error = ServerError::Protocol("invalid message format".to_string());
        assert!(protocol_error.to_string().contains("Protocol error"));
    }
}
