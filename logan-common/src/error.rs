//! Common error types for the Logan Kafka broker


/// A specialized `Result` type for Logan operations
pub type Result<T> = std::result::Result<T, Error>;

/// The error type for Logan operations
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An I/O error occurred
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    
    /// A serialization/deserialization error occurred
    #[error("Serialization error: {0}")]
    Serialization(String),
    
    /// A protocol error occurred
    #[error("Protocol error: {0}")]
    Protocol(String),
    
    /// An invalid argument was provided
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    
    /// A resource was not found
    #[error("Not found: {0}")]
    NotFound(String),
    
    /// A resource already exists
    #[error("Already exists: {0}")]
    AlreadyExists(String),
    
    /// A precondition was not met
    #[error("Precondition failed: {0}")]
    PreconditionFailed(String),
    
    /// An operation was attempted on a closed resource
    #[error("Resource closed: {0}")]
    ResourceClosed(String),
    
    /// An internal error occurred
    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Serialization(err.to_string())
    }
}

impl From<Error> for std::io::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Io(e) => e,
            e => std::io::Error::new(std::io::ErrorKind::Other, e),
        }
    }
}
