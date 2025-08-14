//! Kafka protocol error codes

use std::fmt;

/// Kafka protocol error codes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorCode {
    /// No error
    None = 0,
    /// The requested offset is outside the range of offsets maintained by the server
    OffsetOutOfRange = 1,
    /// This message has failed its CRC checksum or is otherwise corrupt
    CorruptMessage = 2,
    /// This server does not host this topic-partition
    UnknownTopicOrPartition = 3,
    /// The requested fetch size is invalid
    InvalidFetchSize = 4,
    /// There is no leader for this topic-partition as we are in the middle of a leadership election
    LeaderNotAvailable = 5,
    /// This server is not the leader for that topic-partition
    NotLeaderForPartition = 6,
    /// The request timed out
    RequestTimedOut = 7,
    /// The broker is not available
    BrokerNotAvailable = 8,
    /// The replica is not available for the requested topic-partition
    ReplicaNotAvailable = 9,
    // ... other error codes as needed
}

impl ErrorCode {
    /// Convert an i16 to an ErrorCode
    pub fn from_i16(code: i16) -> Option<Self> {
        match code {
            0 => Some(ErrorCode::None),
            1 => Some(ErrorCode::OffsetOutOfRange),
            2 => Some(ErrorCode::CorruptMessage),
            3 => Some(ErrorCode::UnknownTopicOrPartition),
            4 => Some(ErrorCode::InvalidFetchSize),
            5 => Some(ErrorCode::LeaderNotAvailable),
            6 => Some(ErrorCode::NotLeaderForPartition),
            7 => Some(ErrorCode::RequestTimedOut),
            8 => Some(ErrorCode::BrokerNotAvailable),
            9 => Some(ErrorCode::ReplicaNotAvailable),
            _ => None,
        }
    }

    /// Convert an ErrorCode to an i16
    pub fn to_i16(&self) -> i16 {
        *self as i16
    }
}

impl std::error::Error for ErrorCode {}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = match self {
            ErrorCode::None => "No error",
            ErrorCode::OffsetOutOfRange => "The requested offset is outside the range of offsets maintained by the server",
            ErrorCode::CorruptMessage => "This message has failed its CRC checksum or is otherwise corrupt",
            ErrorCode::UnknownTopicOrPartition => "This server does not host this topic-partition",
            ErrorCode::InvalidFetchSize => "The requested fetch size is invalid",
            ErrorCode::LeaderNotAvailable => "There is no leader for this topic-partition as we are in the middle of a leadership election",
            ErrorCode::NotLeaderForPartition => "This server is not the leader for that topic-partition",
            ErrorCode::RequestTimedOut => "The request timed out",
            ErrorCode::BrokerNotAvailable => "The broker is not available",
            ErrorCode::ReplicaNotAvailable => "The replica is not available for the requested topic-partition",
        };
        write!(f, "{}", msg)
    }
}

impl TryFrom<i16> for ErrorCode {
    type Error = String;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        Self::from_i16(value).ok_or_else(|| format!("Unknown error code: {}", value))
    }
}

impl From<ErrorCode> for i16 {
    fn from(code: ErrorCode) -> Self {
        code as i16
    }
}
