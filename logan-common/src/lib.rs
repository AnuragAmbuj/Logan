//! Common types and utilities for the Logan Kafka broker

#![forbid(unsafe_code)]
#![warn(
    missing_docs,
    missing_debug_implementations,
    rust_2018_idioms,
    unreachable_pub
)]

pub mod error;
pub mod error_code;

/// Re-export commonly used items
pub use error::{Error, Result};
pub use error_code::ErrorCode;

/// A type alias for byte vectors
pub type Bytes = Vec<u8>;

/// A type alias for byte slices
pub type BytesSlice<'a> = &'a [u8];

/// A type alias for string types
pub type Str = str;

/// A type alias for string slices
pub type StrSlice<'a> = &'a str;

/// A type alias for string values
pub type String = std::string::String;

/// A type alias for string references
pub type StringRef<'a> = &'a std::string::String;

/// A type alias for string options
pub type StringOption = Option<String>;

/// A type alias for string references in options
pub type StringOptionRef<'a> = Option<&'a str>;

/// A type alias for string results
pub type StringResult = Result<String>;

/// A type alias for unit results
pub type UnitResult = Result<()>;

/// A type alias for boolean results
pub type BoolResult = Result<bool>;

/// A type alias for numeric results
pub type NumResult<T> = Result<T>;

/// A type alias for optional results
pub type OptionResult<T> = Result<Option<T>>;

/// A type alias for vector results
pub type VecResult<T> = Result<Vec<T>>;

/// A type alias for hash map results
pub type HashMapResult<K, V> = Result<std::collections::HashMap<K, V>>;

/// A type alias for hash set results
pub type HashSetResult<T> = Result<std::collections::HashSet<T>>;

/// A type alias for binary heap results
pub type BinaryHeapResult<T> = Result<std::collections::BinaryHeap<T>>;

/// A type alias for B-tree map results
pub type BTreeMapResult<K, V> = Result<std::collections::BTreeMap<K, V>>;

/// A type alias for B-tree set results
pub type BTreeSetResult<T> = Result<std::collections::BTreeSet<T>>;

/// A type alias for linked list results
pub type LinkedListResult<T> = Result<std::collections::LinkedList<T>>;

/// A type alias for vecdeque results
pub type VecDequeResult<T> = Result<std::collections::VecDeque<T>>;

/// A type alias for boxed results
pub type BoxResult<T> = Result<Box<T>>;

/// A type alias for boxed dynamic error results
pub type BoxDynErrorResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// A type alias for boxed dynamic error results with a static lifetime
pub type BoxDynErrorStaticResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync + 'static>>;

/// A type alias for boxed dynamic error results with a static lifetime and send + sync
pub type BoxDynErrorSendSyncResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync + 'static>>;

/// A type alias for boxed dynamic error results with a static lifetime and send + sync + 'static
pub type BoxDynErrorSendSyncStaticResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync + 'static>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_conversion() {
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let error: Error = io_error.into();
        assert!(matches!(error, Error::Io(_)));
        
        let json_error: Error = serde_json::from_str::<i32>("invalid").unwrap_err().into();
        assert!(matches!(json_error, Error::Serialization(_)));
    }
}
