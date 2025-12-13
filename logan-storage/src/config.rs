#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupPolicy {
    Delete,
    Compact,
}

impl Default for CleanupPolicy {
    fn default() -> Self {
        CleanupPolicy::Delete
    }
}

#[derive(Debug, Clone)]
pub struct LogConfig {
    /// Maximum total size of a log partition before deleting old segments.
    pub retention_bytes: Option<u64>,
    /// Maximum age of a segment file in milliseconds before deletion.
    /// Using ms for easier testing, can map to hours in higher level config.
    pub retention_ms: Option<u64>,
    /// The cleanup policy for this log (Delete or Compact).
    pub cleanup_policy: CleanupPolicy,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            retention_bytes: None,
            retention_ms: None,
            cleanup_policy: CleanupPolicy::default(),
        }
    }
}
