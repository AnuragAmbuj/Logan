use dashmap::DashMap;
use std::sync::Arc;

/// A simple in-memory offset manager.
/// Keys: (Group ID, Topic Name, Partition Index)
/// Values: Committed Offset
#[derive(Debug, Clone)]
pub struct OffsetManager {
    offsets: Arc<DashMap<(String, String, i32), i64>>,
}

impl OffsetManager {
    pub fn new() -> Self {
        Self {
            offsets: Arc::new(DashMap::new()),
        }
    }

    pub fn commit_offset(&self, group_id: String, topic: String, partition: i32, offset: i64) {
        self.offsets.insert((group_id, topic, partition), offset);
    }

    pub fn fetch_offset(&self, group_id: &str, topic: &str, partition: i32) -> Option<i64> {
        self.offsets
            .get(&(group_id.to_string(), topic.to_string(), partition))
            .map(|val| *val)
    }
}
