use crate::log::Log;
use anyhow::Result;
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct LogManager {
    log_dir: PathBuf,
    logs: DashMap<(String, i32), Arc<Mutex<Log>>>, // (topic, partition) -> Log
}

impl LogManager {
    pub fn new(log_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&log_dir)?;

        // TODO: Scan log_dir to recover existing logs?
        // For now, we load them lazily or when requested.
        // But to properly support "list topics" we might need to scan.
        // Given the requirement is to replace in-memory with persistent, lazy loading is fine if we assume requests drive creation.
        // But existing data won't be visible until accessed.
        // Better: Scan directory on startup.

        let logs = DashMap::new();
        // Structure: log_dir / topic-partition /
        // We can scan directories.

        for entry in std::fs::read_dir(&log_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    // Parse "topic-partition"
                    if let Some(idx) = name.rfind('-') {
                        let topic = &name[0..idx];
                        if let Ok(partition) = name[idx + 1..].parse::<i32>() {
                            let log_path = entry.path();
                            // Try to load log
                            if let Ok(log) = Log::new(log_path) {
                                logs.insert(
                                    (topic.to_string(), partition),
                                    Arc::new(Mutex::new(log)),
                                );
                            }
                        }
                    }
                }
            }
        }

        Ok(Self { log_dir, logs })
    }

    pub fn get_or_create_log(&self, topic: &str, partition: i32) -> Result<Arc<Mutex<Log>>> {
        let key = (topic.to_string(), partition);
        if let Some(log) = self.logs.get(&key) {
            return Ok(log.clone());
        }

        let subdir = self.log_dir.join(format!("{}-{}", topic, partition));
        let log = Arc::new(Mutex::new(Log::new(subdir)?));
        self.logs.insert(key, log.clone());
        Ok(log)
    }
}
