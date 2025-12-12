use crate::config::LogConfig;
use crate::log::Log;
use anyhow::Result;
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct LogManager {
    log_dir: PathBuf,
    config: LogConfig,
    logs: DashMap<(String, i32), Arc<Mutex<Log>>>, // (topic, partition) -> Log
}

impl LogManager {
    pub fn new(log_dir: PathBuf, config: LogConfig) -> Result<Self> {
        std::fs::create_dir_all(&log_dir)?;

        let logs = DashMap::new();

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
                            if let Ok(log) = Log::new(log_path, config.clone()) {
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

        Ok(Self {
            log_dir,
            config,
            logs,
        })
    }

    pub fn read(
        &self,
        topic: &str,
        partition: i32,
        offset: u64,
        max_bytes: i32,
    ) -> Result<crate::LogReadResult> {
        let key = (topic.to_string(), partition);

        if let Some(log) = self.logs.get(&key) {
            let mut log = log.lock().unwrap();
            log.read(offset, max_bytes)
        } else {
            // Or return Error? Kafka returns specific error code usually handled by upper layer.
            // But here we return Result.
            Ok(crate::LogReadResult::Data(Vec::new()))
        }
    }

    pub fn get_or_create_log(&self, topic: &str, partition: i32) -> Result<Arc<Mutex<Log>>> {
        let key = (topic.to_string(), partition);
        if let Some(log) = self.logs.get(&key) {
            return Ok(log.clone());
        }

        let subdir = self.log_dir.join(format!("{}-{}", topic, partition));
        let log = Arc::new(Mutex::new(Log::new(subdir, self.config.clone())?));
        self.logs.insert(key, log.clone());
        Ok(log)
    }

    pub fn cleanup(&self) -> Result<()> {
        for entry in self.logs.iter() {
            let log = entry.value();
            let mut log = log.lock().expect("Log lock poisoned");
            if let Err(e) = log.cleanup() {
                // Log error but continue
                tracing::error!("Error cleaning up log {:?}: {}", entry.key(), e);
            }
        }
        Ok(())
    }
}
