use anyhow::Result;
use logan_storage::{Log, LogReadResult, config::LogConfig};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot};
use tracing::info;

pub type TopicPartition = (String, i32);

#[derive(Debug)]
pub enum ShardCommand {
    Append {
        topic_partition: TopicPartition,
        records: Vec<u8>, // Raw bytes or RecordBatch? server currently passes raw bytes.
        // We might want to pass RecordBatch if we move decoding to Shard?
        // For now, let's keep raw bytes as `Log::append` takes `&[u8]`.
        // But wait, `Log::append` takes `&[u8]`.
        resp_tx: oneshot::Sender<Result<u64>>, // Returns offset
    },
    Fetch {
        topic_partition: TopicPartition,
        offset: u64,
        max_bytes: i32,
        resp_tx: oneshot::Sender<Result<LogReadResult>>,
    },
    CreatePartition {
        topic_partition: TopicPartition,
        resp_tx: oneshot::Sender<Result<()>>,
    },
    // We can add Stop/Shutdown later
}

/// A Shard owns a subset of partitions and processes commands sequentially.
#[derive(Debug)]
pub struct Shard {
    id: usize,
    logs: HashMap<TopicPartition, Log>,
    log_dir: PathBuf,
    config: LogConfig,
    base_dir: PathBuf,
}

impl Shard {
    pub fn new(id: usize, base_dir: PathBuf, config: LogConfig) -> Self {
        Self {
            id,
            logs: HashMap::new(),
            log_dir: base_dir.clone(), // In reality we join topic-part
            base_dir,
            config,
        }
    }

    pub async fn run(mut self, mut rx: mpsc::Receiver<ShardCommand>) {
        info!("Shard {} started", self.id);
        while let Some(cmd) = rx.recv().await {
            match cmd {
                ShardCommand::Append {
                    topic_partition,
                    records,
                    resp_tx,
                } => {
                    let res = self.handle_append(topic_partition, records);
                    let _ = resp_tx.send(res);
                }
                ShardCommand::Fetch {
                    topic_partition,
                    offset,
                    max_bytes,
                    resp_tx,
                } => {
                    let res = self.handle_fetch(topic_partition, offset, max_bytes);
                    let _ = resp_tx.send(res);
                }
                ShardCommand::CreatePartition {
                    topic_partition,
                    resp_tx,
                } => {
                    let res = self.handle_create(topic_partition);
                    let _ = resp_tx.send(res);
                }
            }
        }
        info!("Shard {} stopped", self.id);
    }

    fn get_or_load_log(&mut self, tp: &TopicPartition) -> Result<&mut Log> {
        if self.logs.contains_key(tp) {
            return Ok(self.logs.get_mut(tp).unwrap());
        }

        // Try to load
        let (topic, partition) = tp;
        let subdir = self.base_dir.join(format!("{}-{}", topic, partition));
        if subdir.exists() {
            let log = Log::new(subdir, self.config.clone())?;
            self.logs.insert(tp.clone(), log);
            Ok(self.logs.get_mut(tp).unwrap())
        } else {
            Err(anyhow::anyhow!("Log not found"))
        }
    }

    fn handle_append(&mut self, tp: TopicPartition, records: Vec<u8>) -> Result<u64> {
        // Auto-create wrapper? LogManager did `get_or_create`.
        // Let's assume strict separation: Create must be called explicitly or we do it here.
        // LogManager logic: `get_or_create_log`.

        let log = if self.logs.contains_key(&tp) {
            self.logs.get_mut(&tp).unwrap()
        } else {
            // Try create/load
            let (topic, partition) = &tp;
            let subdir = self.base_dir.join(format!("{}-{}", topic, partition));
            // Optimization: Log::new creates dir if missing?
            let log = Log::new(subdir, self.config.clone())?;
            self.logs.insert(tp.clone(), log);
            self.logs.get_mut(&tp).unwrap()
        };

        log.append(&records).map(|o| o as u64)
    }

    fn handle_fetch(
        &mut self,
        tp: TopicPartition,
        offset: u64,
        max_bytes: i32,
    ) -> Result<LogReadResult> {
        match self.get_or_load_log(&tp) {
            Ok(log) => log.read(offset, max_bytes),
            Err(_) => Ok(LogReadResult::Data(Vec::new())), // Not found = empty
        }
    }

    fn handle_create(&mut self, tp: TopicPartition) -> Result<()> {
        let (topic, partition) = &tp;
        let subdir = self.base_dir.join(format!("{}-{}", topic, partition));
        let log = Log::new(subdir, self.config.clone())?;
        self.logs.insert(tp.clone(), log);
        Ok(())
    }
}

#[derive(Debug)]
pub struct ShardManager {
    shards: Vec<mpsc::Sender<ShardCommand>>,
    num_shards: usize,
}

impl ShardManager {
    pub async fn new(base_dir: PathBuf, config: LogConfig, num_shards: usize) -> Result<Self> {
        let mut senders = Vec::with_capacity(num_shards);

        for i in 0..num_shards {
            let (tx, rx) = mpsc::channel(1024); // Buffer size TODO: Configurable
            let shard = Shard::new(i, base_dir.clone(), config.clone());
            tokio::spawn(shard.run(rx));
            senders.push(tx);
        }

        Ok(Self {
            shards: senders,
            num_shards,
        })
    }

    fn get_shard_id(&self, topic: &str, partition: i32) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        topic.hash(&mut hasher);
        partition.hash(&mut hasher);
        (hasher.finish() as usize) % self.num_shards
    }

    pub async fn append(&self, topic: String, partition: i32, records: Vec<u8>) -> Result<u64> {
        let shard_id = self.get_shard_id(&topic, partition);
        let (tx, rx) = oneshot::channel();
        let cmd = ShardCommand::Append {
            topic_partition: (topic, partition),
            records,
            resp_tx: tx,
        };

        self.shards[shard_id]
            .send(cmd)
            .await
            .map_err(|_| anyhow::anyhow!("Shard closed"))?;
        rx.await?
    }

    pub async fn fetch(
        &self,
        topic: String,
        partition: i32,
        offset: u64,
        max_bytes: i32,
    ) -> Result<LogReadResult> {
        let shard_id = self.get_shard_id(&topic, partition);
        let (tx, rx) = oneshot::channel();
        let cmd = ShardCommand::Fetch {
            topic_partition: (topic, partition),
            offset,
            max_bytes,
            resp_tx: tx,
        };
        self.shards[shard_id]
            .send(cmd)
            .await
            .map_err(|_| anyhow::anyhow!("Shard closed"))?;
        rx.await?
    }

    pub async fn create_partition(&self, topic: String, partition: i32) -> Result<()> {
        let shard_id = self.get_shard_id(&topic, partition);
        let (tx, rx) = oneshot::channel();
        let cmd = ShardCommand::CreatePartition {
            topic_partition: (topic, partition),
            resp_tx: tx,
        };
        self.shards[shard_id]
            .send(cmd)
            .await
            .map_err(|_| anyhow::anyhow!("Shard closed"))?;
        rx.await?
    }
}
