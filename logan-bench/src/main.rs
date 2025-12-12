use anyhow::{Context, Result};
use bytes::Bytes;
use clap::{Parser, Subcommand};
use hdrhistogram::Histogram;
use logan_client::Client;
use logan_protocol::batch::{RecordBatch, SimpleRecord};
use logan_protocol::codec::Encodable;
use logan_protocol::compression::CompressionType;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Broker address
    #[arg(short, long, default_value = "127.0.0.1:9092")]
    broker: String,
}

#[derive(Subcommand)]
enum Commands {
    /// Run producer benchmark
    Produce {
        /// Topic to produce to
        #[arg(short, long, default_value = "bench-topic")]
        topic: String,

        /// Message size in bytes
        #[arg(short, long, default_value_t = 100)]
        size: usize,

        /// Number of messages to produce per thread/task
        #[arg(short, long, default_value_t = 100000)]
        count: usize,

        /// Parallelism (concurrent requests)
        #[arg(short, long, default_value_t = 1)]
        parallelism: usize,

        /// Number of partitions to distribute load across
        #[arg(long, default_value_t = 12)]
        partitions: i32,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    // Parse address
    let addr: SocketAddr = cli
        .broker
        .parse()
        .expect("Invalid broker address format (e.g., 127.0.0.1:9092)");

    match cli.command {
        Commands::Produce {
            topic,
            size,
            count,
            parallelism,
            partitions,
        } => {
            println!("Starting Produce Benchmark");
            println!("Target: {}", addr);
            println!("Topic: {}", topic);
            println!(
                "Size: {} bytes, Count: {}, Parallelism: {}, Partitions: {}",
                size, count, parallelism, partitions
            );

            // Create a valid RecordBatch payload
            let record_value = Bytes::from(vec![b'a'; size]);
            let simple_record = SimpleRecord {
                key: None,
                value: Some(record_value),
                headers: vec![],
                offset_delta: 0,
                timestamp_delta: 0,
            };

            // Build batch
            // Base offset 0 is fine for produce requests (broker assigns it)
            let batch = RecordBatch::new(0, CompressionType::None, vec![simple_record]);

            // Encode batch to bytes
            let mut payload_buf = Vec::new();
            batch
                .encode(&mut payload_buf)
                .context("Failed to encode batch")?;
            let payload = payload_buf;

            // Test connection once before spawning tasks
            let _ = Client::connect(addr).await?;

            let mut handles = vec![];
            let start = Instant::now();
            let histogram = Arc::new(Mutex::new(Histogram::<u64>::new(3).unwrap()));

            let messages_per_task = count / parallelism;

            for i in 0..parallelism {
                let topic = topic.clone();
                let payload = payload.clone();
                let histogram = histogram.clone();
                let addr = addr; // Copy

                handles.push(tokio::spawn(async move {
                    // Create a dedicated client per task
                    let mut client = Client::connect(addr).await.unwrap();

                    // Round-robin start partition for this thread
                    let mut p_idx = (i as i32) % partitions;

                    for _ in 0..messages_per_task {
                        let t0 = Instant::now();

                        // Use correct partition
                        let _ = client.produce(&topic, p_idx, &payload).await;

                        let latency = t0.elapsed().as_micros() as u64;
                        histogram.lock().await.record(latency).unwrap();

                        // Rotate partition
                        p_idx = (p_idx + 1) % partitions;
                    }
                }));
            }

            for h in handles {
                h.await?;
            }

            let elapsed = start.elapsed();
            // Total messages produced
            let total_messages = messages_per_task * parallelism;
            let throughput = total_messages as f64 / elapsed.as_secs_f64();
            let histo = histogram.lock().await;

            println!("\n--- Results ---");
            println!("Elapsed: {:.2?}", elapsed);
            println!("Throughput: {:.2} msgs/sec", throughput);
            println!("Min Latency: {} us", histo.min());
            println!("Mean Latency: {:.2} us", histo.mean());
            println!("P50 Latency: {} us", histo.value_at_quantile(0.5));
            println!("P99 Latency: {} us", histo.value_at_quantile(0.99));
            println!("P99.9 Latency: {} us", histo.value_at_quantile(0.999));
            println!("Max Latency: {} us", histo.max());
        }
    }

    Ok(())
}
