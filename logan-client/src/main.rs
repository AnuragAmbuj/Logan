use anyhow::Result;
use clap::{Parser, Subcommand};
use logan_client::Client;
use std::net::SocketAddr;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:9092")]
    broker: SocketAddr,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Produce a message to a topic
    Produce {
        #[arg(short, long)]
        topic: String,
        #[arg(short, long)]
        message: String,
        #[arg(short, long, default_value = "0")]
        partition: i32,
    },
    /// Consume messages from a topic
    Consume {
        #[arg(short, long)]
        topic: String,
        #[arg(short, long, default_value = "0")]
        partition: i32,
    },
    /// Manage topics
    Topic {
        #[command(subcommand)]
        subcmd: TopicCommands,
    },
}

#[derive(Subcommand, Debug)]
enum TopicCommands {
    /// List all topics
    List,
    /// Create a new topic
    Create {
        #[arg(short, long)]
        topic: String,
        #[arg(short, long, default_value = "1")]
        partitions: i32,
        #[arg(short, long, default_value = "1")]
        replication_factor: i16,
    },
    /// Delete a topic
    Delete {
        #[arg(short, long)]
        topic: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    logan_common::logging::init_logging(tracing::Level::INFO);
    let args = Args::parse();

    let mut client = Client::connect(args.broker).await?;

    match args.command {
        Commands::Produce {
            topic,
            message,
            partition,
        } => {
            let response = client
                .produce(&topic, partition, message.as_bytes())
                .await?;
            println!(
                "Produced message to {}/{}: {:?}",
                topic, partition, response
            );
        }
        Commands::Consume { topic, partition } => {
            let response = client.fetch(&topic, partition).await?;
            for topic_resp in response.responses.0 {
                for part_resp in topic_resp.partitions.0 {
                    if let Some(records) = part_resp.records.0 {
                        println!("Consumed: {:?}", String::from_utf8_lossy(&records));
                    } else {
                        println!("No records found.");
                    }
                }
            }
        }
        Commands::Topic { subcmd } => match subcmd {
            TopicCommands::List => {
                let metadata = client.send_metadata_request().await?;
                for topic in metadata.topics.0 {
                    println!("Topic: {}", topic.name.0);
                }
            }
            TopicCommands::Create {
                topic,
                partitions,
                replication_factor: _,
            } => {
                // TODO: Support replication factor in client lib if needed, currently ignored by proto?
                // The client lib create_topics takes generic params, let's fix that usage
                let response = client.create_topics(vec![topic.clone()]).await?;
                println!("Create topic result: {:?}", response);
            }
            TopicCommands::Delete { topic } => {
                let response = client.delete_topics(vec![topic.clone()]).await?;
                println!("Delete topic result: {:?}", response);
            }
        },
    }

    Ok(())
}
