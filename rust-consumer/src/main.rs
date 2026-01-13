use rdkafka::consumer::{Consumer, CommitMode};
use anyhow::Result;
use rust_consumer::session::{config, consumer, processor};

#[tokio::main]
async fn main() -> Result<()> {

    let configuration = config::KafkaConfig::from_env();
    let consumer = consumer::create_consumer(&configuration)?;

    println!("Consumiendo topic: {}", configuration.topic);

    loop {
        match consumer.recv().await {
            Err(e) => eprintln!("Error Kafka: {}", e),
            Ok(msg) => {
                processor::process_message(&msg)?;
                consumer.commit_message(&msg, CommitMode::Async)?;
            }
        }
    }
}
