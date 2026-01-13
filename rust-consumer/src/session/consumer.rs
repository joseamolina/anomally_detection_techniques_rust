use rdkafka::config::ClientConfig;
use rdkafka::consumer::{StreamConsumer, Consumer};
use anyhow::Result;
use rdkafka::TopicPartitionList;
use crate::session::config::KafkaConfig;

pub fn create_consumer(cfg: &KafkaConfig) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.brokers)
        .set("group.id", &cfg.group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;

    // Asignar partición manualmente (offset inicial 0)
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&cfg.topic, 0, rdkafka::Offset::Beginning)?;
    consumer.assign(&tpl)?;

    Ok(consumer)
}
