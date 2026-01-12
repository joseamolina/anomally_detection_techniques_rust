use std::env;

pub struct KafkaConfig {
    pub brokers: String,
    pub group_id: String,
    pub topic: String,
}

impl KafkaConfig {
    pub fn from_env() -> Self {
        Self {
            brokers: env::var("KAFKA_BROKERS")
                .unwrap_or_else(|_| "localhost:9092".into()),
            group_id: env::var("KAFKA_GROUP").unwrap_or_else(|_| "orders-producer".into()),
            topic: env::var("KAFKA_TOPIC")
                .unwrap_or_else(|_| "events".into()),
        }
    }
}
