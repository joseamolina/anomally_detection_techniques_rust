use std::{collections::HashMap};
use serde_json::Value;
use rust_consumer::session::{config, consumer, processor};

#[tokio::test]
pub async fn test_joke_endpoint_with_params() {
    let people = "Rustaceans";
    let value: String = format!("{{\"sensor\": \"0\", \"value\": \"{people}\"}}");

    let result= processor::write_to_table(&value);

    println!("{:?}", result);
}