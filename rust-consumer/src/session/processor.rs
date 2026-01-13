use rdkafka::message::BorrowedMessage;
use rdkafka::message::Message;
use arrow::datatypes::{Schema, Field, DataType};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use serde_json::Value;
use arrow_array::{ArrayRef, Int32Array, StringArray};
use anyhow::Result;
use deltalake::{DeltaTable, DeltaTableBuilder};
use deltalake::parquet::arrow::async_writer::AsyncFileWriter;
use http::Uri;


pub fn write_to_table(json_str: &str) -> Result<RecordBatch> {
    let fields = vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ];
    let schema = Arc::new(Schema::new(fields));

    // Parsing from String to JSON
    let v: Value = serde_json::from_str(json_str)?;

    // Retrieving each value and parsing to specific type
    let id: i32 = v["id"].as_i64().unwrap_or(0) as i32;
    let value: &str = v["value"].as_str().unwrap_or("");

    // We are defining ArrayRef for a single specific value.
    let id_array = Arc::new(Int32Array::from(vec![id])) as ArrayRef;
    let value_array = Arc::new(StringArray::from(vec![value])) as ArrayRef;

    // Creation of a batch based from a vector that actually defines the number of columns by vectoring
    // all columns
    let batch = RecordBatch::try_new(schema, vec![id_array, value_array])?;

    Ok(batch)
}

fn store_value_to_delta(path: String, batch: RecordBatch) -> DeltaTable {
    let table = DeltaTableBuilder::from_url(path.parse::<Uri>().unwrap())
        .build()
        .unwrap();

    let ops = DeltaTable::from(table);

    let commit_result = ops.write(vec![batch.clone()]).await.unwrap();
    commit_result
}

pub fn process_message(msg: &BorrowedMessage) -> Result<()> {
    if let Some(payload) = msg.payload() {
        let value = String::from_utf8_lossy(payload);
        println!(
            "partition={}, offset={}, value={}",
            msg.partition(),
            msg.offset(),
            value
        );

        match write_to_table(&value) {
            Ok(batch) => {
                println!("RecordBatch creado exitosamente con {} filas.", batch.num_rows());
                // Aquí podrías guardar el RecordBatch en Delta Lake u otro destino
            }
            Err(e) => {
                eprintln!("Error parsing the value to RecordBatch: {}", e);
            }
        }
    }

    Ok(())
}
