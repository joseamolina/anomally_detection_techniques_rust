use rdkafka::message::BorrowedMessage;
use anyhow::Result;
use rdkafka::message::Message;
use deltalake::operations::write::DeltaOps;
use arrow::datatypes::{Schema, Field, DataType};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

fn write_to_table(value: String) -> Result<()> {
    let fields = vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ];
    let schema = Arc::new(Schema::new(fields));

    // Construir un RecordBatch con datos
    let ids = arrow::array::Int32Array::from(vec![1, 2, 3]);
    let values = arrow::array::StringArray::from(vec!["a", "b", "c"]);
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(ids),
        Arc::new(values),
    ])?;

    // Escribir (create/append)
    let table_uri = "../path/to/delta-table";
    let ops = DeltaOps::try_from_uri(table_uri).await?;
    // Por defecto `write` crea la tabla si no existe / append si existe
    ops.write(vec![batch]).await?;

    Ok(())
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

        // Aquí:
        // - parsear JSON / Avro
        // - escribir a filesystem
        // - enviar a otro sistema
    }

    Ok(())
}
