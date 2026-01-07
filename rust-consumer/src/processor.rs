use rdkafka::message::BorrowedMessage;
use anyhow::Result;
use rdkafka::message::Message;

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
