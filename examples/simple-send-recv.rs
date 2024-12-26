use pilgrimage::broker::Broker;
use pilgrimage::schema::registry::SchemaRegistry;
use pilgrimage::subscriber::types::Subscriber;
use std::sync::{Arc, Mutex};
use std::thread;

fn main() {
    // Create a schema registry
    let schema_registry = SchemaRegistry::new();
    let schema_def = r#"{"type":"record","name":"test","fields":[{"name":"id","type":"string"}]}"#;
    schema_registry
        .register_schema("test_topic", schema_def)
        .unwrap();

    // Create a broker
    let broker = Arc::new(Mutex::new(Broker::new("broker1", 3, 2, "logs")));

    // Create a topic
    {
        let mut broker = broker.lock().unwrap();
        broker.create_topic("test_topic", None).unwrap();
    }

    // Create a producer
    let broker_producer = Arc::clone(&broker);
    let producer_handle = thread::spawn(move || {
        let message = "test_message".to_string();
        let mut broker = broker_producer.lock().unwrap();
        broker
            .publish_with_ack("test_topic", message, None)
            .unwrap();
    });

    // Create a consumer
    let broker_consumer = Arc::clone(&broker);
    let consumer_handle = thread::spawn(move || {
        let subscriber = Subscriber::new(
            "consumer_1",
            Box::new(move |msg: String| {
                println!("Consumed message: {}", msg);
            }),
        );
        broker_consumer
            .lock()
            .unwrap()
            .subscribe("test_topic", subscriber, Some("group1"))
            .unwrap();
    });

    // Wait for producer and consumer to finish
    producer_handle.join().unwrap();
    consumer_handle.join().unwrap();
}
