use rust_kafka_like::broker::Broker;
use rust_kafka_like::subscriber::types::Subscriber;
use std::sync::{Arc, Mutex};

fn main() {
    let broker = Arc::new(Mutex::new(Broker::new("broker1", 3, 2, "logs")));

    // Create a topic
    {
        let mut broker = broker.lock().unwrap();
        broker.create_topic("test_topic", None).unwrap();
    }

    // Add a subscriber
    let subscriber = Subscriber::new(
        "sub1",
        Box::new(|msg: String| {
            println!("Received message: {}", msg);
        }),
    );
    {
        let mut broker = broker.lock().unwrap();
        broker.subscribe("test_topic", subscriber).unwrap();
    }

    // Publish message
    {
        let mut broker = broker.lock().unwrap();
        broker
            .publish_with_ack("test_topic", "Hello, world!".to_string(), None)
            .unwrap();
    }
}
