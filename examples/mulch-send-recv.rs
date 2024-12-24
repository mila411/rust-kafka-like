use rust_kafka_like::broker::Broker;
use rust_kafka_like::schema::registry::SchemaRegistry;
use rust_kafka_like::subscriber::types::Subscriber;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::SystemTime;

fn simple_rng(seed: u64) -> u64 {
    // Pseudo-random number generation using the linear congruential method
    const A: u64 = 6364136223846793005;
    const C: u64 = 1;
    const M: u64 = 1 << 31;
    (A.wrapping_mul(seed).wrapping_add(C)) % M
}

fn main() {
    // Create a schema registry
    let schema_registry = SchemaRegistry::new();
    let schema_def = r#"{"type":"record","name":"test","fields":[{"name":"id","type":"string"}]}"#;
    schema_registry
        .register_schema("test_topic", schema_def)
        .unwrap();

    let _peers: HashSet<String> = ["peer1".to_string(), "peer2".to_string()]
        .iter()
        .cloned()
        .collect();
    let broker = Arc::new(Mutex::new(Broker::new("broker1", 3, 2, "logs")));

    // Create a topic
    {
        let mut broker = broker.lock().unwrap();
        broker.create_topic("test_topic", None).unwrap();
    }

    // Create multiple producers
    let broker_producer: Arc<Mutex<Broker>> = Arc::clone(&broker);
    let producer_handles: Vec<_> = (0..3)
        .map(|_i| {
            let broker_producer: Arc<Mutex<Broker>> = Arc::clone(&broker_producer);
            thread::spawn(move || {
                let mut seed = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                for _ in 0..10 {
                    let message = format!("message_{}", simple_rng(seed));
                    let mut broker = broker_producer.lock().unwrap();
                    broker
                        .publish_with_ack("test_topic", message, None)
                        .unwrap();
                    seed = simple_rng(seed);
                }
            })
        })
        .collect();

    // Create multiple consumers
    let num_consumers = 3;
    let broker_consumer: Arc<Mutex<Broker>> = Arc::clone(&broker);
    let consumer_handles: Vec<_> = (0..num_consumers)
        .map(|i| {
            let broker_consumer: Arc<Mutex<Broker>> = Arc::clone(&broker_consumer);
            thread::spawn(move || {
                let subscriber = Subscriber::new(
                    &format!("consumer_{}", i),
                    Box::new(move |msg: String| {
                        println!("Consumer {}: Consumed message: {}", i, msg);
                    }),
                );
                broker_consumer
                    .lock()
                    .unwrap()
                    .subscribe("test_topic", subscriber, Some("group1"))
                    .unwrap();
            })
        })
        .collect();

    // Wait for all producers to finish
    for handle in producer_handles {
        handle.join().unwrap();
    }

    // Wait for all consumers to finish
    for handle in consumer_handles {
        handle.join().unwrap();
    }
}
