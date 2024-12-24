# rust kafka-like

This is a Rust implementation of a distributed messaging system. It uses a simple design inspired by Apache Kafka. It simply records messages to local files.

## Features

- Topic-based pub/sub model
- Scalability through partitioning
- Persistent messages (log file based)
- Leader/Follower Replication
- Fault Detection and Automatic Recovery
- Delivery guaranteed by acknowledgement (ACK)
- Fully implemented leader selection mechanism
- Partition Replication
- Persistent messages
- Schema Registry for managing message schemas and ensuring compatibility

## Usage

### Dependency

- Rust 1.51.0 or later

### Basic usage

```rust
use rust_kafka_like::broker::Broker;
use rust_kafka_like::schema::registry::SchemaRegistry;
use rust_kafka_like::subscriber::types::Subscriber;
use std::sync::{Arc, Mutex};
use std::thread;

fn main() {
    // Create a schema registry
    let schema_registry = SchemaRegistry::new();
    let schema_def = r#"{"type":"record","name":"test","fields":[{"name":"id","type":"string"}]}"#;
    schema_registry.register_schema("test_topic", schema_def).unwrap();

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
        broker.publish_with_ack("test_topic", message, None).unwrap();
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
```

### Fault Detection and Automatic Recovery

The system includes mechanisms for fault detection and automatic recovery. Nodes are monitored using heartbeat signals, and if a fault is detected, the system will attempt to recover automatically.

```rust
use rust_kafka_like::broker::Broker;
use std::time::Duration;

fn main() {
    let broker = Broker::new("broker1", 3, 2, "logs");

    // Check node health
    if broker.detect_faults() {
        broker.recover_node();
    }
}
```

### Planned features(perhaps)

1. Add security features
2. High Availability and Scalability
3. Strengthening the mechanism for selecting leaders

### License

MIT

### Information

- main.rs: Main implementation
- logs: Persistent Message Directory

### Examples

To execute a basic example, use the following command:

```bash
cargo run --example simple-send-recv
cargo run --example mulch-send-recv
```
