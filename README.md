# rust kafka-like

⚠️⚠️⚠️<br>
This is a toy I created to play with Rust and kubernetes. Please note that it lacks many features that are Production Ready<br>
⚠️⚠️⚠️

This is a Rust implementation of a distributed messaging system. It uses a simple design inspired by Apache Kafka. It simply records messages to local files.

## Features

- Topic-based pub/sub model
- Scalability through partitioning
- Persistent messages (log file based)
- Leader/Follower Replication
- Fault Detection and Automatic Recovery
- Delivery guaranteed by acknowledgement (ACK)

## Usage

### Dependency

- Rust 1.51.0 or later

### Basic usage

```rust
// Broker initialization
let broker = Broker::new(
    "broker1".to_string(),
    "logs",
    3,
    peers,
    2
)?;

// Creating a topic
broker.create_topic("test_topic", Some(3))?;

// Publish Message
broker.publish_with_ack(
    "test_topic",
    "Test message".to_string(),
    None
)?;

// Subscriber registration
broker.subscribe("test_topic", Box::new(|message, ack| {
    println!("Received: {}", message);
    // ACK transmission
}))?;
```

### Planned features(perhaps)

1. Strengthening message persistence
1. Fully implemented leader selection mechanism
1. Partition Replication
1. Improvements in fault detection and automatic recovery

### License

MIT

### Information

- main.rs: Main implementation
- logs: Persistent Message Directory
