<p align="center">
  <img src=".github/images/logo.png" alt="logo" width=65%>
</p>

<p align="center">
    <em>
        Very few dependencies
    </em>
</p>

<p align="center">
    <a href="https://blog.rust-lang.org/2024/11/28/Rust-1.83.0.html">
      <img src="https://img.shields.io/badge/Rust-1.83-007ACC.svg?logo=Rust">
    </a>
</p>

# pilgrimage

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

### Functionality Implemented

- **Message Queue**: Efficient message queue implementation using `Mutex` and `VecDeque`.
- **Broker**: Core broker functionality including message handling, node management, and leader election.
- **Consumer Groups**: Support for consumer groups to allow multiple consumers to read from the same topic.
- **Leader Election**: Mechanism for electing a leader among brokers to manage partitions and replication.
- **Storage**: Persistent storage of messages using local files.
- **Replication**: Replication of messages across multiple brokers for fault tolerance.
- **Schema Registry**: Management of message schemas to ensure compatibility between producers and consumers.
- **Benchmarking**: Comprehensive benchmarking tests to measure performance of various components.

### Basic usage

```rust
use pilgrimage::broker::{Broker, Node};
use std::sync::{Arc, Mutex};

fn main() {
    // Broker Creation
    let broker = Broker::new("broker1", 3, 2, "storage_path");

    // Adding a node
    let node = Node {
        data: Arc::new(Mutex::new(Vec::new())),
    };
    broker.add_node("node1".to_string(), node);

    // Send a message
    broker.send_message("Hello, world!".to_string());

    // Message received
    if let Some(message) = broker.receive_message() {
        println!("Received: {}", message);
    }
}
```

### Multi-threaded message processing

```rust
use pilgrimage::broker::Broker;
use std::sync::Arc;
use std::thread;

fn main() {
    let broker = Arc::new(Broker::new("broker1", 3, 2, "storage_path"));

    let broker_sender = Arc::clone(&broker);
    let sender_handle = thread::spawn(move || {
        for i in 0..10 {
            let message = format!("Message {}", i);
            broker_sender.send_message(message);
        }
    });

    let broker_receiver = Arc::clone(&broker);
    let receiver_handle = thread::spawn(move || {
        for _ in 0..10 {
            if let Some(message) = broker_receiver.receive_message() {
                println!("Received: {:?}", message);
            }
        }
    });

    sender_handle.join().unwrap();
    receiver_handle.join().unwrap();
}
```

### Fault Detection and Automatic Recovery

The system includes mechanisms for fault detection and automatic recovery. Nodes are monitored using heartbeat signals, and if a fault is detected, the system will attempt to recover automatically.

```rust
use pilgrimage::broker::Broker;
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

### License

MIT

### Examples

To execute a basic example, use the following command:

```bash
cargo run --example simple-send-recv
cargo run --example mulch-send-recv
cargo run --example thread-send-recv
```

### Bench

`cargo bench`
