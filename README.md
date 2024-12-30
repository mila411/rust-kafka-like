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
    <a href="https://codecov.io/gh/mila411/pilgrimage">
      <img src="https://codecov.io/gh/mila411/pilgrimage/graph/badge.svg?token=HVMZX0580X"/>
    </a>
    <a href="https://app.deepsource.com/gh/mila411/pilgrimage/" target="_blank">
      <img alt="DeepSource" title="DeepSource" src="https://app.deepsource.com/gh/mila411/pilgrimage.svg/?label=active+issues&show_trend=true&token=tsauTwVl8Nd7UH7xuQCtLR9H"/>
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
- Automatic Scaling
- Broker Clustering
- Message processing in parallel
- Authentication and Authorization Mechanisms
- Data Encryption

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
- **Automatic Scaling:** Automatically scale the number of instances based on load.
- **Log Compressions:** Compress and optimize logs.

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
use pilgrimage::Broker;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

fn main() {
    let storage = Arc::new(Mutex::new(Storage::new("test_db_path").unwrap()));
    let mut broker = Broker::new("broker_id", 1, 1, "test_db_path");
    broker.storage = storage.clone();

    // Simulating a disability
    {
        let mut storage_guard = storage.lock().unwrap();
        storage_guard.available = false;
    }

    // Simulating a disability
    broker.monitor_nodes();

    // Simulating a disability
    thread::sleep(Duration::from_millis(100));
    let storage_guard = storage.lock().unwrap();
    assert!(storage_guard.is_available());
}
```

### Examples

- Simple message sending and receiving
- Sending and receiving multiple messages
- Sending and receiving messages in multiple threads
- Authentication processing example
- Sending and receiving messages as an authenticated user

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

## Version increment on release

- The commit message is parsed and the version of either major, minor or patch is incremented.
- The version of Cargo.toml is updated.
- The updated Cargo.toml is committed and a new tag is created.
- The changes and tag are pushed to the remote repository.

The version is automatically incremented based on the commit message. Here, we treat `feat` as minor, `fix` as patch, and `BREAKING CHANGE` as major.
