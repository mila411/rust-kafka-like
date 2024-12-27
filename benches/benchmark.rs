use criterion::{Criterion, criterion_group, criterion_main};
use pilgrimage::broker::{Broker, Node};
use std::sync::{Arc, Mutex};
use std::thread;

fn broker_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker_benchmarks");

    group.bench_function("start_election", |b| {
        b.iter(|| {
            let broker = Broker::new("broker1", 3, 2, "storage_path");
            let node1 = Node {
                data: Arc::new(Mutex::new(Vec::new())),
            };
            let node2 = Node {
                data: Arc::new(Mutex::new(Vec::new())),
            };
            broker.add_node("node1".to_string(), node1);
            broker.add_node("node2".to_string(), node2);
            broker.start_election();
        })
    });

    group.bench_function("send_message", |b| {
        b.iter(|| {
            let broker = Broker::new("broker1", 3, 2, "storage_path");
            broker.send_message("test message".to_string());
        })
    });

    group.bench_function("receive_message", |b| {
        b.iter(|| {
            let broker = Broker::new("broker1", 3, 2, "storage_path");
            broker.send_message("test message".to_string());
            broker.receive_message();
        })
    });

    group.bench_function("process_messages", |b| {
        b.iter(|| {
            let broker = Broker::new("broker1", 3, 2, "storage_path");
            for i in 0..100 {
                broker.send_message(format!("message {}", i));
            }
            for _ in 0..100 {
                broker.receive_message();
            }
        })
    });

    group.bench_function("send_message_multithreaded", |b| {
        b.iter(|| {
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
                    if let Some(_message) = broker_receiver.receive_message() {}
                }
            });

            sender_handle.join().unwrap();
            receiver_handle.join().unwrap();
        })
    });

    group.bench_function("process_messages_multithreaded", |b| {
        b.iter(|| {
            let broker = Arc::new(Broker::new("broker1", 3, 2, "storage_path"));

            let broker_sender = Arc::clone(&broker);
            let sender_handle = thread::spawn(move || {
                for i in 0..100 {
                    let message = format!("message {}", i);
                    broker_sender.send_message(message);
                }
            });

            let broker_receiver = Arc::clone(&broker);
            let receiver_handle = thread::spawn(move || {
                for _ in 0..100 {
                    if let Some(_message) = broker_receiver.receive_message() {}
                }
            });

            sender_handle.join().unwrap();
            receiver_handle.join().unwrap();
        })
    });

    group.finish();
}

criterion_group!(benches, broker_benchmark);
criterion_main!(benches);