use rust_kafka_like::{Broker, Subscriber};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};

fn simple_rng(seed: u64) -> u64 {
    // Pseudo-random number generation using the linear congruential method
    const A: u64 = 6364136223846793005;
    const C: u64 = 1;
    const M: u64 = 1 << 31;
    (A.wrapping_mul(seed).wrapping_add(C)) % M
}

fn main() {
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
        .map(|i| {
            let broker_producer = Arc::clone(&broker_producer);
            thread::spawn(move || {
                let mut seed = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                for j in 0..10 {
                    let message = format!("Producer {}: message {}", i, j);
                    let ack = broker_producer
                        .lock()
                        .unwrap()
                        .publish_with_ack("test_topic", message, None)
                        .unwrap();
                    println!("Produced message with ack: {:?}", ack);
                    seed = simple_rng(seed);
                    thread::sleep(Duration::from_millis(50 + (seed % 100)));
                }
            })
        })
        .collect();

    // Create multiple consumers
    let broker_consumer: Arc<Mutex<Broker>> = Arc::clone(&broker);
    let consumer_handles: Vec<_> = (0..2)
        .map(|i| {
            let broker_consumer = Arc::clone(&broker_consumer);
            thread::spawn(move || {
                broker_consumer
                    .lock()
                    .unwrap()
                    .subscribe(
                        "test_topic",
                        Subscriber::new(
                            &format!("consumer_{}", i),
                            Box::new(move |msg: String| {
                                println!("Consumer {}: Consumed message: {}", i, msg);
                            }),
                        ),
                    )
                    .unwrap();

                // End the thread after the message has been consumed.
                thread::sleep(Duration::from_secs(5));
            })
        })
        .collect();

    // Waiting for the end of the producer thread.
    for handle in producer_handles {
        handle.join().unwrap();
    }

    // Waiting for the consumer thread to finish
    for handle in consumer_handles {
        handle.join().unwrap();
    }
}
