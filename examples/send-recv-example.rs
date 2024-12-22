use rust_kafka_like::{Broker, Subscriber};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

fn main() {
    let _peers: HashSet<String> = ["peer1".to_string(), "peer2".to_string()]
        .iter()
        .cloned()
        .collect();
    let broker = Arc::new(Mutex::new(Broker::new("broker1", 3, 2)));

    // Create a topic
    {
        let mut broker = broker.lock().unwrap();
        broker.create_topic("test_topic", None).unwrap();
    }

    let broker_producer: Arc<Mutex<Broker>> = Arc::clone(&broker);
    let producer_handle = thread::spawn(move || {
        for i in 0..10 {
            let message = format!("message {}", i);
            let ack = broker_producer
                .lock()
                .unwrap()
                .publish_with_ack("test_topic", message, None)
                .unwrap();
            println!("Produced message with ack: {:?}", ack);
            thread::sleep(Duration::from_millis(100));
        }
    });

    let broker_consumer: Arc<Mutex<Broker>> = Arc::clone(&broker);
    let consumer_handle = thread::spawn(move || {
        broker_consumer
            .lock()
            .unwrap()
            .subscribe(
                "test_topic",
                Box::new(|msg: String| {
                    println!("Consumed message: {}", msg);
                }) as Subscriber,
            )
            .unwrap();

        // End the thread after the message has been consumed.
        thread::sleep(Duration::from_secs(2));
    });

    producer_handle.join().unwrap();
    consumer_handle.join().unwrap();
}
