extern crate rust_kafka_like as rufka;   
use std::error::Error;
use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
    mpsc::{Receiver, Sender, channel},
};
use std::thread;
use std::time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
    let mut peers = HashSet::new();
    peers.insert("broker2".to_string());
    peers.insert("broker3".to_string());
    
    println!("Initializing broker...");
    let broker = Arc::new(Mutex::new(rufka::Broker::new(
        "broker1".to_string(),
        "logs",
        3,
        peers,
        2,
    ).expect("Failed to create Broker"),
));

    if let Ok(mut broker_ref) = broker.lock() {
        println!("Creating a topic...");
        broker_ref.create_topic("test_topic", Some(3))?;

        println!("Registering a subscriber...");
        broker_ref.subscribe(
            "test_topic",
            Box::new(|message, ack_sender| {
                println!("Received message: {}", message);
                ack_sender
                    .send(rufka::MessageAck {
                        message_id: 0, // TODO: Set an appropriate ID
                        topic: "test_topic".to_string(),
                        status: rufka::AckStatus::Success,
                    })
                    .unwrap();
            }),
        )?;

        broker_ref.start_health_check();
    }

    let broker_producer = Arc::clone(&broker);
    let producer_handle = thread::spawn(move || {
        println!("I started as a producer....");
        thread::sleep(Duration::from_secs(2));

        if let Ok(broker) = broker_producer.lock() {
            println!("Sending message...");
            match broker.publish_with_ack("test_topic", "Test message".to_string(), None) {
                Ok(ack) => println!("Received message confirmation response: {:?}", ack),
                Err(e) => eprintln!("Message publication error: {}", e),
            }
        }
    });

    thread::sleep(Duration::from_secs(3));
    producer_handle.join().unwrap();

    println!("Quit the program");
    Ok(())
}