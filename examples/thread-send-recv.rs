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
