use crate::pubsub::message::Message;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct Broker {
    subscribers: Arc<Mutex<HashMap<String, Vec<Message>>>>,
}

impl Broker {
    pub fn new() -> Self {
        Self {
            subscribers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn subscribe(&self, topic: &str) {
        let mut subscribers = self.subscribers.lock().unwrap();
        subscribers
            .entry(topic.to_string())
            .or_insert_with(Vec::new);
    }

    pub fn publish(&self, topic: &str, message: Message) {
        let mut subscribers = self.subscribers.lock().unwrap();
        if let Some(messages) = subscribers.get_mut(topic) {
            messages.push(message);
        }
    }

    pub fn get_messages(&self, topic: &str) -> Vec<Message> {
        let subscribers = self.subscribers.lock().unwrap();
        if let Some(messages) = subscribers.get(topic) {
            return messages.clone();
        }
        Vec::new()
    }
}
