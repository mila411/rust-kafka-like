use crate::broker::scaling::AutoScaler;
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

pub struct MessageQueue {
    queue: Mutex<VecDeque<String>>,
    condvar: Condvar,
    auto_scaler: Arc<AutoScaler>,
}

impl MessageQueue {
    pub fn new(min_instances: usize, max_instances: usize, check_interval: Duration) -> Self {
        let auto_scaler = Arc::new(AutoScaler::new(min_instances, max_instances));
        auto_scaler.clone().monitor_and_scale(check_interval);

        MessageQueue {
            queue: Mutex::new(VecDeque::default()),
            condvar: Condvar::new(),
            auto_scaler,
        }
    }

    pub fn send(&self, message: String) {
        let mut queue = self.queue.lock().unwrap();
        queue.push_back(message);
        self.condvar.notify_one();

        // Scale up based on the length of the queue
        if queue.len() > 10 {
            let _ = self.auto_scaler.scale_up();
        }
    }

    pub fn receive(&self) -> String {
        let mut queue = self.queue.lock().unwrap();
        while queue.is_empty() {
            queue = self.condvar.wait(queue).unwrap();
        }
        let message = queue.pop_front().unwrap();

        // Scale down based on the length of the cue
        if queue.len() < 5 {
            let _ = self.auto_scaler.scale_down();
        }

        message
    }

    pub fn is_empty(&self) -> bool {
        let queue = self.queue.lock().unwrap();
        queue.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_auto_scaler_scale_up() {
        let auto_scaler = Arc::new(AutoScaler::new(1, 3));
        let auto_scaler_clone = Arc::clone(&auto_scaler);

        // Test scale-up
        auto_scaler_clone.scale_up().unwrap();
        auto_scaler_clone.scale_up().unwrap();

        let instances = auto_scaler_clone.current_instances.lock().unwrap();
        assert_eq!(*instances, 3);
    }

    #[test]
    fn test_auto_scaler_scale_down() {
        let auto_scaler = Arc::new(AutoScaler::new(1, 3));
        let auto_scaler_clone = Arc::clone(&auto_scaler);

        // Test scale down after scale up
        auto_scaler_clone.scale_up().unwrap();
        auto_scaler_clone.scale_up().unwrap();
        auto_scaler_clone.scale_down().unwrap();

        let instances = auto_scaler_clone.current_instances.lock().unwrap();
        assert_eq!(*instances, 2);
    }

    #[test]
    fn test_auto_scaler_monitor_and_scale() {
        let auto_scaler = Arc::new(AutoScaler::new(1, 3));
        let auto_scaler_clone = Arc::clone(&auto_scaler);

        // Monitor and test scale-up
        auto_scaler_clone
            .clone()
            .monitor_and_scale(Duration::from_secs(1));

        // Simulate scale-up
        thread::sleep(Duration::from_secs(2));
        let instances = auto_scaler_clone.current_instances.lock().unwrap();
        assert_eq!(*instances, 3);
    }
}
