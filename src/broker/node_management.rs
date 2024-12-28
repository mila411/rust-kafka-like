use crate::broker::consumer::group::ConsumerGroup;
use crate::broker::storage::Storage;
use std::collections::HashMap;
use std::sync::Mutex;

pub type ConsumerGroups = HashMap<String, ConsumerGroup>;

pub fn check_node_health(storage: &Mutex<Storage>) -> bool {
    let storage_guard = storage.lock().unwrap();
    storage_guard.is_available()
}

pub fn recover_node(storage: &Mutex<Storage>, consumer_groups: &Mutex<ConsumerGroups>) {
    let mut storage_guard = storage.lock().unwrap();
    if let Err(e) = storage_guard.reinitialize() {
        eprintln!("Storage initialization failed.: {}", e);
    }

    let mut groups_guard = consumer_groups.lock().unwrap();
    for group in groups_guard.values_mut() {
        group.reset_assignments();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Broker;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_auto_recovery() {
        let storage = Arc::new(Mutex::new(Storage::new("test_db_path").unwrap()));
        let mut broker = Broker::new("broker_id", 1, 1, "test_db_path");
        broker.storage = storage.clone();

        // Simulate a disability
        {
            let mut storage_guard = storage.lock().unwrap();
            storage_guard.available = false;
        }

        // Perform automatic recovery
        broker.monitor_nodes();

        // Check recovery
        thread::sleep(Duration::from_millis(100));
        let storage_guard = storage.lock().unwrap();
        assert!(storage_guard.is_available());
    }
}
