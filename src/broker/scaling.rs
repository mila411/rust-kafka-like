use crate::broker::BrokerError;
use log::info;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub struct AutoScaler {
    min_instances: usize,
    max_instances: usize,
    pub current_instances: Arc<Mutex<usize>>,
}

impl AutoScaler {
    pub fn new(min_instances: usize, max_instances: usize) -> Self {
        AutoScaler {
            min_instances,
            max_instances,
            current_instances: Arc::new(Mutex::new(min_instances)),
        }
    }

    pub fn scale_up(&self) -> Result<(), BrokerError> {
        let mut instances = self.current_instances.lock().unwrap();
        if *instances < self.max_instances {
            *instances += 1;
            info!("Scaled up to {} instances", *instances);
            Ok(())
        } else {
            Err(BrokerError::ScalingError(
                "Max instances reached".to_string(),
            ))
        }
    }

    pub fn scale_down(&self) -> Result<(), BrokerError> {
        let mut instances = self.current_instances.lock().unwrap();
        if *instances > self.min_instances {
            *instances -= 1;
            info!("Scaled down to {} instances", *instances);
            Ok(())
        } else {
            Err(BrokerError::ScalingError(
                "Min instances reached".to_string(),
            ))
        }
    }

    pub fn monitor_and_scale(self: Arc<Self>, check_interval: Duration) {
        std::thread::spawn(move || {
            loop {
                // Scale up/down according to the load ratio
                let load = 0.75;
                if load > 0.7 {
                    let _ = self.scale_up();
                } else if load < 0.3 {
                    let _ = self.scale_down();
                }

                std::thread::sleep(check_interval);
            }
        });
    }
}
