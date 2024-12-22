use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum BrokerError {
    TopicError(String),
    PartitionError(String),
    AckError(String),
    IoError(std::io::Error),
}

impl From<std::io::Error> for BrokerError {
    fn from(error: std::io::Error) -> Self {
        BrokerError::IoError(error)
    }
}

impl fmt::Display for BrokerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BrokerError::TopicError(msg) => write!(f, "Topic error: {}", msg),
            BrokerError::PartitionError(msg) => write!(f, "Partition error: {}", msg),
            BrokerError::AckError(msg) => write!(f, "Acknowledgment error: {}", msg),
            BrokerError::IoError(err) => write!(f, "IO error: {}", err),
        }
    }
}

impl Error for BrokerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            BrokerError::IoError(err) => Some(err),
            _ => None,
        }
    }
}
