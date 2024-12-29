#[derive(Debug, Clone)]
pub struct Message {
    pub content: String,
}

impl Message {
    pub fn new(content: &str) -> Self {
        Self {
            content: content.to_string(),
        }
    }
}
