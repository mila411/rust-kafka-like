use std::collections::HashMap;
use std::error::Error;

pub trait Authenticator {
    fn authenticate(&self, username: &str, password: &str) -> Result<bool, Box<dyn Error>>;
}

pub struct BasicAuthenticator {
    credentials: std::collections::HashMap<String, String>,
}

impl BasicAuthenticator {
    pub fn new() -> Self {
        Self {
            credentials: HashMap::new(),
        }
    }

    pub fn add_user(&mut self, username: &str, password: &str) {
        self.credentials
            .insert(username.to_string(), password.to_string());
    }
}

impl Default for BasicAuthenticator {
    fn default() -> Self {
        Self::new()
    }
}

impl Authenticator for BasicAuthenticator {
    fn authenticate(&self, username: &str, password: &str) -> Result<bool, Box<dyn Error>> {
        Ok(self
            .credentials
            .get(username)
            .is_some_and(|stored_password| stored_password == password))
    }
}
