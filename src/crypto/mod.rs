use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit},
};
use rand::Rng;
use std::fmt;

#[derive(Debug)]
pub enum CryptoError {
    EncryptionError(String),
    DecryptionError(String),
    InvalidData(String),
}

impl fmt::Display for CryptoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CryptoError::EncryptionError(msg) => write!(f, "Encryption error: {}", msg),
            CryptoError::DecryptionError(msg) => write!(f, "Decryption error: {}", msg),
            CryptoError::InvalidData(msg) => write!(f, "Invalid data: {}", msg),
        }
    }
}

impl std::error::Error for CryptoError {}

pub struct Encryptor {
    cipher: Aes256Gcm,
}

impl Encryptor {
    pub fn new(key: &[u8; 32]) -> Self {
        Self {
            cipher: Aes256Gcm::new(key.into()),
        }
    }

    pub fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>, CryptoError> {
        let mut rng = rand::thread_rng();
        let mut nonce_bytes = [0u8; 12];
        rng.fill(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = self
            .cipher
            .encrypt(nonce, data)
            .map_err(|e| CryptoError::EncryptionError(e.to_string()))?;

        let mut result = nonce_bytes.to_vec();
        result.extend(ciphertext);
        Ok(result)
    }

    pub fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>, CryptoError> {
        if data.len() < 12 {
            return Err(CryptoError::InvalidData("Data too short".to_string()));
        }
        let (nonce_bytes, ciphertext) = data.split_at(12);
        let nonce = Nonce::from_slice(nonce_bytes);

        self.cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| CryptoError::DecryptionError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_encryption_decryption() {
        let key = [0u8; 32];
        let encryptor = Encryptor::new(&key);
        let data = b"Hello, world!";

        let encrypted = encryptor.encrypt(data).unwrap();
        let decrypted = encryptor.decrypt(&encrypted).unwrap();

        assert_eq!(data, decrypted.as_slice());
    }

    #[test]
    fn test_invalid_data_decrypt() {
        let key = [0u8; 32];
        let encryptor = Encryptor::new(&key);
        let invalid_data = vec![0u8; 8];

        match encryptor.decrypt(&invalid_data) {
            Err(CryptoError::InvalidData(_)) => (),
            _ => panic!("Expected InvalidData error"),
        }
    }

    #[test]
    fn test_different_keys() {
        let key1 = [0u8; 32];
        let key2 = [1u8; 32];
        let encryptor1 = Encryptor::new(&key1);
        let encryptor2 = Encryptor::new(&key2);

        let data = b"Secret message";
        let encrypted = encryptor1.encrypt(data).unwrap();

        match encryptor2.decrypt(&encrypted) {
            Err(CryptoError::DecryptionError(_)) => (),
            _ => panic!("Expected DecryptionError"),
        }
    }

    #[test]
    fn test_random_data() {
        let key = [0u8; 32];
        let encryptor = Encryptor::new(&key);
        let mut rng = rand::thread_rng();
        let mut data = vec![0u8; 100];
        rng.fill(&mut data[..]);

        let encrypted = encryptor.encrypt(&data).unwrap();
        let decrypted = encryptor.decrypt(&encrypted).unwrap();

        assert_eq!(data, decrypted);
    }
}
