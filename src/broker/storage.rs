use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::Path;

pub struct Storage {
    file: BufWriter<File>,
    path: String,
    pub available: bool,
}

impl Storage {
    /// Creates a new storage instance.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the storage file.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::storage::Storage;
    ///
    /// let storage = Storage::new("test_logs").unwrap();
    /// ```
    pub fn new(path: &str) -> io::Result<Self> {
        let file = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(path)?,
        );
        Ok(Storage {
            path: path.to_string(),
            file,
            available: true,
        })
    }

    /// Writes a message to the storage.
    ///
    /// # Arguments
    ///
    /// * `message` - The message to write.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::storage::Storage;
    ///
    /// let mut storage = Storage::new("test_logs").unwrap();
    /// storage.write_message("test_message").unwrap();
    /// ```
    pub fn write_message(&mut self, message: &str) -> io::Result<()> {
        self.file.write_all(message.as_bytes())?;
        self.file.write_all(b"\n")?;
        self.file.flush()?;
        Ok(())
    }

    /// Reads all messages from the storage.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::storage::Storage;
    ///
    /// let storage = Storage::new("test_logs").unwrap();
    /// let messages = storage.read_messages().unwrap();
    /// ```
    pub fn read_messages(&self) -> io::Result<Vec<String>> {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        let mut messages = Vec::new();
        for line in reader.lines() {
            messages.push(line?);
        }
        Ok(messages)
    }

    /// Rotates the current log file.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::storage::Storage;
    ///
    /// let mut storage = Storage::new("test_logs").unwrap();
    /// storage.rotate_logs().unwrap();
    /// ```
    pub fn rotate_logs(&mut self) -> io::Result<()> {
        let new_path = format!("{}.old", self.path);
        std::fs::rename(&self.path, &new_path)?;
        self.file = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&self.path)?,
        );
        Ok(())
    }

    /// Cleans up the old logs.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::broker::storage::Storage;
    ///
    /// let mut storage = Storage::new("test_logs").unwrap();
    /// storage.write_message("test_message").unwrap();
    /// storage.rotate_logs().unwrap();
    /// storage.cleanup_logs().unwrap();
    /// ```
    pub fn cleanup_logs(&self) -> io::Result<()> {
        let old_path = format!("{}.old", self.path);
        if Path::new(&old_path).exists() {
            std::fs::remove_file(&old_path)?;
        } else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Old log file does not exist",
            ));
        }
        Ok(())
    }

    pub fn is_available(&self) -> bool {
        self.available
    }

    pub fn reinitialize(&mut self) -> Result<(), String> {
        let file = File::create(&self.path).map_err(|e| e.to_string())?;
        self.file = BufWriter::new(file);
        writeln!(self.file, "Initialized").map_err(|e| e.to_string())?;
        self.available = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_write_message_to_invalid_path() {
        let invalid_path = "/invalid_path/test_logs";
        let result = Storage::new(invalid_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_message_from_nonexistent_file() {
        let test_log = format!("/tmp/test_log_{}", std::process::id());

        assert!(!Path::new(&test_log).exists());

        let storage = Storage::new(&test_log).unwrap();
        std::fs::remove_file(&test_log).unwrap_or(());

        let result = storage.read_messages();
        assert!(
            result.is_err(),
            "Loading a non-existent file should return an error"
        );
    }

    #[test]
    fn test_rotate_logs_with_invalid_path() {
        let invalid_path = "/invalid_path/test_logs";
        let mut storage = Storage::new(invalid_path).unwrap_or_else(|_| Storage {
            path: invalid_path.to_string(),
            file: BufWriter::new(File::create("/dev/null").unwrap()),
            available: false,
        });
        let result = storage.rotate_logs();
        assert!(result.is_err());
    }

    #[test]
    fn test_cleanup_logs_with_invalid_path() {
        let invalid_path = format!("/tmp/nonexistent_{}/test.log", std::process::id());
        assert!(!Path::new(&invalid_path).exists());

        let storage = Storage {
            path: invalid_path,
            file: BufWriter::new(File::create("/dev/null").unwrap()),
            available: false,
        };

        let result = storage.cleanup_logs();
        assert!(
            result.is_err(),
            "Deleting a file that does not exist should return an error"
        );
    }
}
