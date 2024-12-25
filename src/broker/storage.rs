use std::fs::{File, OpenOptions};
use std::io::BufRead;
use std::io::{self, BufReader, BufWriter, Write};

pub struct Storage {
    file: BufWriter<File>,
    path: String,
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
    /// use rust_kafka_like::broker::storage::Storage;
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
    /// use rust_kafka_like::broker::storage::Storage;
    ///
    /// let mut storage = Storage::new("test_logs").unwrap();
    /// storage.write_message("test_message").unwrap();
    /// ```
    pub fn write_message(&mut self, message: &str) -> io::Result<()> {
        writeln!(self.file, "{}", message)?;
        self.file.flush()?;
        Ok(())
    }

    /// Reads messages from the storage.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the storage file.
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_kafka_like::broker::storage::Storage;
    ///
    /// let messages = Storage::read_messages("test_logs").unwrap();
    /// ```
    pub fn read_messages(path: &str) -> io::Result<Vec<String>> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut messages = Vec::new();
        for line in reader.lines() {
            messages.push(line?);
        }
        Ok(messages)
    }

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
    /// use rust_kafka_like::broker::storage::Storage;
    ///
    /// let mut storage = Storage::new("test_logs").unwrap();
    /// storage.write_message("test_message").unwrap();
    /// storage.rotate_logs().unwrap();
    /// storage.cleanup_logs().unwrap();
    /// ```
    pub fn cleanup_logs(&self) -> io::Result<()> {
        let old_path = format!("{}.old", self.path);
        if std::fs::metadata(&old_path).is_ok() {
            std::fs::remove_file(&old_path)?;
        }
        Ok(())
    }
}
