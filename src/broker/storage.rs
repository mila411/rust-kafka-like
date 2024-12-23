use std::fs::{File, OpenOptions};
use std::io::BufRead; // 追加
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

pub struct Storage {
    file: BufWriter<File>,
    path: String,
}

impl Storage {
    pub fn new(path: &str) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(path)?;
        Ok(Storage {
            file: BufWriter::new(file),
            path: path.to_string(),
        })
    }

    pub fn write_message(&mut self, message: &str) -> io::Result<()> {
        writeln!(self.file, "{}", message)?;
        self.file.flush()
    }

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

    pub fn cleanup_logs(&self) -> io::Result<()> {
        let old_path = format!("{}.old", self.path);
        if Path::new(&old_path).exists() {
            std::fs::remove_file(old_path)?;
        }
        Ok(())
    }
}
