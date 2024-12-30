use flate2::Compression;
use flate2::write::{GzDecoder, GzEncoder};
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;

pub struct LogCompressor;

impl LogCompressor {
    pub fn compress_file<P: AsRef<Path>>(input_path: P, output_path: P) -> io::Result<()> {
        let input_file = File::open(input_path)?;
        let output_file = File::create(output_path)?;
        let mut encoder = GzEncoder::new(output_file, Compression::default());
        let mut buffer = Vec::new();
        input_file.take(1024 * 1024).read_to_end(&mut buffer)?;
        encoder.write_all(&buffer)?;
        encoder.finish()?;
        Ok(())
    }

    pub fn decompress_file<P: AsRef<Path>>(input_path: P, output_path: P) -> io::Result<()> {
        let input_file = File::open(input_path)?;
        let output_file = File::create(output_path)?;
        let mut decoder = GzDecoder::new(output_file);
        let mut buffer = Vec::new();
        input_file.take(1024 * 1024).read_to_end(&mut buffer)?;
        decoder.write_all(&buffer)?;
        decoder.finish()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{File, remove_file};
    use std::io::{Read, Write};
    use tempfile::tempdir;

    #[test]
    fn test_compress_and_decompress() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("input.txt");
        let compressed_path = dir.path().join("output.gz");
        let decompressed_path = dir.path().join("decompressed.txt");

        {
            let mut file = File::create(&input_path).unwrap();
            writeln!(file, "Hello, compression!").unwrap();
        }

        // Normal Flow: compression
        LogCompressor::compress_file(&input_path, &compressed_path).unwrap();
        assert!(compressed_path.exists());

        // Normal Flow: Defrosting
        LogCompressor::decompress_file(&compressed_path, &decompressed_path).unwrap();
        assert!(decompressed_path.exists());

        // Checking the contents
        let mut content = String::default();
        let mut file = File::open(&decompressed_path).unwrap();
        file.read_to_string(&mut content).unwrap();
        assert_eq!(content.trim(), "Hello, compression!");

        let _ = remove_file(&input_path);
        let _ = remove_file(&compressed_path);
        let _ = remove_file(&decompressed_path);
    }

    #[test]
    fn test_compress_file_not_found() {
        let dir = tempdir().unwrap();
        let missing_input = dir.path().join("no_such_file.txt");
        let compressed = dir.path().join("compressed.gz");

        // Exceptional Flow: The input file does not exist.
        let result = LogCompressor::compress_file(&missing_input, &compressed);
        assert!(result.is_err());
    }

    #[test]
    fn test_decompress_file_not_found() {
        let dir = tempdir().unwrap();
        let missing_input = dir.path().join("no_such_file.gz");
        let output = dir.path().join("output.txt");

        // Exceptional Flow: The input file does not exist.
        let result = LogCompressor::decompress_file(&missing_input, &output);
        assert!(result.is_err());
    }

    #[test]
    fn test_compress_and_decompress_empty_file() {
        let dir = tempdir().unwrap();
        let input_path = dir.path().join("empty.txt");
        let compressed = dir.path().join("empty.gz");
        let decompressed = dir.path().join("decompressed_empty.txt");

        // Edge case: Create a blank file
        File::create(&input_path).unwrap();

        LogCompressor::compress_file(&input_path, &compressed).unwrap();
        assert!(compressed.exists());

        LogCompressor::decompress_file(&compressed, &decompressed).unwrap();
        assert!(decompressed.exists());

        // Check the contents
        let mut buf = String::default();
        let mut file = File::open(&decompressed).unwrap();
        file.read_to_string(&mut buf).unwrap();
        assert_eq!(buf, "");

        let _ = remove_file(&input_path);
        let _ = remove_file(&compressed);
        let _ = remove_file(&decompressed);
    }
}
