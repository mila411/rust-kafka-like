use core::fmt;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct SchemaVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl SchemaVersion {
    /// Creates a new `SchemaVersion` with the specified major version.
    ///
    /// # Arguments
    ///
    /// * `version` - The major version number.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::schema::version::SchemaVersion;
    ///
    /// let version = SchemaVersion::new(1);
    /// assert_eq!(version.major, 1);
    /// assert_eq!(version.minor, 0);
    /// assert_eq!(version.patch, 0);
    /// ```
    pub fn new(version: u32) -> Self {
        SchemaVersion {
            major: version,
            minor: 0,
            patch: 0,
        }
    }

    /// Creates a new `SchemaVersion` with the specified major, minor, and patch versions.
    ///
    /// # Arguments
    ///
    /// * `major` - The major version number.
    /// * `minor` - The minor version number.
    /// * `patch` - The patch version number.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::schema::version::SchemaVersion;
    ///
    /// let version = SchemaVersion::new_with_version(1, 2, 3);
    /// assert_eq!(version.major, 1);
    /// assert_eq!(version.minor, 2);
    /// assert_eq!(version.patch, 3);
    /// ```
    pub fn new_with_version(major: u32, minor: u32, patch: u32) -> Self {
        SchemaVersion {
            major,
            minor,
            patch,
        }
    }

    /// Increments the major version, resetting minor and patch versions to 0.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::schema::version::SchemaVersion;
    ///
    /// let mut version = SchemaVersion::new(1);
    /// version.increment_major();
    /// assert_eq!(version.major, 2);
    /// assert_eq!(version.minor, 0);
    /// assert_eq!(version.patch, 0);
    /// ```
    pub fn increment_major(&mut self) {
        self.major += 1;
        self.minor = 0;
        self.patch = 0;
    }

    /// Increments the minor version, resetting the patch version to 0.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::schema::version::SchemaVersion;
    ///
    /// let mut version = SchemaVersion::new(1);
    /// version.increment_minor();
    /// assert_eq!(version.minor, 1);
    /// assert_eq!(version.patch, 0);
    /// ```
    pub fn increment_minor(&mut self) {
        self.minor += 1;
        self.patch = 0;
    }

    /// Increments the patch version.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::schema::version::SchemaVersion;
    ///
    /// let mut version = SchemaVersion::new(1);
    /// version.increment_patch();
    /// assert_eq!(version.patch, 1);
    /// ```
    pub fn increment_patch(&mut self) {
        self.patch += 1;
    }
}

impl fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl PartialOrd for SchemaVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SchemaVersion {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.major.cmp(&other.major) {
            Ordering::Equal => match self.minor.cmp(&other.minor) {
                Ordering::Equal => self.patch.cmp(&other.patch),
                ord => ord,
            },
            ord => ord,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_creation() {
        let version = SchemaVersion::new(1);
        assert_eq!(version.major, 1);
        assert_eq!(version.minor, 0);
        assert_eq!(version.patch, 0);
    }

    #[test]
    fn test_version_increment() {
        let mut version = SchemaVersion::new(1);
        version.increment_minor();
        assert_eq!(version.to_string(), "1.1.0");
        version.increment_patch();
        assert_eq!(version.to_string(), "1.1.1");
        version.increment_major();
        assert_eq!(version.to_string(), "2.0.0");
    }

    #[test]
    fn test_version_comparison() {
        let v1 = SchemaVersion::new_with_version(1, 0, 0);
        let v2 = SchemaVersion::new_with_version(1, 1, 0);
        let v3 = SchemaVersion::new_with_version(2, 0, 0);

        assert!(v1 < v2);
        assert!(v2 < v3);
        assert!(v1 < v3);
    }
}
