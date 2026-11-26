//! Small validated types shared across the workspace.

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Identifier of a job, unique for the lifetime of a data directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobId(pub u64);

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A fencing token issued with every lease.
///
/// Tokens only increase, so a worker whose lease expired cannot acknowledge a
/// job that has since been handed to someone else.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LeaseToken(pub u64);

/// Milliseconds since the Unix epoch.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Timestamp(u64);

impl Timestamp {
    /// Builds a timestamp from epoch milliseconds.
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis)
    }

    /// Epoch milliseconds.
    pub const fn as_millis(self) -> u64 {
        self.0
    }

    /// `self + duration`, clamped at the maximum representable time.
    #[must_use]
    pub fn saturating_add(self, duration: Duration) -> Self {
        let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
        Self(self.0.saturating_add(millis))
    }

    /// Time from `earlier` to `self`, or zero if `earlier` is later.
    pub fn saturating_since(self, earlier: Self) -> Duration {
        Duration::from_millis(self.0.saturating_sub(earlier.0))
    }
}

/// Scheduling priority from 0 (lowest) to 9 (highest).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
pub struct Priority(u8);

/// A priority outside `0..=9`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("priority must be between 0 and 9, got {0}")]
pub struct InvalidPriority(pub u8);

impl Priority {
    /// The highest priority.
    pub const MAX: Self = Self(9);
}

impl Default for Priority {
    fn default() -> Self {
        Self(4)
    }
}

impl TryFrom<u8> for Priority {
    type Error = InvalidPriority;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        if value <= Self::MAX.0 {
            Ok(Self(value))
        } else {
            Err(InvalidPriority(value))
        }
    }
}

impl From<Priority> for u8 {
    fn from(priority: Priority) -> Self {
        priority.0
    }
}

/// A queue name: 1 to 64 ASCII letters, digits, `.`, `_` or `-`.
///
/// Cloning is cheap because the text is shared.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct QueueName(Arc<str>);

/// A string that is not a valid [`QueueName`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("queue names are 1-64 characters from A-Z, a-z, 0-9, '.', '_' and '-'")]
pub struct InvalidQueueName;

impl QueueName {
    /// Maximum length in bytes.
    pub const MAX_LEN: usize = 64;

    /// The name as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for QueueName {
    type Err = InvalidQueueName;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        let allowed = |b: u8| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-');
        // "." and ".." would be rewritten by URL path normalisation.
        if (1..=Self::MAX_LEN).contains(&name.len())
            && name.bytes().all(allowed)
            && !matches!(name, "." | "..")
        {
            Ok(Self(name.into()))
        } else {
            Err(InvalidQueueName)
        }
    }
}

impl TryFrom<String> for QueueName {
    type Error = InvalidQueueName;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        name.parse()
    }
}

impl From<QueueName> for String {
    fn from(name: QueueName) -> Self {
        name.as_str().to_owned()
    }
}

impl fmt::Display for QueueName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Everything a producer chooses about a job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobSpec {
    /// Destination queue.
    pub queue: QueueName,
    /// Opaque application data. The queue never interprets it.
    pub payload: String,
    /// Scheduling priority.
    pub priority: Priority,
    /// Number of leases granted before the job is dead-lettered. At least 1 is
    /// always granted.
    pub max_attempts: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_names_are_validated() {
        for good in ["a", "mail.high_priority-2", &"x".repeat(64)] {
            assert!(good.parse::<QueueName>().is_ok(), "{good}");
        }
        for bad in [
            "",
            ".",
            "..",
            "with space",
            "slash/name",
            "ünicode",
            &"x".repeat(65),
        ] {
            assert!(bad.parse::<QueueName>().is_err(), "{bad}");
        }
    }

    #[test]
    fn deserialisation_cannot_bypass_validation() {
        assert!(serde_json::from_str::<QueueName>("\"a/b\"").is_err());
        assert!(serde_json::from_str::<Priority>("10").is_err());
    }
}
