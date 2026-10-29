use std::sync::{Arc, OnceLock};

use url::Url;

use super::CommitConflictError;

/// What the commit loop did after another writer committed first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitConflictOutcome {
    /// The target log version already existed. The loop tries the next version.
    Retry,
    /// A newer commit passed the conflict check. The loop moves its snapshot forward.
    Rebase,
    /// A newer commit failed the conflict check. The loop returns the error.
    Abort,
    /// The loop used every attempt, or retries were disabled.
    Exhausted,
}

/// One commit-conflict event from the commit retry loop.
#[derive(Debug)]
pub struct CommitConflictEvent<'a> {
    /// Root URL of the Delta table, including the table name as the last path segment.
    pub table_url: &'a Url,
    /// [`crate::protocol::DeltaOperation::name`] of the commit that lost the race.
    pub operation: &'a str,
    /// `commitInfo.operation` of the commit that won. Absent for a lost put, because
    /// that attempt has not read the winning commit yet.
    pub winner_operation: Option<&'a str>,
    pub outcome: CommitConflictOutcome,
    /// Set only when `outcome` is [`CommitConflictOutcome::Abort`].
    pub error: Option<&'a CommitConflictError>,
}

/// Receives commit-conflict events. The Delta library does not record metrics itself.
pub trait CommitConflictObserver: Send + Sync {
    fn on_commit_conflict(&self, event: &CommitConflictEvent<'_>);
}

static OBSERVER: OnceLock<Arc<dyn CommitConflictObserver>> = OnceLock::new();

/// Installs the process-wide observer. Returns `false` when one is already installed.
pub fn set_commit_conflict_observer(observer: Arc<dyn CommitConflictObserver>) -> bool {
    OBSERVER.set(observer).is_ok()
}

pub(crate) fn notify(event: &CommitConflictEvent<'_>) {
    if let Some(observer) = OBSERVER.get() {
        observer.on_commit_conflict(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct Counter(Mutex<u32>);

    impl CommitConflictObserver for Counter {
        fn on_commit_conflict(&self, event: &CommitConflictEvent<'_>) {
            assert_eq!(event.operation, "WRITE");
            assert_eq!(event.outcome, CommitConflictOutcome::Retry);
            *self.0.lock().unwrap() += 1;
        }
    }

    #[test]
    fn installed_observer_counts_one_notification() {
        let counter = Arc::new(Counter(Mutex::new(0)));
        assert!(set_commit_conflict_observer(counter.clone()));
        let table_url = Url::parse("s3://bucket/acme/traces_v2").unwrap();
        notify(&CommitConflictEvent {
            table_url: &table_url,
            operation: "WRITE",
            winner_operation: None,
            outcome: CommitConflictOutcome::Retry,
            error: None,
        });
        assert_eq!(*counter.0.lock().unwrap(), 1);
        assert!(!set_commit_conflict_observer(counter));
    }
}
