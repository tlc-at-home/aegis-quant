//! Thread-local lock-discipline guard around the global execution lock (`R14-H2`).
//!
//! Ensures zero blocking HTTP IO or sleeps execute while `execution_lock()` is held on
//! the current thread, and catches any accidental re-entrant acquisition in debug builds.

use parking_lot::ReentrantMutexGuard;
use std::cell::Cell;

thread_local! {
    static LOCK_DEPTH: Cell<usize> = const { Cell::new(0) };
}

/// Returns `true` when the current thread holds at least one [`ExecutionLockGuard`].
#[inline]
pub(crate) fn is_execution_lock_held_on_current_thread() -> bool {
    LOCK_DEPTH.with(|d| d.get() > 0)
}

/// RAII wrapper around `ReentrantMutexGuard<'a, ()>` that tracks thread-local acquisition depth.
#[must_use = "ExecutionLockGuard releases the execution lock and depth counter when dropped"]
pub(crate) struct ExecutionLockGuard<'a> {
    _inner: ReentrantMutexGuard<'a, ()>,
}

impl Drop for ExecutionLockGuard<'_> {
    fn drop(&mut self) {
        LOCK_DEPTH.with(|d| {
            let cur = d.get();
            debug_assert!(cur > 0, "ExecutionLockGuard dropped with zero LOCK_DEPTH");
            d.set(cur.saturating_sub(1));
        });
    }
}

/// Acquires `crate::execution_lock()` while asserting zero re-entrant acquisition in debug builds.
pub(crate) fn acquire_execution_lock<'a>() -> ExecutionLockGuard<'a> {
    let inner = crate::execution_lock().lock();
    LOCK_DEPTH.with(|d| {
        let prev = d.get();
        debug_assert_eq!(
            prev, 0,
            "Reentrant execution_lock acquisition detected on current thread (prev_depth={prev})"
        );
        d.set(prev.saturating_add(1));
    });
    ExecutionLockGuard { _inner: inner }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_lock_guard_tracks_thread_local_depth() {
        assert!(!is_execution_lock_held_on_current_thread());
        {
            let _guard = acquire_execution_lock();
            assert!(is_execution_lock_held_on_current_thread());

            // Other threads must not observe this thread's lock depth.
            let other = std::thread::spawn(is_execution_lock_held_on_current_thread)
                .join()
                .expect("join lock depth probe thread");
            assert!(!other);
        }
        assert!(!is_execution_lock_held_on_current_thread());
    }
}
