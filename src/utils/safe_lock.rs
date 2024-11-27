use std::sync::{Mutex, MutexGuard};

/// Implements lock safely by recovering the value from poison
pub trait SafeLock<T> {
    fn safe_lock(&self) -> MutexGuard<'_, T>;
}

impl<T> SafeLock<T> for Mutex<T> {
    fn safe_lock(&self) -> MutexGuard<'_, T> {
        match self.lock() {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Mutex lock poisoned recovering ..");

                let val = e.into_inner();
                self.clear_poison();
                val
            }
        }
    }
}
