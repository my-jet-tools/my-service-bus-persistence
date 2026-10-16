use std::{
    sync::atomic::{AtomicI64, AtomicU64},
    time::Duration,
};

use rust_extensions::date_time::DateTimeAsMicroseconds;

#[derive(Debug)]
pub struct TopicDataMetrics {
    last_saved_duration: AtomicU64,
    last_saved_moment: AtomicI64,
}

impl TopicDataMetrics {
    pub fn new() -> Self {
        Self {
            last_saved_duration: AtomicU64::new(0),
            last_saved_moment: AtomicI64::new(DateTimeAsMicroseconds::now().unix_microseconds),
        }
    }

    pub fn update_last_saved_duration(&self, duration: Duration) {
        self.last_saved_duration.store(
            duration.as_millis() as u64,
            std::sync::atomic::Ordering::SeqCst,
        );
    }

    pub fn get_last_saved_duration(&self) -> Duration {
        let result = self
            .last_saved_duration
            .load(std::sync::atomic::Ordering::SeqCst);

        return Duration::from_millis(result);
    }

    pub fn update_last_saved_moment(&self, moment: DateTimeAsMicroseconds) {
        self.last_saved_moment.store(
            moment.unix_microseconds,
            std::sync::atomic::Ordering::SeqCst,
        );
    }

    pub fn get_last_saved_moment(&self) -> DateTimeAsMicroseconds {
        let unix_microseconds = self
            .last_saved_moment
            .load(std::sync::atomic::Ordering::SeqCst);
        return DateTimeAsMicroseconds::new(unix_microseconds);
    }
}
