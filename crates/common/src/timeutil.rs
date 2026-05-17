use std::{
    sync::OnceLock,
    time::{Instant, SystemTime},
};

pub fn monotonic_now_ns() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

pub fn unix_now() -> time::OffsetDateTime {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => {
            time::OffsetDateTime::from_unix_timestamp_nanos(duration.as_nanos() as i128)
                .unwrap_or(time::OffsetDateTime::UNIX_EPOCH)
        }
        Err(_) => time::OffsetDateTime::UNIX_EPOCH,
    }
}
