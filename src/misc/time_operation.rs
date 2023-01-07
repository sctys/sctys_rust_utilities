use std::{thread, time};
use rand::{thread_rng, Rng};
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime, NaiveDateTime, Utc, TimeZone};
use chrono_tz::Tz;

const SEC_TO_HOUR: i32 = 3600;
const ONE_E3: i64 = 1_000;
const ONE_E6: i64 = 1_000_000;
const ONE_E9: i64 = 1_000_000_000;

pub fn sleep(time_sec: u64) {
    thread::sleep(time::Duration::from_secs(time_sec));
}

pub fn random_sleep(lower_time_sec: f64, upper_time_sec: f64) {
    let mut rng = thread_rng();
    let time_sec = rng.gen_range(lower_time_sec..upper_time_sec);
    thread::sleep(time::Duration::from_secs_f64(time_sec));
}

pub enum SecPrecision {
    Sec,
    MilliSec,
    MicroSec,
    NanoSec,
}

pub fn utc_now() -> DateTime<Utc> {
    Utc::now()
}

pub fn timestamp_now(precision: SecPrecision) -> i64 {
    match precision {
        SecPrecision::Sec => Utc::now().timestamp(),
        SecPrecision::MilliSec => Utc::now().timestamp_millis(),
        SecPrecision::MicroSec => Utc::now().timestamp_micros(),
        SecPrecision::NanoSec => Utc::now().timestamp_nanos(),
    }
}

pub fn naive_date(year: i32, month: u32, day: u32) -> NaiveDate {
    match NaiveDate::from_ymd_opt(year, month, day) {
        Some(d) => d,
        None => panic!("Invalid date {year}, {month}, {day}")
    }
}

pub fn naive_date_time(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> NaiveDateTime {
    let date = naive_date(year, month, day);
    let time = match NaiveTime::from_hms_opt(hour, min, sec) {
        Some(t) => t,
        None => panic!("Invalid time {hour}, {min}, {sec}")
    };
    NaiveDateTime::new(date, time)
}

pub fn utc_date_time(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> DateTime<Utc> {
    match Utc.with_ymd_and_hms(year, month, day, hour, min, sec).single() {
        Some(dt) => dt,
        None => panic!("Unable to construct the date time {year}, {month}, {day}, {hour}, {min}, {sec}")
    }
}

pub fn naive_date_time_to_utc(naive_date_time: NaiveDateTime) -> DateTime<Utc> {
    DateTime::<Utc>::from_utc(naive_date_time, Utc)
}

pub fn date_time_to_timestamp<T: TimeZone>(date_time: DateTime<T>, precision: SecPrecision) -> i64 {
    match precision {
        SecPrecision::Sec => date_time.timestamp(),
        SecPrecision::MilliSec => date_time.timestamp_millis(),
        SecPrecision::MicroSec => date_time.timestamp_micros(),
        SecPrecision::NanoSec => date_time.timestamp_nanos(),
    }
}

pub fn utc_date_time_from_timestamp(timestamp: i64, precision: SecPrecision) -> DateTime<Utc> {
    let (secs, nsecs) = match precision {
        SecPrecision::Sec => (timestamp, 0),
        SecPrecision::MilliSec => (timestamp / ONE_E3, (timestamp % ONE_E3 * ONE_E6) as u32),
        SecPrecision::MicroSec => (timestamp / ONE_E6, (timestamp % ONE_E6 * ONE_E3) as u32),
        SecPrecision::NanoSec => (timestamp / ONE_E9, (timestamp % ONE_E9) as u32)
    };
    match NaiveDateTime::from_timestamp_opt(secs, nsecs) {
        Some(dt) => naive_date_time_to_utc(dt),
        None => panic!("Invalid timestamp {timestamp}")
    }
}

fn fixed_offset_from_hour(hour: i32) -> FixedOffset {
    match FixedOffset::east_opt(hour * SEC_TO_HOUR) {
        Some(o) => o,
        None => panic!("Invalid time offset {hour}")
    }
}

pub fn naive_date_time_to_fixed_offset(naive_date_time: NaiveDateTime, hour: i32) -> DateTime<FixedOffset> {
    let offset = fixed_offset_from_hour(hour);
    DateTime::<FixedOffset>::from_local(naive_date_time, offset)
}

pub fn utc_date_time_to_fixed_offset(date_time: DateTime<Utc>, hour: i32) -> DateTime<FixedOffset> {
    let offset = fixed_offset_from_hour(hour);
    date_time.with_timezone(&offset)
}

pub fn timezone_to_utc_date_time<T: TimeZone>(date_time: DateTime<T>) -> DateTime<Utc> {
    date_time.with_timezone(&Utc)
}

pub fn naive_date_time_to_timezone(naive_date_time: NaiveDateTime, timezone: Tz) -> DateTime<Tz> {
    match timezone.from_local_datetime(&naive_date_time).single() {
        Some(dt) => dt,
        None => panic! ("Unable to convert naive date time {naive_date_time} into timezone {timezone}")
    }
}

pub fn utc_date_time_to_timezone(date_time: DateTime<Utc>, timezone: Tz) -> DateTime<Tz> {
    date_time.with_timezone(&timezone)
}

pub fn naive_date_from_string(date_str: &str, fmt: &str) -> NaiveDate {
    match NaiveDate::parse_from_str(date_str, fmt) {
        Ok(d) => d,
        Err(e) => panic! ("Unable to parse the date from string for {date_str} in {fmt}, {e}")
    }
}

pub fn naive_date_time_from_string(date_time_str: &str, fmt: &str) -> NaiveDateTime {
    match NaiveDateTime::parse_from_str(date_time_str, fmt) {
        Ok(dt) => dt,
        Err(e) => panic! ("Unable to parse the date time from string for {date_time_str} in {fmt}, {e}")
    }
}

pub fn date_time_timezone_from_string(date_time_str: &str, fmt: &str) -> DateTime<FixedOffset> {
    match DateTime::parse_from_str(date_time_str, fmt) {
        Ok(dt) => dt,
        Err(e) => panic! ("Unable to parse the date time with time zone from string for {date_time_str} in {fmt}, {e}")
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use chrono_tz::Europe;

    #[test]
    fn test_create_date_time() {
        let (year, month, day, hour, min, sec) = (2021, 10, 15, 18, 36, 44);
        let naive_datetime = naive_date_time(year, month, day, hour, min, sec);
        let utc_datetime = utc_date_time(year, month, day, hour, min, sec);
        assert_eq!(naive_date_time_to_utc(naive_datetime), utc_datetime);
    }

    #[test]
    fn test_timestamp_conversion() {
        let (year, month, day, hour, min, sec) = (2021, 10, 15, 18, 36, 44);
        let utc_datetime = utc_date_time(year, month, day, hour, min, sec);
        let timestamp = date_time_to_timestamp(utc_datetime, SecPrecision::Sec);
        assert_eq!(utc_date_time_from_timestamp(timestamp, SecPrecision::Sec), utc_datetime);
    }

    #[test]
    fn test_fixed_offset() {
        let (year, month, day, hour, min, sec) = (2021, 10, 15, 18, 36, 44);
        let offset_hour = 8;
        let naive_datetime = naive_date_time(year, month, day, hour, min, sec);
        let local_datetime = naive_date_time_to_fixed_offset(naive_datetime, offset_hour);
        let utc_datetime = utc_date_time(year, month, day, hour - offset_hour as u32, min, sec);
        assert_eq!(timezone_to_utc_date_time(local_datetime), utc_datetime);
    }

    #[test]
    fn test_timezone() {
        let (year, month, day, hour, min, sec) = (2021, 10, 15, 18, 36, 44);
        let timezone = Europe::London;
        let naive_datetime = naive_date_time(year, month, day, hour, min, sec);
        let local_datetime = naive_date_time_to_timezone(naive_datetime, timezone);
        let utc_datetime = utc_date_time(year, month, day, hour - 1, min, sec);
        assert_eq!(timezone_to_utc_date_time(local_datetime), utc_datetime);
    }
}
