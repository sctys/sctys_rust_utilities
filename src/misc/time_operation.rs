use chrono::{
    DateTime, Datelike, Duration as LongDuration, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, ParseResult, TimeZone, Timelike, Utc
};
use chrono_tz::Tz;
use mongodb::bson::DateTime as BsonDateTime;
use rand::SeedableRng;
use rand::{thread_rng, Rng, rngs::StdRng};
use std::thread;
use std::time::{Duration, SystemTime};
use tokio::time;

const SEC_TO_HOUR: i32 = 3600;
const ONE_E3: i64 = 1_000;
const ONE_E6: i64 = 1_000_000;
const ONE_E9: i64 = 1_000_000_000;

pub fn sleep(sleep_time: Duration) {
    thread::sleep(sleep_time);
}

pub fn random_sleep((min_sleep_time, max_sleep_time): (Duration, Duration)) {
    if min_sleep_time == max_sleep_time {
        thread::sleep(min_sleep_time)
    } else {
        let mut rng = thread_rng();
        let sleep_time = rng.gen_range(min_sleep_time..max_sleep_time);
        thread::sleep(sleep_time);
    }
}

pub async fn async_sleep(sleep_time: Duration) {
    time::sleep(sleep_time).await;
}

pub async fn async_random_sleep((min_sleep_time, max_sleep_time): (Duration, Duration)) {
    if min_sleep_time == max_sleep_time {
        time::sleep(min_sleep_time).await
    } else {
        let mut rng = StdRng::from_entropy();
        let sleep_time = rng.gen_range(min_sleep_time..max_sleep_time);
        time::sleep(sleep_time).await;
    }
}

pub enum SecPrecision {
    Sec,
    MilliSec,
    MicroSec,
    NanoSec,
}

pub fn utc_start_of_today() -> DateTime<Utc> {
    let date_time = Utc::now();
    date_time
        - LongDuration::hours(date_time.hour().into())
        - LongDuration::minutes(date_time.minute().into())
        - LongDuration::seconds(date_time.second().into())
}

pub fn timestamp_now(precision: SecPrecision) -> i64 {
    match precision {
        SecPrecision::Sec => Utc::now().timestamp(),
        SecPrecision::MilliSec => Utc::now().timestamp_millis(),
        SecPrecision::MicroSec => Utc::now().timestamp_micros(),
        SecPrecision::NanoSec => Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_else(|| panic!("Error in parsing timestmap now to nanoseconds.")),
    }
}

pub fn system_time_now() -> SystemTime {
    SystemTime::now()
}

pub fn system_time_to_timestamp(system_time: &SystemTime, precision: SecPrecision) -> i64 {
    match system_time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(st) => match precision {
            SecPrecision::Sec => st.as_secs() as i64,
            SecPrecision::MilliSec => st.as_millis() as i64,
            SecPrecision::MicroSec => st.as_micros() as i64,
            SecPrecision::NanoSec => st.as_nanos() as i64,
        },
        Err(e) => panic!("Unable to convert the system time {system_time:?} to timestamp. {e}"),
    }
}

pub fn diff_system_time_date_time_sec<T: TimeZone>(
    system_time: &SystemTime,
    date_time: &DateTime<T>,
) -> i64 {
    let system_timestamp = system_time_to_timestamp(system_time, SecPrecision::Sec);
    let date_timestamp = date_time_to_timestamp(date_time, SecPrecision::Sec);
    system_timestamp - date_timestamp
}

pub fn get_year<T: TimeZone>(date_time: &DateTime<T>) -> i32 {
    date_time.year()
}

pub fn get_month<T: TimeZone>(date_time: &DateTime<T>) -> u32 {
    date_time.month()
}

pub fn get_day<T: TimeZone>(date_time: &DateTime<T>) -> u32 {
    date_time.day()
}

pub fn naive_date(year: i32, month: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, day)
        .unwrap_or_else(|| panic!("Invalid date {year}, {month}, {day}"))
}

pub fn naive_date_time(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    min: u32,
    sec: u32,
) -> NaiveDateTime {
    let date = naive_date(year, month, day);
    let time = NaiveTime::from_hms_opt(hour, min, sec)
        .unwrap_or_else(|| panic!("Invalid time {hour}, {min}, {sec}"));
    NaiveDateTime::new(date, time)
}

pub fn naive_date_to_naive_date_time(naive_date: &NaiveDate) -> NaiveDateTime {
    let year = naive_date.year();
    let month = naive_date.month();
    let day = naive_date.day();
    naive_date_time(year, month, day, 0, 0, 0)
}

pub fn utc_date_time(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    min: u32,
    sec: u32,
) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(year, month, day, hour, min, sec)
        .single()
        .unwrap_or_else(|| {
            panic!("Unable to construct the date time {year}, {month}, {day}, {hour}, {min}, {sec}")
        })
}

pub fn naive_date_time_to_utc(naive_date_time: &NaiveDateTime) -> DateTime<Utc> {
    Utc.from_utc_datetime(naive_date_time)
}

pub fn date_time_to_timestamp<T: TimeZone>(
    date_time: &DateTime<T>,
    precision: SecPrecision,
) -> i64 {
    match precision {
        SecPrecision::Sec => date_time.timestamp(),
        SecPrecision::MilliSec => date_time.timestamp_millis(),
        SecPrecision::MicroSec => date_time.timestamp_micros(),
        SecPrecision::NanoSec => date_time
            .timestamp_nanos_opt()
            .unwrap_or_else(|| panic!("Error in parsing timestmap now to nanoseconds.")),
    }
}

pub fn date_time_to_int<T: TimeZone>(date_time: &DateTime<T>) -> i32 {
    date_time.year() * 10000 + date_time.month() as i32 * 100 + date_time.day() as i32
}

pub fn naive_date_to_int(naive_date: &NaiveDate) -> i32 {
    naive_date.year() * 10000 + naive_date.month() as i32 * 100 + naive_date.day() as i32
}

pub fn naive_date_time_to_int(naive_date_time: &NaiveDateTime) -> i32 {
    naive_date_time.year() * 10000
        + naive_date_time.month() as i32 * 100
        + naive_date_time.day() as i32
}

pub fn parse_int_to_utc_date_time(date_int: i32) -> DateTime<Utc> {
    let year = date_int / 10000;
    let month = (date_int % 10000) / 100;
    let day = date_int % 100;
    utc_date_time(year, month as u32, day as u32, 0, 0, 0)
}

pub fn date_time_to_month<T: TimeZone>(date_time: &DateTime<T>) -> i32 {
    date_time.year() * 100 + date_time.month() as i32
}

pub fn get_utc_start_of_the_month(date_time: &DateTime<Utc>) -> DateTime<Utc> {
    let year = date_time.year();
    let month = date_time.month();
    let day = 1;
    let hour = 0;
    let min = 0;
    let sec = 0;
    utc_date_time(year, month, day, hour, min, sec)
}

pub fn utc_date_time_from_timestamp(timestamp: i64, precision: SecPrecision) -> DateTime<Utc> {
    let (secs, nsecs) = match precision {
        SecPrecision::Sec => (timestamp, 0),
        SecPrecision::MilliSec => (timestamp / ONE_E3, (timestamp % ONE_E3 * ONE_E6) as u32),
        SecPrecision::MicroSec => (timestamp / ONE_E6, (timestamp % ONE_E6 * ONE_E3) as u32),
        SecPrecision::NanoSec => (timestamp / ONE_E9, (timestamp % ONE_E9) as u32),
    };
    DateTime::from_timestamp(secs, nsecs).unwrap_or_else(|| panic!("Invalid timestamp {timestamp}"))
}

fn fixed_offset_from_hour(hour: i32) -> FixedOffset {
    FixedOffset::east_opt(hour * SEC_TO_HOUR)
        .unwrap_or_else(|| panic!("Invalid time offset {hour}"))
}

pub fn naive_date_time_to_fixed_offset(
    naive_date_time: &NaiveDateTime,
    hour: i32,
) -> DateTime<FixedOffset> {
    let offset = fixed_offset_from_hour(hour);
    offset.from_local_datetime(naive_date_time).unwrap()
}

pub fn utc_date_time_to_fixed_offset(
    date_time: &DateTime<Utc>,
    hour: i32,
) -> DateTime<FixedOffset> {
    let offset = fixed_offset_from_hour(hour);
    date_time.with_timezone(&offset)
}

pub fn timezone_to_utc_date_time<T: TimeZone>(date_time: &DateTime<T>) -> DateTime<Utc> {
    date_time.with_timezone(&Utc)
}

pub fn naive_date_time_to_timezone(
    naive_date_time: &NaiveDateTime,
    timezone: Tz,
) -> Option<DateTime<Tz>> {
    timezone.from_local_datetime(naive_date_time).earliest()
}

pub fn utc_date_time_to_timezone(date_time: &DateTime<Utc>, timezone: Tz) -> DateTime<Tz> {
    date_time.with_timezone(&timezone)
}

pub fn naive_date_from_string(date_str: &str, fmt: &str) -> ParseResult<NaiveDate> {
    NaiveDate::parse_from_str(date_str, fmt)
}

pub fn naive_date_time_from_string(date_time_str: &str, fmt: &str) -> ParseResult<NaiveDateTime> {
    NaiveDateTime::parse_from_str(date_time_str, fmt)
}

pub fn date_time_timezone_from_string(date_time_str: &str, fmt: &str) -> ParseResult<DateTime<FixedOffset>> {
    DateTime::parse_from_rfc3339(date_time_str).or_else(|_| DateTime::parse_from_str(date_time_str, fmt))
}

pub fn utc_date_range(start: DateTime<Utc>, end: DateTime<Utc>) -> Vec<DateTime<Utc>> {
    let mut dates = Vec::new();
    let mut current = start;

    while current <= end {
        dates.push(current);
        current += chrono::Duration::days(1);
    }

    dates
}

pub fn convert_date_time_to_bson<T: TimeZone>(date_time: &DateTime<T>) -> BsonDateTime {
    BsonDateTime::from_millis(date_time.timestamp_millis())
}

pub fn convert_system_time_to_bson(system_time: SystemTime) -> BsonDateTime {
    BsonDateTime::from_system_time(system_time)
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
        assert_eq!(naive_date_time_to_utc(&naive_datetime), utc_datetime);
    }

    #[test]
    fn test_timestamp_conversion() {
        let (year, month, day, hour, min, sec) = (2021, 10, 15, 18, 36, 44);
        let utc_datetime = utc_date_time(year, month, day, hour, min, sec);
        let timestamp = date_time_to_timestamp(&utc_datetime, SecPrecision::Sec);
        assert_eq!(
            utc_date_time_from_timestamp(timestamp, SecPrecision::Sec),
            utc_datetime
        );
    }

    #[test]
    fn test_int_date_to_utc_datetime() {
        let int_date = 20220121;
        let (year, month, day, hour, min, sec) = (2022, 1, 21, 0, 0, 0);
        let utc_datetime = utc_date_time(year, month, day, hour, min, sec);
        assert_eq!(parse_int_to_utc_date_time(int_date), utc_datetime)
    }

    #[test]
    fn test_fixed_offset() {
        let (year, month, day, hour, min, sec) = (2021, 10, 15, 18, 36, 44);
        let offset_hour = 8;
        let naive_datetime = naive_date_time(year, month, day, hour, min, sec);
        let local_datetime = naive_date_time_to_fixed_offset(&naive_datetime, offset_hour);
        let utc_datetime = utc_date_time(year, month, day, hour - offset_hour as u32, min, sec);
        assert_eq!(timezone_to_utc_date_time(&local_datetime), utc_datetime);
    }

    #[test]
    fn test_timezone() {
        let (year, month, day, hour, min, sec) = (2021, 10, 15, 18, 36, 44);
        let timezone = Europe::London;
        let naive_datetime = naive_date_time(year, month, day, hour, min, sec);
        let local_datetime = naive_date_time_to_timezone(&naive_datetime, timezone).unwrap();
        let utc_datetime = utc_date_time(year, month, day, hour - 1, min, sec);
        assert_eq!(timezone_to_utc_date_time(&local_datetime), utc_datetime);
    }
}
