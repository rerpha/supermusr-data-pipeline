use crate::frame_metadata_v1_generated::GpsTime;
use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc};

impl From<GpsTime> for DateTime<Utc> {
    fn from(t: GpsTime) -> Self {
        println!("t {:?}", t);

        let nanosecond = (t.millisecond() as u32 * 1_000_000)
            + (t.microsecond() as u32 * 1_000)
            + (t.nanosecond() as u32);

        NaiveDate::from_yo_opt(2000 + (t.year() as i32), t.day().into())
            .expect("Date should be valid")
            .and_hms_nano_opt(
                t.hour().into(),
                t.minute().into(),
                t.second().into(),
                nanosecond,
            )
            .expect("Time should be valid")
            .and_local_timezone(Utc)
            .unwrap()
    }
}

impl From<DateTime<Utc>> for GpsTime {
    fn from(t: DateTime<Utc>) -> Self {
        Self::new(
            (t.year() - 2000) as u8,
            t.ordinal() as u16,
            t.hour() as u8,
            t.minute() as u8,
            t.second() as u8,
            (t.nanosecond() / 1_000_000) as u16,
            ((t.nanosecond() % 1_000_000) / 1_000) as u16,
            (t.nanosecond() % 1_000) as u16,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gpstime_to_datetimeutc() {
        let t1 = GpsTime::new(22, 205, 14, 52, 22, 100, 200, 300);

        let t2: DateTime<Utc> = t1.into();

        assert_eq!(t2.year(), 2022);
        assert_eq!(t2.month(), 7);
        assert_eq!(t2.day(), 24);

        assert_eq!(t2.hour(), 14);
        assert_eq!(t2.minute(), 52);
        assert_eq!(t2.second(), 22);

        assert_eq!(t2.nanosecond(), 100200300);
    }

    #[test]
    fn test_datetimeutc_to_gpstime() {
        let t1 = NaiveDate::from_ymd_opt(2022, 7, 24)
            .unwrap()
            .and_hms_nano_opt(14, 52, 22, 100200300)
            .unwrap()
            .and_local_timezone(Utc)
            .unwrap();

        let t2: GpsTime = t1.into();

        assert_eq!(t2, GpsTime::new(22, 205, 14, 52, 22, 100, 200, 300));
    }
}
