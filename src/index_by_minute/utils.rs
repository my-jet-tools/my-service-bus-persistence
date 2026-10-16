use std::usize;

//cSpell:disable
use chrono::{Datelike, Timelike};
//cSpell:enable
use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::typing::Year;

use super::MinuteWithinYear;

const MINUTES_PER_DAY: u32 = 60 * 24;
pub const INDEX_STEP: usize = 8;

const LAST_DAY_OF_YEAR: usize = 527039;

pub const MINUTE_INDEX_FILE_SIZE: usize = (LAST_DAY_OF_YEAR + 1) * INDEX_STEP;

pub struct IndexByMinuteUtils {
    day_no_in_year: Vec<u32>,
}

impl IndexByMinuteUtils {
    pub fn new() -> Self {
        let mut result = IndexByMinuteUtils {
            day_no_in_year: Vec::new(),
        };

        result.init_days_in_year_index();

        result
    }

    fn init_days_in_year_index(&mut self) {
        self.day_no_in_year.push(0);
        let mut minute = 1;

        //January
        self.day_no_in_year.push(minute);
        minute += 31 * MINUTES_PER_DAY;

        //February
        self.day_no_in_year.push(minute);
        minute += 29 * MINUTES_PER_DAY;

        //March
        self.day_no_in_year.push(minute);
        minute += 31 * MINUTES_PER_DAY;

        //April
        self.day_no_in_year.push(minute);
        minute += 30 * MINUTES_PER_DAY;

        //May
        self.day_no_in_year.push(minute);
        minute += 31 * MINUTES_PER_DAY;

        //June
        self.day_no_in_year.push(minute);
        minute += 30 * MINUTES_PER_DAY;

        //July
        self.day_no_in_year.push(minute);
        minute += 31 * MINUTES_PER_DAY;

        //August
        self.day_no_in_year.push(minute);
        minute += 31 * MINUTES_PER_DAY;

        //September
        self.day_no_in_year.push(minute);
        minute += 30 * MINUTES_PER_DAY;

        //October
        self.day_no_in_year.push(minute);
        minute += 31 * MINUTES_PER_DAY;

        //November
        self.day_no_in_year.push(minute);
        minute += 30 * MINUTES_PER_DAY;

        //December
        self.day_no_in_year.push(minute);
    }

    pub fn get_minute_within_the_year(
        &self,
        dt_micros: DateTimeAsMicroseconds,
    ) -> (MinuteWithinYear, Year) {
        let d = dt_micros.to_chrono_utc();

        let month = d.month();
        let day = d.day();
        let hour = d.hour();
        let minute = d.minute();

        let minute =
            self.day_no_in_year[month as usize] + (day - 1) * MINUTES_PER_DAY + hour * 60 + minute
                - 1;

        (MinuteWithinYear::new(minute), (d.year() as u32).into())
    }
}

#[cfg(test)]
mod tests {

    use rust_extensions::date_time::DateTimeAsMicroseconds;

    use super::IndexByMinuteUtils;

    #[test]
    fn test_minute_within_year() {
        let utils = IndexByMinuteUtils::new();

        let dt = DateTimeAsMicroseconds::parse_iso_string("2021-01-01T00:00:00").unwrap();

        let (minute, _) = utils.get_minute_within_the_year(dt);

        assert_eq!(0, minute.get_value());

        let dt = DateTimeAsMicroseconds::parse_iso_string("2021-01-01T00:00:01").unwrap();

        let (minute, _) = utils.get_minute_within_the_year(dt);

        assert_eq!(0, minute.get_value());

        //First Minute

        let dt = DateTimeAsMicroseconds::parse_iso_string("2021-01-01T00:01:00").unwrap();

        let (minute, _) = utils.get_minute_within_the_year(dt);

        assert_eq!(1, minute.get_value());

        //First Hour

        let dt = DateTimeAsMicroseconds::parse_iso_string("2021-01-01T01:00:05").unwrap();

        let (minute, _) = utils.get_minute_within_the_year(dt);

        assert_eq!(60, minute.get_value());

        //First Hour

        let dt = DateTimeAsMicroseconds::parse_iso_string("2021-05-29T08:50:00").unwrap();

        let (minute, _) = utils.get_minute_within_the_year(dt);

        assert_eq!(215090, minute.get_value());
    }
}
