use crate::{
    Channel, DigitizerId, Timestamp,
    app::sections::search::search_settings::{SearchBy, SearchMode},
    structs::DefaultData,
};
use chrono::{NaiveDate, NaiveTime, Utc};
use leptos::prelude::*;

/// This struct enable a degree of type-checking for the [use_context]/[use_context] functions.
/// Any component making use of the following fields should call `use_context::<SearchLevelContext>()`
/// and select the desired field.
#[derive(Clone)]
pub(crate) struct SearchLevelContext {
    pub(crate) search_mode: RwSignal<SearchMode>,
    pub(crate) search_by: RwSignal<SearchBy>,
    pub(crate) date: RwSignal<NaiveDate>,
    pub(crate) time: RwSignal<NaiveTime>,
    pub(crate) channels: RwSignal<Vec<Channel>>,
    pub(crate) digitiser_ids: RwSignal<Vec<DigitizerId>>,
    pub(crate) number: RwSignal<usize>,
    pub(crate) backstep: RwSignal<i64>,
    pub(crate) forward_distance: RwSignal<usize>,
}

impl SearchLevelContext {
    pub(crate) fn new(default_data: &DefaultData) -> Self {
        let default_timestamp = default_data.timestamp.unwrap_or_else(Utc::now);
        let default_date = default_timestamp.date_naive();
        let default_time = default_timestamp.time();
        let search_by = if default_data.channels.is_some() {
            SearchBy::ByChannels
        } else if default_data.digitiser_ids.is_some() {
            SearchBy::ByDigitiserIds
        } else {
            SearchBy::All
        };

        Self {
            search_mode: RwSignal::new(SearchMode::default()),
            search_by: RwSignal::new(search_by),
            channels: RwSignal::new(default_data.channels.clone().unwrap_or_default()),
            digitiser_ids: RwSignal::new(default_data.digitiser_ids.clone().unwrap_or_default()),
            date: RwSignal::new(default_date),
            time: RwSignal::new(default_time),
            number: RwSignal::new(default_data.number.unwrap_or(1)),
            backstep: RwSignal::new(100),
            forward_distance: RwSignal::new(400),
        }
    }

    pub(crate) fn get_timestamp_with_utc(&self) -> Timestamp {
        self.date.get().and_time(self.time.get()).and_utc()
    }
}
