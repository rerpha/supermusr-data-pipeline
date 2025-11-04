use crate::{
    app::SessionError,
    structs::digitiser_messages::{
        DigitiserEventList, DigitiserMetadata, DigitiserTrace, FromMessage,
    },
};
use digital_muon_streaming_types::{
    dat2_digitizer_analog_trace_v2_generated::DigitizerAnalogTraceMessage,
    dev2_digitizer_event_v2_generated::DigitizerEventListMessage,
    time_conversions::GpsTimeConversionError,
};
use std::collections::{
    BTreeMap,
    btree_map::{self, Entry},
};
use tracing::{error, info};

#[derive(Debug, Clone)]
pub(crate) enum SearchResults {
    Cancelled,
    Successful { cache: Cache },
}

impl SearchResults {
    pub fn cache(&self) -> Result<&Cache, SessionError> {
        match self {
            SearchResults::Cancelled => Err(SessionError::SearchCancelled),
            SearchResults::Successful { cache } => Ok(cache),
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct Cache {
    traces: BTreeMap<DigitiserMetadata, DigitiserTrace>,
    events: BTreeMap<DigitiserMetadata, DigitiserEventList>,
}

impl Cache {
    #[tracing::instrument(skip_all)]
    pub(crate) fn push_trace(
        &mut self,
        msg: &DigitizerAnalogTraceMessage<'_>,
    ) -> Result<(), GpsTimeConversionError> {
        let metadata = DigitiserMetadata {
            id: msg.digitizer_id(),
            timestamp: msg
                .metadata()
                .timestamp()
                .copied()
                .expect("Timestamp should exist.")
                .try_into()?,
            frame_number: msg.metadata().frame_number(),
            period_number: msg.metadata().period_number(),
            protons_per_pulse: msg.metadata().protons_per_pulse(),
            running: msg.metadata().running(),
            veto_flags: msg.metadata().veto_flags(),
        };

        match self.traces.entry(metadata) {
            Entry::Occupied(occupied_entry) => {
                error!("Trace already found: {0:?}", occupied_entry.key());
            }
            Entry::Vacant(vacant_entry) => {
                info!("Trace Entered: {:?}", vacant_entry.key());
                vacant_entry.insert(DigitiserTrace::from_message(msg));
            }
        }
        Ok(())
    }

    pub(crate) fn iter(&self) -> btree_map::Iter<'_, DigitiserMetadata, DigitiserTrace> {
        self.traces.iter()
    }

    #[tracing::instrument(skip_all)]
    pub(crate) fn push_events(
        &mut self,
        msg: &DigitizerEventListMessage<'_>,
    ) -> Result<(), GpsTimeConversionError> {
        let metadata = DigitiserMetadata {
            id: msg.digitizer_id(),
            timestamp: msg
                .metadata()
                .timestamp()
                .copied()
                .expect("Timestamp should exist.")
                .try_into()?,
            frame_number: msg.metadata().frame_number(),
            period_number: msg.metadata().period_number(),
            protons_per_pulse: msg.metadata().protons_per_pulse(),
            running: msg.metadata().running(),
            veto_flags: msg.metadata().veto_flags(),
        };
        match self.events.entry(metadata) {
            Entry::Occupied(occupied_entry) => {
                error!("Event list already found: {0:?}", occupied_entry.key());
            }
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(DigitiserEventList::from_message(msg));
            }
        }
        Ok(())
    }

    pub(crate) fn attach_event_lists_to_trace(&mut self) {
        for (metadata, events) in &self.events {
            match self.traces.entry(metadata.clone()) {
                Entry::Occupied(mut occupied_entry) => {
                    info!("Found Trace for Events");
                    occupied_entry.get_mut().events = Some(events.clone());
                }
                Entry::Vacant(vacant_entry) => {
                    error!("Trace not found: {0:?}", vacant_entry.key());
                }
            }
        }
    }
}
