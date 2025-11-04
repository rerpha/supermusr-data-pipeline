//! Converts borrowed trace and eventlist flatbuffer messages into convenient structures.
use crate::{Channel, DigitizerId, FrameNumber, Intensity, Time, Timestamp};
use cfg_if::cfg_if;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Timeseries of signal intensities.
///
/// The time and value scaling is not stored here, so interpretation is owner dependent.
pub(crate) type Trace = Vec<Intensity>;

/// Bundles all metadata which uniquely defines each digitiser message.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub(crate) struct DigitiserMetadata {
    /// Unique to each frame.
    pub(crate) timestamp: Timestamp,
    /// Unique to each digitiser.
    pub(crate) id: DigitizerId,
    /// The Frame Number.
    pub(crate) frame_number: FrameNumber,
    /// The Period Number.
    pub(crate) period_number: u64,
    /// The Protons per Pulse.
    pub(crate) protons_per_pulse: u8,
    /// The Running Flag.
    pub(crate) running: bool,
    /// The Veto Flags.
    pub(crate) veto_flags: u16,
}

/// Encapsulates all traces of a digitiser trace message.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct DigitiserTrace {
    /// Maps channels to traces.
    pub(crate) traces: HashMap<Channel, Trace>,
    /// If present, maps channels to [EventList]s.
    pub(crate) events: Option<DigitiserEventList>,
}

/// A pair defining a muon detection.
#[derive(Clone, Debug, PartialEq, Copy, Serialize, Deserialize)]
pub(crate) struct Event {
    /// The time the detection occured.
    pub(crate) time: Time,
    /// The intensity of the detection.
    pub(crate) intensity: Intensity,
}

/// A list of muon detection events.
///
/// The time and value scaling is not stored here, so interpretation is owner dependent.
/// N.B. in practice, these should be consecuitve in time, but this is not checked, nor required.
pub(crate) type EventList = Vec<Event>;

/// Maps each [Channel] to a unique [EventList].
pub(crate) type DigitiserEventList = HashMap<Channel, EventList>;

cfg_if! {
    if #[cfg(feature = "ssr")] {
        use digital_muon_streaming_types::{
            dat2_digitizer_analog_trace_v2_generated::DigitizerAnalogTraceMessage,
            dev2_digitizer_event_v2_generated::DigitizerEventListMessage,
        };

        /// Provides method for creating object from a generic message.
        ///
        /// This trait is used instead of [From<&M>] so it can be implemented for [DigitizerEventListMessage],
        /// which is an alias of [HashMap].
        /// Rust does not allow foreign traits to be implemented on foreign types.
        pub(crate) trait FromMessage<M> {
            /// Performs the same function as [From::from].
            fn from_message(msg: M) -> Self;
        }

        impl FromMessage<&DigitizerAnalogTraceMessage<'_>> for DigitiserTrace {
            fn from_message(msg: &DigitizerAnalogTraceMessage) -> Self {
                let pairs: Vec<(Channel, Trace)> = msg
                    .channels()
                    .unwrap()
                    .iter()
                    .map(|x| (x.channel(), x.voltage().unwrap().iter().collect()))
                    .collect();
                let traces: HashMap<Channel, Trace> = HashMap::from_iter(pairs);
                DigitiserTrace {
                    traces,
                    events: None,
                }
            }
        }

        impl FromMessage<&DigitizerEventListMessage<'_>> for DigitiserEventList {
            fn from_message(msg: &DigitizerEventListMessage) -> Self {
                let mut events = HashMap::<Channel, EventList>::new();
                for (idx, chnl) in msg.channel().unwrap().iter().enumerate() {
                    events.entry(chnl).or_default().push(Event {
                        time: msg.time().unwrap().get(idx),
                        intensity: msg.voltage().unwrap().get(idx),
                    })
                }
                events
            }
        }
    }
}
