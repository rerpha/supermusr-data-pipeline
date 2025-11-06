use chrono::{DateTime, Utc};
use digital_muon_common::{Channel, DigitizerId, Intensity};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::daq_trace::app::format_timestamp;

pub(crate) type DigitiserDataHashMap = Arc<Mutex<HashMap<u8, DigitiserData>>>;

const NUM_COLUMNS: usize = 10;

const COLUMNS: [(&str, u16); NUM_COLUMNS] = [
    ("Digitiser ID", 2),            // 1
    ("#Msgs Received", 2),          // 2
    ("First Msg Timestamp", 3),     // 3
    ("Last Msg Timestamp", 3),      // 4
    ("Last Msg Frame", 2),          // 5
    ("Message Rate (Hz)", 2),       // 6
    ("#Channels Present", 2),       // 7
    ("#Samples", 3),                // 8
    ("#Bad Frames?", 3),            // 9
    ("Channel\n(Sample Range)", 4), // 10
];

/// Holds required data for a specific digitiser.
#[derive(Clone)]
pub(crate) struct ChannelData {
    pub(crate) index: usize,
    pub(crate) id: Channel,
    pub(crate) max: Intensity,
    pub(crate) min: Intensity,
}

impl ChannelData {
    fn new() -> Self {
        Self {
            index: 0,
            id: 0,
            max: Intensity::MIN,
            min: Intensity::MAX,
        }
    }
}

/// Holds required data for a specific digitiser.
pub(crate) struct DigitiserData {
    pub(crate) msg_count: usize,
    pub(crate) last_msg_count: usize,
    pub(crate) msg_rate: f64,
    pub(crate) first_msg_timestamp: Option<DateTime<Utc>>,
    pub(crate) last_msg_timestamp: Option<DateTime<Utc>>,
    pub(crate) last_msg_frame: u32,
    pub(crate) num_channels_present: usize,
    pub(crate) has_num_channels_changed: bool,
    pub(crate) num_samples_in_first_channel: usize,
    pub(crate) is_num_samples_identical: bool,
    pub(crate) has_num_samples_changed: bool,
    pub(crate) bad_frame_count: usize,
    pub(crate) channel_data: ChannelData,
}

impl DigitiserData {
    /// Create a new instance with default values.
    pub(crate) fn new(
        timestamp: Option<DateTime<Utc>>,
        frame: u32,
        num_channels_present: usize,
        num_samples_in_first_channel: usize,
        is_num_samples_identical: bool,
    ) -> Self {
        DigitiserData {
            msg_count: 1,
            msg_rate: 0 as f64,
            last_msg_count: 1,
            first_msg_timestamp: timestamp,
            last_msg_timestamp: timestamp,
            last_msg_frame: frame,
            num_channels_present,
            has_num_channels_changed: false,
            num_samples_in_first_channel,
            is_num_samples_identical,
            has_num_samples_changed: false,
            bad_frame_count: 0,
            channel_data: ChannelData::new(),
        }
    }

    /// Update instance with new data
    pub(crate) fn update(
        &mut self,
        timestamp: Option<DateTime<Utc>>,
        frame_number: u32,
        num_channels_present: usize,
        num_samples_in_first_channel: usize,
        is_num_samples_identical: bool,
    ) {
        self.msg_count += 1;

        self.last_msg_timestamp = timestamp;
        self.last_msg_frame = frame_number;

        if timestamp.is_none() {
            self.bad_frame_count += 1;
        }

        if !self.has_num_channels_changed {
            self.has_num_channels_changed = num_channels_present != self.num_channels_present;
        }
        self.num_channels_present = num_channels_present;
        if !self.has_num_channels_changed {
            self.has_num_samples_changed =
                num_samples_in_first_channel != self.num_samples_in_first_channel;
        }
        self.num_samples_in_first_channel = num_samples_in_first_channel;
        self.is_num_samples_identical = is_num_samples_identical;
    }

    pub(crate) fn generate_headers() -> Vec<String> {
        COLUMNS
            .map(|x| x.0)
            .into_iter()
            .map(ToString::to_string)
            .collect()
    }

    pub(crate) fn generate_row(&self, digitiser_id: DigitizerId) -> Vec<String> {
        vec![
            // 1. Digitiser ID.
            digitiser_id.to_string(),
            // 2. Number of messages received.
            format!("{}", self.msg_count),
            // 3. First message timestamp.
            format_timestamp(self.first_msg_timestamp),
            // 4. Last message timestamp.
            format_timestamp(self.last_msg_timestamp),
            // 5. Last message frame.
            format!("{}", self.last_msg_frame),
            // 6. Message rate.
            format!("{:.1}", self.msg_rate),
            // 7. Number of channels present.
            format!(
                "{}\n{}",
                self.num_channels_present,
                match self.has_num_channels_changed {
                    true => "unstable",
                    false => "stable",
                }
            ),
            // 8. Number of samples in the first channel.
            format!(
                "{}\n{}\n{}",
                self.num_samples_in_first_channel,
                match self.is_num_samples_identical {
                    true => "all channels",
                    false => "first channel only",
                },
                match self.has_num_samples_changed {
                    true => "unstable",
                    false => "stable",
                }
            ),
            // 9. Number of Bad Frames
            format!("{}", self.bad_frame_count),
            // 10. Channel Data
            format!(
                "{}: {}\n({}:{})",
                self.channel_data.index,
                self.channel_data.id,
                self.channel_data.min,
                self.channel_data.max
            ),
        ]
    }

    pub(crate) fn width_percentages() -> Vec<u16> {
        let weights = COLUMNS.map(|x| x.1);
        let sum = weights.iter().sum::<u16>();
        weights.iter().copied().map(|w| w * 100 / sum).collect()
    }
}
