//! Handles borrowed trace and eventlist flatbuffer messages.
use crate::{Timestamp, structs::SearchTargetBy};
use digital_muon_common::{Channel, DigitizerId};
use digital_muon_streaming_types::{
    dat2_digitizer_analog_trace_v2_generated::{
        DigitizerAnalogTraceMessage, digitizer_analog_trace_message_buffer_has_identifier,
        root_as_digitizer_analog_trace_message,
    },
    dev2_digitizer_event_v2_generated::{
        DigitizerEventListMessage, digitizer_event_list_message_buffer_has_identifier,
        root_as_digitizer_event_list_message,
    },
    flatbuffers::InvalidFlatbuffer,
    time_conversions::GpsTimeConversionError,
};
use rdkafka::{Message, message::BorrowedMessage};
use std::ops::Deref;
use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum BorrowedMessageError {
    #[error("Time Conversion Error: {0}")]
    TimeConversion(#[from] GpsTimeConversionError),
    #[error("Time Not Present")]
    TimeMissing,
    #[error("Cannot Unpack Message")]
    Unpack(#[from] InvalidFlatbuffer),
    #[error("Invalid Message Identifier")]
    InvalidIdentifier,
}

/// Encapsulates functionality to represent a borrowed flatbuffer message with a [Timestamp] and [DigitizerId].
pub(crate) trait FBMessage<'a>:
    Sized
    + TryFrom<BorrowedMessage<'a>, Error = BorrowedMessageError>
    + Deref<Target = BorrowedMessage<'a>>
{
    /// Represents the borrowed message when unpacked into a usable object.
    type UnpackedMessage;

    fn try_unpacked_message(&'a self) -> Result<Self::UnpackedMessage, BorrowedMessageError>;

    /// Returns the timestamp of the message.
    fn timestamp(&self) -> Timestamp;

    /// Returns the digitiser id of the message.
    fn digitiser_id(&self) -> DigitizerId;
}

pub(crate) struct TraceMessage<'a> {
    message: BorrowedMessage<'a>,
    timestamp: Timestamp,
    digitiser_id: DigitizerId,
}

impl<'a> TraceMessage<'a> {
    pub(crate) fn has_channel(&self, channel: Channel) -> bool {
        self.try_unpacked_message()
            .ok()
            .and_then(|d| d.channels())
            .and_then(|c| c.iter().find(|c| c.channel() == channel))
            .is_some()
    }

    pub(crate) fn filter_by(&self, by: &SearchTargetBy) -> bool {
        match by {
            SearchTargetBy::All => true,
            SearchTargetBy::ByChannels { channels } => {
                channels.iter().any(|&c| self.has_channel(c))
            }
            SearchTargetBy::ByDigitiserIds { digitiser_ids } => {
                digitiser_ids.iter().any(|&d: &u8| self.digitiser_id() == d)
            }
        }
    }
}

impl<'a> Deref for TraceMessage<'a> {
    type Target = BorrowedMessage<'a>;

    fn deref(&self) -> &BorrowedMessage<'a> {
        &self.message
    }
}

impl<'a> TryFrom<BorrowedMessage<'a>> for TraceMessage<'a> {
    type Error = BorrowedMessageError;

    fn try_from(message: BorrowedMessage<'a>) -> Result<Self, Self::Error> {
        let trace = message.unpack_trace_message()?;

        let timestamp = trace
            .metadata()
            .timestamp()
            .copied()
            .ok_or(BorrowedMessageError::TimeMissing)?
            .try_into()?;
        let digitiser_id = trace.digitizer_id();

        Ok(Self {
            message,
            timestamp,
            digitiser_id,
        })
    }
}

impl<'a> FBMessage<'a> for TraceMessage<'a> {
    type UnpackedMessage = DigitizerAnalogTraceMessage<'a>;

    fn try_unpacked_message(&'a self) -> Result<Self::UnpackedMessage, BorrowedMessageError> {
        self.message.unpack_trace_message()
    }

    fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    fn digitiser_id(&self) -> DigitizerId {
        self.digitiser_id
    }
}

pub(crate) struct EventListMessage<'a> {
    message: BorrowedMessage<'a>,
    timestamp: Timestamp,
    digitiser_id: DigitizerId,
}

impl<'a> EventListMessage<'a> {
    pub(crate) fn filter_by_digitiser_id(&self, digitiser_ids: &[DigitizerId]) -> bool {
        digitiser_ids.iter().any(|&d: &u8| self.digitiser_id() == d)
    }
}

impl<'a> Deref for EventListMessage<'a> {
    type Target = BorrowedMessage<'a>;

    fn deref(&self) -> &BorrowedMessage<'a> {
        &self.message
    }
}

impl<'a> TryFrom<BorrowedMessage<'a>> for EventListMessage<'a> {
    type Error = BorrowedMessageError;

    fn try_from(message: BorrowedMessage<'a>) -> Result<Self, Self::Error> {
        let evlist = message.unpack_event_list_message()?;

        let timestamp = evlist
            .metadata()
            .timestamp()
            .copied()
            .ok_or(BorrowedMessageError::TimeMissing)?
            .try_into()?;

        let digitiser_id = evlist.digitizer_id();

        Ok(Self {
            message,
            timestamp,
            digitiser_id,
        })
    }
}

impl<'a> FBMessage<'a> for EventListMessage<'a> {
    type UnpackedMessage = DigitizerEventListMessage<'a>;

    fn try_unpacked_message(&'a self) -> Result<Self::UnpackedMessage, BorrowedMessageError> {
        self.message.unpack_event_list_message()
    }

    fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    fn digitiser_id(&self) -> DigitizerId {
        self.digitiser_id
    }
}

pub(crate) trait UnpackMessage<'a> {
    fn unpack_trace_message(
        &'a self,
    ) -> Result<DigitizerAnalogTraceMessage<'a>, BorrowedMessageError>;

    fn unpack_event_list_message(
        &'a self,
    ) -> Result<DigitizerEventListMessage<'a>, BorrowedMessageError>;
}

impl<'a> UnpackMessage<'a> for BorrowedMessage<'a> {
    fn unpack_trace_message(
        &'a self,
    ) -> Result<DigitizerAnalogTraceMessage<'a>, BorrowedMessageError> {
        root_as_digitizer_analog_trace_message(
            self.payload()
                .filter(|payload| digitizer_analog_trace_message_buffer_has_identifier(payload))
                .ok_or(BorrowedMessageError::InvalidIdentifier)?,
        )
        .map_err(Into::into)
    }

    fn unpack_event_list_message(
        &'a self,
    ) -> Result<DigitizerEventListMessage<'a>, BorrowedMessageError> {
        root_as_digitizer_event_list_message(
            self.payload()
                .filter(|payload| digitizer_event_list_message_buffer_has_identifier(payload))
                .ok_or(BorrowedMessageError::InvalidIdentifier)?,
        )
        .map_err(Into::into)
    }
}
