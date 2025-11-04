//! Defines the data type used in [FrameCache].
//!
//! [FrameCache]: crate::frame::FrameCache
mod event;
pub(crate) use event::EventData;

use digital_muon_common::DigitizerId;

pub(crate) type DigitiserData<T> = Vec<(DigitizerId, T)>;

/// Provides a function to accumulate from a collection whose data is tagged by [DigitizerId].
pub(crate) trait Accumulate<D> {
    /// Accumulates a collection whose data is tagged by [DigitizerId] into a single instance of the data object.
    fn accumulate(data: &mut DigitiserData<D>) -> D;
}
