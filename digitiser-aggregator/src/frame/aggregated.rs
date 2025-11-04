//! Defines the struct for a frame which is ready to be dispatched.
use super::partial::PartialFrame;
use crate::data::{Accumulate, DigitiserData};
use digital_muon_common::{
    DigitizerId,
    spanned::{SpanOnce, Spanned, SpannedMut},
};
use digital_muon_streaming_types::FrameMetadata;

/// A frame with that is ready to be dispatched.
pub(crate) struct AggregatedFrame<D> {
    /// Used by the implementation of [SpannedAggregator].
    ///
    /// [SpannedAggregator]: digital_muon_common::spanned::SpannedAggregator
    span: SpanOnce,
    /// The uniquely identifying metadata of the frame, common to all digitiser messages related to this frame (except possibly for [FrameMetadata::veto_flags]).
    pub(crate) metadata: FrameMetadata,
    /// Is `true` if and only if the frame has received data frame all expected digitisers.
    pub(crate) complete: bool,
    /// List of digitisers from which the frame has received data.
    pub(crate) digitiser_ids: Vec<DigitizerId>,
    /// The frame's event data.
    pub(crate) digitiser_data: D,
}

#[cfg(test)]
impl<D> AggregatedFrame<D> {
    pub(crate) fn new(
        metadata: FrameMetadata,
        complete: bool,
        digitiser_ids: Vec<DigitizerId>,
        digitiser_data: D,
    ) -> Self {
        Self {
            span: Default::default(),
            metadata,
            complete,
            digitiser_ids,
            digitiser_data,
        }
    }
}

impl<D> From<PartialFrame<D>> for AggregatedFrame<D>
where
    DigitiserData<D>: Accumulate<D>,
{
    fn from(mut partial: PartialFrame<D>) -> Self {
        Self {
            span: partial
                .span_mut()
                .take()
                .expect("partial frame should have a span"),
            metadata: partial.metadata.clone(),
            complete: partial.is_complete(),
            digitiser_ids: partial.digitiser_ids(),
            digitiser_data: <DigitiserData<D> as Accumulate<D>>::accumulate(
                &mut partial.digitiser_data,
            ),
        }
    }
}

impl<D> Spanned for AggregatedFrame<D> {
    fn span(&self) -> &SpanOnce {
        &self.span
    }
}
