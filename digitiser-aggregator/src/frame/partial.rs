//! Defines the struct for a frame which is awaiting data from digitiser messages.
use crate::data::DigitiserData;
use digital_muon_common::{
    DigitizerId,
    spanned::{SpanOnce, SpanOnceError, Spanned, SpannedAggregator, SpannedMut},
};
use digital_muon_streaming_types::FrameMetadata;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{Span, info_span};

/// Holds the data of a frame, whislt it is in cache being built from digitiser messages.
pub(crate) struct PartialFrame<D> {
    /// Used by the implementation of [SpannedAggregator].
    ///
    /// [SpannedAggregator]: digital_muon_common::spanned::SpannedAggregator
    span: SpanOnce,
    /// IS `true` if and only if all expected digitiser messages have been collected.
    complete: bool,
    /// Time at which the partial frame should be considered expired, and can be dispatched
    /// from the cache even if incomplete.
    expiry: Instant,
    /// The uniquely identifying metadata of the frame, common to all digitiser messages related to this frame (except possibly for [FrameMetadata::veto_flags]).
    pub(super) metadata: FrameMetadata,
    /// The frame's event data.
    pub(super) digitiser_data: DigitiserData<D>,
}

impl<D> PartialFrame<D> {
    pub(super) fn new(ttl: Duration, metadata: FrameMetadata) -> Self {
        let expiry = Instant::now() + ttl;

        Self {
            span: SpanOnce::default(),
            complete: false,
            expiry,
            metadata,
            digitiser_data: Default::default(),
        }
    }

    /// Returns an ordered non-repeating vector of [DigitizerId]s that have currently been collected.
    pub(super) fn digitiser_ids(&self) -> Vec<DigitizerId> {
        let mut cache_digitiser_ids: Vec<DigitizerId> =
            self.digitiser_data.iter().map(|i| i.0).collect();
        cache_digitiser_ids.sort();
        cache_digitiser_ids
    }

    /// Sets the [self.complete] flag to true only if [Self::digitiser_ids] returns
    /// a list equal to the given `expected_digitisers`.
    /// Note that `expected_digitisers` must be increasing and non-repeating, otherwise the
    /// [self.complete] flag is never set. This is not checked, and left to the user.
    ///
    /// [self.complete]: Self::complete
    pub(super) fn set_completion_status(&mut self, expected_digitisers: &[DigitizerId]) {
        if self.digitiser_ids() == expected_digitisers {
            self.complete = true;
        }
    }

    /// Returns `true` if and only if this provided [DigitizerId] has been seen before.
    pub(super) fn has_digitiser_id(&self, did: DigitizerId) -> bool {
        self.digitiser_data.iter().any(|(id, _)| did == *id)
    }

    /// Pushes the given data from a digitser to the frame.
    /// # Parameters
    /// - digitiser_id: the id of the digitiser sending the data.
    /// - data: the data in the message.
    pub(super) fn push(&mut self, digitiser_id: DigitizerId, data: D) {
        self.digitiser_data.push((digitiser_id, data));
    }

    /// Ammends the metadata [veto_flags] field with `veto_flags` from a new digitiser message.
    /// This is necessary until it is determined whether [veto_flags] should be identical accross
    /// all digitisers during a frame.
    ///
    /// [veto_flags]: FrameMetadata::veto_flags
    pub(super) fn push_veto_flags(&mut self, veto_flags: u16) {
        self.metadata.veto_flags |= veto_flags;
    }

    /// Returns value of [self.complete].
    ///
    /// [self.complete]: Self::complete
    pub(super) fn is_complete(&self) -> bool {
        self.complete
    }

    /// Returns `true` if and only if the current time instant is greater than `self.expiry`
    pub(super) fn is_expired(&self) -> bool {
        Instant::now() > self.expiry
    }
}

impl<D> Spanned for PartialFrame<D> {
    fn span(&self) -> &SpanOnce {
        &self.span
    }
}

impl<D> SpannedMut for PartialFrame<D> {
    fn span_mut(&mut self) -> &mut SpanOnce {
        &mut self.span
    }
}

impl<D> SpannedAggregator for PartialFrame<D> {
    fn span_init(&mut self) -> Result<(), SpanOnceError> {
        self.span.init(info_span!(parent: None, "Frame",
            "metadata_timestamp" = self.metadata.timestamp.to_rfc3339(),
            "metadata_frame_number" = self.metadata.frame_number,
            "metadata_period_number" = self.metadata.period_number,
            "metadata_veto_flags" = self.metadata.veto_flags,
            "metadata_protons_per_pulse" = self.metadata.protons_per_pulse,
            "metadata_running" = self.metadata.running,
            "frame_is_expired" = tracing::field::Empty,
        ))
    }

    fn link_current_span<F: Fn() -> Span>(
        &self,
        aggregated_span_fn: F,
    ) -> Result<(), SpanOnceError> {
        let span = self.span.get()?.in_scope(aggregated_span_fn);
        span.follows_from(tracing::Span::current());
        Ok(())
    }

    fn end_span(&self) -> Result<(), SpanOnceError> {
        #[cfg(not(test))] //   In test mode, the frame.span() are not initialised
        self.span()
            .get()?
            .record("frame_is_expired", self.is_expired() && !self.is_complete());
        Ok(())
    }
}
