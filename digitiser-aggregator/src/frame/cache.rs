//! Defines the cache stores frames as they are assembled from digitiser messages.
use super::{AggregatedFrame, RejectMessageError, partial::PartialFrame};
use crate::data::{Accumulate, DigitiserData};
use chrono::{DateTime, Utc};
use digital_muon_common::{
    DigitizerId, record_metadata_fields_to_span, spanned::SpannedAggregator,
};
use digital_muon_streaming_types::FrameMetadata;
use std::{collections::VecDeque, fmt::Debug, time::Duration};
use tracing::{info_span, warn};

/// Contains all the partial frames as well as handling the frame lifetime and completeness.
pub(crate) struct FrameCache<D: Debug> {
    /// Specifies the maximum time that a partial frame should live
    /// in the cache before being dispatched event if it is missing some digitisers.
    ttl: Duration,
    /// Specifies the complete set of digitisers
    /// a partial frame should have before being complete.
    expected_digitisers: Vec<DigitizerId>,
    /// The metadata timestamp of the last frame to be dispatched,
    /// value is [None] if no frame has been dispatched yet.
    latest_timestamp_dispatched: Option<DateTime<Utc>>,
    /// The partial frames currently in the cache.
    frames: VecDeque<PartialFrame<D>>,
}

impl<D: Debug> FrameCache<D>
where
    DigitiserData<D>: Accumulate<D>,
{
    /// Creates and returns a new [FrameCache] instance.
    /// # Parameters
    /// - ttl: time-to-live duration
    /// - expected_digitisers: list of digitisers that form a complete frame.
    ///
    /// Note that `expected_digitisers` should be increasing and without duplicates, this is not checked.
    pub(crate) fn new(ttl: Duration, expected_digitisers: Vec<DigitizerId>) -> Self {
        Self {
            ttl,
            expected_digitisers,
            latest_timestamp_dispatched: None,
            frames: Default::default(),
        }
    }

    /// Pushes the contents of a new digitiser message into the cache.
    /// If a partial frame with the same `metadata` already exists, and is yet
    /// to receive a message with the same `digitiser_id`, then `data` is added
    /// to the partial frame, otherwise a new [PartialFrame] is created.
    #[tracing::instrument(skip_all, level = "trace")]
    pub(crate) fn push<'a>(
        &'a mut self,
        digitiser_id: DigitizerId,
        metadata: &FrameMetadata,
        data: D,
    ) -> Result<(), RejectMessageError> {
        if let Some(latest_timestamp_dispatched) = self.latest_timestamp_dispatched {
            if metadata.timestamp <= latest_timestamp_dispatched {
                warn!(
                    "Frame's timestamp earlier than or equal to the latest frame dispatched: {0} <= {1}",
                    metadata.timestamp, latest_timestamp_dispatched
                );
                return Err(RejectMessageError::TimestampTooEarly);
            }
        }
        let frame = {
            match self
                .frames
                .iter_mut()
                .find(|frame| frame.metadata.equals_ignoring_veto_flags(metadata))
            {
                Some(frame) => {
                    if frame.has_digitiser_id(digitiser_id) {
                        warn!("Frame already has digitiser id: {digitiser_id}");
                        return Err(RejectMessageError::IdAlreadyPresent);
                    }
                    frame.push(digitiser_id, data);
                    frame.push_veto_flags(metadata.veto_flags);
                    frame.set_completion_status(&self.expected_digitisers);
                    frame
                }
                None => {
                    let mut frame = PartialFrame::<D>::new(self.ttl, metadata.clone());

                    // Initialise the span field
                    if let Err(e) = frame.span_init() {
                        warn!("Frame span initiation failed {e}")
                    }

                    frame.push(digitiser_id, data);
                    self.frames.push_back(frame);
                    self.frames
                        .back()
                        .expect("self.frames should be non-empty, this should never fails")
                }
            }
        };

        // Link this span with the frame aggregator span associated with `frame`
        if let Err(e) = frame.link_current_span(|| {
            let span = info_span!(
                "Digitiser Event List",
                digitiser_id = digitiser_id,
                "metadata_timestamp" = tracing::field::Empty,
                "metadata_frame_number" = tracing::field::Empty,
                "metadata_period_number" = tracing::field::Empty,
                "metadata_veto_flags" = tracing::field::Empty,
                "metadata_protons_per_pulse" = tracing::field::Empty,
                "metadata_running" = tracing::field::Empty,
            );
            record_metadata_fields_to_span!(metadata, span);
            span
        }) {
            warn!("Frame span linking failed {e}")
        }

        Ok(())
    }

    /// Checks whether any partial frame is ready to be dispatched, that is either
    /// has a complete complement of digitisers, or has been in the cache past its expiry time.
    /// If one is found it is removed from the cache and returned as an [AggregatedFrame].
    pub(crate) fn poll(&mut self) -> Option<AggregatedFrame<D>> {
        // Find a frame which is completed
        if self
            .frames
            .front()
            .is_some_and(|frame| frame.is_complete() || frame.is_expired())
        {
            let frame = self
                .frames
                .pop_front()
                .expect("self.frames should be non-empty, this should never fail");
            if let Err(e) = frame.end_span() {
                warn!("Frame span drop failed {e}")
            }

            // This frame is the next to be set to latest timestamp dispatched
            self.latest_timestamp_dispatched = Some(frame.metadata.timestamp);
            Some(frame.into())
        } else {
            None
        }
    }

    /// Returns the number of partial frames currently in the cache.
    pub(crate) fn get_num_partial_frames(&self) -> usize {
        self.frames.len()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::data::EventData;
    use chrono::Utc;

    #[test]
    fn one_frame_in_one_frame_out() {
        let mut cache = FrameCache::<EventData>::new(Duration::from_millis(100), vec![0, 1, 4, 8]);

        let frame_1 = FrameMetadata {
            timestamp: Utc::now(),
            period_number: 1,
            protons_per_pulse: 8,
            running: true,
            frame_number: 1728,
            veto_flags: 4,
        };

        assert!(cache.poll().is_none());

        assert_eq!(cache.get_num_partial_frames(), 0);
        assert!(
            cache
                .push(0, &frame_1, EventData::dummy_data(0, 5, &[0, 1, 2]))
                .is_ok()
        );
        assert_eq!(cache.get_num_partial_frames(), 1);

        assert!(cache.poll().is_none());

        assert!(
            cache
                .push(1, &frame_1, EventData::dummy_data(0, 5, &[3, 4, 5]))
                .is_ok()
        );

        assert!(cache.poll().is_none());

        assert!(
            cache
                .push(4, &frame_1, EventData::dummy_data(0, 5, &[6, 7, 8]))
                .is_ok()
        );

        assert!(cache.poll().is_none());

        assert!(
            cache
                .push(8, &frame_1, EventData::dummy_data(0, 5, &[9, 10, 11]))
                .is_ok()
        );

        {
            let frame = cache.poll().unwrap();
            assert_eq!(cache.get_num_partial_frames(), 0);

            assert_eq!(frame.metadata, frame_1);

            let mut dids = frame.digitiser_ids;
            dids.sort();
            assert_eq!(dids, &[0, 1, 4, 8]);

            assert_eq!(
                frame.digitiser_data,
                EventData::new(
                    vec![
                        0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4,
                        0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4,
                        0, 1, 2, 3, 4, 0, 1, 2, 3, 4
                    ],
                    vec![0; 60],
                    vec![
                        0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4,
                        5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 9, 9, 9, 9, 9,
                        10, 10, 10, 10, 10, 11, 11, 11, 11, 11
                    ],
                )
            );
        }

        assert!(cache.poll().is_none());
    }

    #[tokio::test]
    async fn one_frame_in_one_frame_out_missing_digitiser_timeout() {
        let mut cache = FrameCache::<EventData>::new(Duration::from_millis(100), vec![0, 1, 4, 8]);

        let frame_1 = FrameMetadata {
            timestamp: Utc::now(),
            period_number: 1,
            protons_per_pulse: 8,
            running: true,
            frame_number: 1728,
            veto_flags: 4,
        };

        assert!(cache.poll().is_none());

        assert!(
            cache
                .push(0, &frame_1, EventData::dummy_data(0, 5, &[0, 1, 2]))
                .is_ok()
        );

        assert!(cache.poll().is_none());

        assert!(
            cache
                .push(1, &frame_1, EventData::dummy_data(0, 5, &[3, 4, 5]))
                .is_ok()
        );

        assert!(cache.poll().is_none());

        assert!(
            cache
                .push(8, &frame_1, EventData::dummy_data(0, 5, &[9, 10, 11]))
                .is_ok()
        );

        assert!(cache.poll().is_none());

        tokio::time::sleep(Duration::from_millis(105)).await;

        {
            let frame = cache.poll().unwrap();

            assert_eq!(frame.metadata, frame_1);

            let mut dids = frame.digitiser_ids;
            dids.sort();
            assert_eq!(dids, &[0, 1, 8]);

            assert_eq!(
                frame.digitiser_data,
                EventData::new(
                    vec![
                        0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4,
                        0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4,
                    ],
                    vec![0; 45],
                    vec![
                        0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4,
                        5, 5, 5, 5, 5, 9, 9, 9, 9, 9, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11
                    ],
                )
            );
        }

        assert!(cache.poll().is_none());
    }

    #[tokio::test]
    async fn one_frame_in_one_frame_out_missing_digitiser_and_late_message_timeout() {
        let mut cache = FrameCache::<EventData>::new(Duration::from_millis(100), vec![0, 1, 4, 8]);

        let frame_1 = FrameMetadata {
            timestamp: Utc::now(),
            period_number: 1,
            protons_per_pulse: 8,
            running: true,
            frame_number: 1728,
            veto_flags: 4,
        };
        assert!(
            cache
                .push(0, &frame_1, EventData::dummy_data(0, 5, &[0, 1, 2]))
                .is_ok()
        );
        assert!(
            cache
                .push(1, &frame_1, EventData::dummy_data(0, 5, &[3, 4, 5]))
                .is_ok()
        );
        assert!(
            cache
                .push(8, &frame_1, EventData::dummy_data(0, 5, &[9, 10, 11]))
                .is_ok()
        );

        tokio::time::sleep(Duration::from_millis(105)).await;

        let _ = cache.poll().unwrap();

        //  This call to push should return an error
        assert!(
            cache
                .push(4, &frame_1, EventData::dummy_data(0, 5, &[6, 7, 8]))
                .is_err()
        );
    }

    #[test]
    fn test_metadata_equality() {
        let mut cache = FrameCache::<EventData>::new(Duration::from_millis(100), vec![1, 2]);

        let timestamp = Utc::now();
        let frame_1 = FrameMetadata {
            timestamp,
            period_number: 1,
            protons_per_pulse: 8,
            running: true,
            frame_number: 1728,
            veto_flags: 4,
        };

        let frame_2 = FrameMetadata {
            timestamp,
            period_number: 1,
            protons_per_pulse: 8,
            running: true,
            frame_number: 1728,
            veto_flags: 5,
        };

        assert_eq!(frame_1, frame_2);

        assert_eq!(cache.frames.len(), 0);
        assert!(cache.poll().is_none());

        assert!(
            cache
                .push(1, &frame_1, EventData::dummy_data(0, 5, &[0, 1, 2]))
                .is_ok()
        );
        assert_eq!(cache.frames.len(), 1);
        assert!(cache.poll().is_none());

        assert!(
            cache
                .push(2, &frame_2, EventData::dummy_data(0, 5, &[0, 1, 2]))
                .is_ok()
        );
        assert_eq!(cache.frames.len(), 1);
        assert!(cache.poll().is_some());
    }
}
