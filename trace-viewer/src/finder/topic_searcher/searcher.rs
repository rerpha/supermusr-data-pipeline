use crate::{
    Timestamp,
    finder::topic_searcher::{
        BackstepIter, BinarySearchIter, ForwardSearchIter, iterators::DragNetIter,
    },
    structs::{BorrowedMessageError, FBMessage},
};
use digital_muon_streaming_types::time_conversions::GpsTimeConversionError;
use rdkafka::{
    Offset, TopicPartitionList,
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
    message::BorrowedMessage,
    util::Timeout,
};
use std::time::Duration;
use thiserror::Error;
use tokio::time::timeout;
use tracing::{instrument, warn};

#[derive(Error, Debug)]
pub(crate) enum SearcherError {
    #[error("Topic start reached")]
    StartOfTopicReached,
    #[error("Topic end reached")]
    EndOfTopicReached,
    #[error("Broker Timed Out")]
    BrokerTimeout,
    #[error("No valid message found")]
    NoMessageFound(#[from] BorrowedMessageError),
    #[error("Timestamp Conversion Error: {0}")]
    TimestampConversion(#[from] GpsTimeConversionError),
    #[error("Kafka Error: {0}")]
    Kafka(#[from] KafkaError),
}

/// Object to search through the broker from a given offset, on a given topic, for messages of type `M`.
pub(crate) struct Searcher<'a, M, C> {
    /// Reference to the Kafka consumer.
    pub(super) consumer: &'a C,
    /// Topic to search on.
    pub(super) topic: String,
    /// Current offset.
    pub(super) offset: i64,
    /// Results accumulate here.
    pub(super) results: Vec<M>,
}

impl<'a, M> Searcher<'a, M, StreamConsumer> {
    /// Creates a new instance, and assigns the given topic to the broker's consumer.
    ///
    /// # Parameters
    /// - consumer: the broker's consumer to use.
    /// - topic: the topic to search on.
    /// - offset: the offset to search from.
    /// - send_status: send channel, along which status messages should be sent.
    #[instrument(skip_all)]
    pub(crate) fn new(
        consumer: &'a StreamConsumer,
        topic: &str,
        offset: i64,
    ) -> Result<Self, SearcherError> {
        consumer.unassign()?;
        let mut tpl = TopicPartitionList::with_capacity(1);
        tpl.add_partition_offset(topic, 0, rdkafka::Offset::End)?;
        consumer.assign(&tpl)?;
        Ok(Self {
            consumer,
            offset,
            topic: topic.to_owned(),
            results: Default::default(),
        })
    }

    #[instrument(skip_all)]
    /// Consumer the searcher and create a backstep iterator.
    pub(crate) fn iter_backstep(self) -> BackstepIter<'a, M, StreamConsumer> {
        BackstepIter {
            inner: self,
            step_size: None,
        }
    }

    #[instrument(skip_all)]
    /// Consumer the searcher and create a forward iterator.
    pub(crate) fn iter_forward(self) -> ForwardSearchIter<'a, M, StreamConsumer> {
        ForwardSearchIter {
            inner: self,
            message: None,
        }
    }

    #[instrument(skip_all)]
    /// Consumer the searcher and create a forward iterator.
    pub(crate) fn iter_binary(self, target: Timestamp) -> BinarySearchIter<'a, M, StreamConsumer> {
        BinarySearchIter {
            inner: self,
            bound: Default::default(),
            max_bound: Default::default(),
            target,
        }
    }

    #[instrument(skip_all)]
    /// Consumer the searcher and create a forward iterator.
    pub(crate) fn iter_dragnet(self, number: usize) -> DragNetIter<'a, M, StreamConsumer> {
        DragNetIter {
            inner: self,
            timestamps: Vec::with_capacity(number),
        }
    }

    /// Sets the offset.
    pub(super) fn set_offset(&mut self, offset: i64) {
        self.offset = offset;
    }

    /// Gets the offset.
    pub(crate) fn get_offset(&self) -> i64 {
        self.offset
    }

    #[instrument(skip_all)]
    pub(crate) async fn recv(&self) -> Option<BorrowedMessage<'a>> {
        const FORWARD_ITER_TIMEOUT: Duration = Duration::from_secs(2);

        timeout(FORWARD_ITER_TIMEOUT, self.consumer.recv())
            .await
            .inspect_err(|_| warn!("Recv Timed Out."))
            .ok()
            .map(Result::ok)
            .flatten()
    }

    pub(crate) fn get_current_bounds(&self) -> (i64, i64) {
        const FETCH_WATERMARKS_TIMEOUT: Timeout = Timeout::After(Duration::from_secs(2));

        self.consumer
            .fetch_watermarks(&self.topic, 0, FETCH_WATERMARKS_TIMEOUT)
            .expect("Should get watermarks, this should not fail.")
    }
}

/// Extracts the results from the searcher, when the user is finished with it.
impl<'a, M, C> From<Searcher<'a, M, C>> for Vec<M> {
    #[instrument(skip_all)]
    fn from(value: Searcher<'a, M, C>) -> Vec<M> {
        value.results
    }
}

impl<'a, M> Searcher<'a, M, StreamConsumer>
where
    M: FBMessage<'a>,
{
    #[instrument(skip_all, level = "trace", fields(offset=offset, timestamp))]
    pub(crate) async fn message(&mut self, offset: i64) -> Result<M, SearcherError> {
        const SEEK_TIMEOUT: Duration = Duration::from_millis(1);
        const MESSAGE_TIMEOUT: Duration = Duration::from_millis(5000);

        self.consumer
            .seek(&self.topic, 0, Offset::Offset(offset), SEEK_TIMEOUT)?;

        let msg = M::try_from(
            timeout(MESSAGE_TIMEOUT, self.consumer.recv())
                .await
                .map_err(|_| SearcherError::BrokerTimeout)??,
        )?;

        tracing::Span::current().record("timestamp", msg.timestamp().to_rfc3339());
        Ok(msg)
    }
}
