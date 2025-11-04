use super::CommonOpts;
use miette::IntoDiagnostic;
use rdkafka::{
    Message,
    consumer::{CommitMode, Consumer, StreamConsumer},
};
use tracing::{debug, error, warn};

// Message dumping tool
pub(crate) async fn run(args: CommonOpts) -> miette::Result<()> {
    tracing_subscriber::fmt::init();

    let kafka_opts = args.common_kafka_options;

    let consumer: StreamConsumer = digital_muon_common::generate_kafka_client_config(
        &kafka_opts.broker,
        &kafka_opts.username,
        &kafka_opts.password,
    )
    .set("group.id", &args.consumer_group)
    .set("enable.partition.eof", "false")
    .set("session.timeout.ms", "6000")
    .set("enable.auto.commit", "false")
    .create()
    .into_diagnostic()?;

    consumer.subscribe(&[&args.topic]).into_diagnostic()?;

    loop {
        match consumer.recv().await {
            Err(e) => warn!("Kafka error: {}", e),
            Ok(msg) => {
                debug!(
                    "key: '{:?}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                    msg.key(),
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                    msg.timestamp()
                );

                if let Some(payload) = msg.payload() {
                    super::decode_and_print(payload);
                }

                if let Err(e) = consumer.commit_message(&msg, CommitMode::Async) {
                    error!("Failed to commit message: {e}");
                }
            }
        };
    }
}
