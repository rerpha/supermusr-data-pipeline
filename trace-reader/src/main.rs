mod loader;
mod processing;

use crate::{loader::load_trace_file, processing::dispatch_trace_file};
use clap::Parser;
use digital_muon_common::{CommonKafkaOpts, DigitizerId, FrameNumber};
use rand::seq::IteratorRandom;
use rdkafka::producer::FutureProducer;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[clap(author, version = digital_muon_common::version!(), about)]
struct Cli {
    #[clap(flatten)]
    common_kafka_options: CommonKafkaOpts,

    /// Kafka consumer group
    #[clap(long)]
    consumer_group: String,

    /// The Kafka topic that trace messages will be produced to
    #[clap(long)]
    trace_topic: String,

    /// Relative path to the .trace file to be read
    #[clap(long)]
    file_name: PathBuf,

    /// The frame number to assign the message
    #[clap(long, default_value = "0")]
    frame_number: FrameNumber,

    /// The digitizer ID to assign the message
    #[clap(long, default_value = "0")]
    digitizer_id: DigitizerId,

    /// The number of trace events to read. If zero, then all trace events are read
    #[clap(long, default_value = "1")]
    number_of_trace_events: usize,

    /// If set, then trace events are sampled randomly with replacement, if not set then trace events are read in order
    #[clap(long, default_value = "false")]
    random_sample: bool,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let args = Cli::parse();

    let kafka_opts = args.common_kafka_options;

    let client_config = digital_muon_common::generate_kafka_client_config(
        &kafka_opts.broker,
        &kafka_opts.username,
        &kafka_opts.password,
    );

    let producer: FutureProducer = client_config
        .create()
        .expect("Kafka Producer should be created");

    let trace_file = load_trace_file(args.file_name).expect("Trace File should load");
    let total_trace_events = trace_file.get_number_of_trace_events();
    let num_trace_events = if args.number_of_trace_events == 0 {
        total_trace_events
    } else {
        args.number_of_trace_events
    };

    let trace_event_indices: Vec<_> = if args.random_sample {
        (0..num_trace_events)
            .map(|_| {
                (0..num_trace_events)
                    .choose(&mut rand::rng())
                    .unwrap_or_default()
            })
            .collect()
    } else {
        (0..num_trace_events)
            .cycle()
            .take(num_trace_events)
            .collect()
    };

    dispatch_trace_file(
        trace_file,
        trace_event_indices,
        args.frame_number,
        args.digitizer_id,
        &producer,
        &args.trace_topic,
        6000,
    )
    .await
    .expect("Trace File should be dispatched to Kafka");
}
