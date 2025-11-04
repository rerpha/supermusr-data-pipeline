mod daq_trace;
mod flatbuffer_decode;
mod kafka_tail;

use clap::{Args, Parser, Subcommand};
use digital_muon_common::CommonKafkaOpts;
use digital_muon_streaming_types::{
    dat2_digitizer_analog_trace_v2_generated::{
        digitizer_analog_trace_message_buffer_has_identifier,
        root_as_digitizer_analog_trace_message,
    },
    ecs_6s4t_run_stop_generated::{root_as_run_stop, run_stop_buffer_has_identifier},
    ecs_pl72_run_start_generated::{root_as_run_start, run_start_buffer_has_identifier},
};
use tracing::{info, warn};

#[derive(Debug, Parser)]
#[clap(author, version = digital_muon_common::version!(), about)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Provides metrics regarding data transmission from the digitisers via Kafka.
    #[clap(name = "daq-trace")]
    DaqTrace(DaqTraceOpts),

    /// Run message dumping tool.
    #[clap(name = "kafka-tail")]
    KafkaTail(CommonOpts),

    /// Decode a flatbuffer encoded message.
    ///
    /// Shows a basic summary of the following message types:
    /// Digitiser trace, Run start, Run stop
    ///
    /// Encoded message must be provided in hexadecimal representation with a space between each
    /// byte (as per the format used by Redpanda Console).
    #[clap(name = "decode")]
    FlatbufferDecode,
}

#[derive(Debug, Args)]
struct CommonOpts {
    #[clap(flatten)]
    common_kafka_options: CommonKafkaOpts,

    /// Kafka consumer group
    #[clap(long = "group")]
    consumer_group: String,

    /// The Kafka topic to consume messages from
    #[clap(long)]
    topic: String,
}

#[derive(Debug, Args)]
struct DaqTraceOpts {
    /// The interval at which the message rate is calculated in seconds.
    #[clap(long, default_value_t = 5)]
    message_rate_interval: u64,

    #[clap(flatten)]
    common: CommonOpts,
}

#[tokio::main]
async fn main() -> miette::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::DaqTrace(args) => daq_trace::run(args).await,
        Commands::KafkaTail(args) => kafka_tail::run(args).await,
        Commands::FlatbufferDecode => flatbuffer_decode::run().await,
    }
}

fn decode_and_print(payload: &[u8]) {
    if digitizer_analog_trace_message_buffer_has_identifier(payload) {
        match root_as_digitizer_analog_trace_message(payload) {
            Ok(data) => {
                info!(
                    "Trace packet: dig. ID: {}, metadata: {:?}",
                    data.digitizer_id(),
                    data.metadata()
                );
            }
            Err(e) => {
                warn!("Failed to parse message: {}", e);
            }
        }
    } else if run_start_buffer_has_identifier(payload) {
        match root_as_run_start(payload) {
            Ok(data) => {
                info!(
                    "Run start: run name: {:?}, start time: {}",
                    data.run_name(),
                    data.start_time()
                );
            }
            Err(e) => {
                warn!("Failed to parse message: {}", e);
            }
        }
    } else if run_stop_buffer_has_identifier(payload) {
        match root_as_run_stop(payload) {
            Ok(data) => {
                info!("Run stop: run name: {:?}", data.run_name());
            }
            Err(e) => {
                warn!("Failed to parse message: {}", e);
            }
        }
    } else {
        // TODO: other message types
        warn!("Unexpected message type");
    }
}
