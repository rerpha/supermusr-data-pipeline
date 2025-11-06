mod app;
mod data;
mod ui;

use self::app::App;
use self::ui::ui;
use super::DaqTraceOpts;
use crossterm::event::{self, DisableMouseCapture, EnableMouseCapture, Event as CEvent, KeyCode};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use data::{DigitiserData, DigitiserDataHashMap};
use digital_muon_common::Intensity;
use digital_muon_streaming_types::dat2_digitizer_analog_trace_v2_generated::{
    DigitizerAnalogTraceMessage, digitizer_analog_trace_message_buffer_has_identifier,
    root_as_digitizer_analog_trace_message,
};
use miette::IntoDiagnostic;
use ratatui::{Terminal, prelude::CrosstermBackend};
use rdkafka::{
    consumer::{CommitMode, Consumer, stream_consumer::StreamConsumer},
    message::Message,
};
use std::collections::HashMap;
use std::{
    io,
    sync::{Arc, Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};
use tokio::task;
use tokio::time::sleep;
use tracing::{debug, info, warn};

enum Event<I> {
    Input(I),
    Tick,
}

// Trace topic diagnostic tool
pub(crate) async fn run(args: DaqTraceOpts) -> miette::Result<()> {
    let kafka_opts = args.common.common_kafka_options;

    let consumer: StreamConsumer = digital_muon_common::generate_kafka_client_config(
        &kafka_opts.broker,
        &kafka_opts.username,
        &kafka_opts.password,
    )
    .set("group.id", &args.common.consumer_group)
    .set("enable.partition.eof", "false")
    .set("session.timeout.ms", "6000")
    .set("enable.auto.commit", "false")
    .create()
    .into_diagnostic()?;

    consumer
        .subscribe(&[&args.common.topic])
        .into_diagnostic()?;

    // Set up terminal.
    enable_raw_mode().into_diagnostic()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture).into_diagnostic()?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).into_diagnostic()?;

    // Set up app and common data.
    let mut app = App::new();

    let common_dig_data_map: DigitiserDataHashMap = Arc::new(Mutex::new(HashMap::new()));

    // Set up event polling.
    let (tx, rx) = mpsc::channel();
    let tick_rate = Duration::from_millis(200);

    // Event polling thread.
    thread::spawn(move || {
        let mut last_tick = Instant::now();
        loop {
            let timeout = tick_rate
                .checked_sub(last_tick.elapsed())
                .unwrap_or_else(|| Duration::from_secs(0));

            if event::poll(timeout).is_ok() {
                if let CEvent::Key(key) =
                    event::read().expect("should be able to read an event after a successful poll")
                {
                    tx.send(Event::Input(key))
                        .expect("should be able to send the key event via channel");
                }
            }

            if last_tick.elapsed() >= tick_rate && tx.send(Event::Tick).is_ok() {
                last_tick = Instant::now();
            }
        }
    });

    // Message polling thread.
    task::spawn(poll_kafka_msg(consumer, Arc::clone(&common_dig_data_map)));

    // Message rate calculation thread.
    task::spawn(update_message_rate(
        Arc::clone(&common_dig_data_map),
        args.message_rate_interval,
    ));

    // Run app.
    loop {
        // Poll events.
        match rx.recv().into_diagnostic()? {
            Event::Input(event) => match event.code {
                KeyCode::Char('q') => break,
                KeyCode::Down => app.next(),
                KeyCode::Up => app.previous(),
                KeyCode::Right => {
                    app.selected_digitiser_channel_delta(Arc::clone(&common_dig_data_map), 1)
                }
                KeyCode::Left => {
                    app.selected_digitiser_channel_delta(Arc::clone(&common_dig_data_map), -1)
                }
                _ => (),
            },
            Event::Tick => (),
        }

        // Use the current data to regenerate the table body (may be inefficient to call every time).
        app.generate_table_body(Arc::clone(&common_dig_data_map));

        // Draw terminal using common data.
        terminal
            .draw(|frame| ui(frame, &mut app))
            .into_diagnostic()?;
    }

    // Clean up terminal.
    disable_raw_mode().into_diagnostic()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )
    .into_diagnostic()?;
    terminal.show_cursor().into_diagnostic()?;
    terminal.clear().into_diagnostic()?;

    Ok(())
}

async fn update_message_rate(
    common_dig_data_map: DigitiserDataHashMap,
    recent_message_lifetime: u64,
) {
    loop {
        // Wait a set period of time before calculating average.
        sleep(Duration::from_secs(recent_message_lifetime)).await;
        let mut logged_data = common_dig_data_map
            .lock()
            .expect("should be able to lock common data");
        // Calculate message rate for each digitiser.
        for digitiser_data in logged_data.values_mut() {
            digitiser_data.msg_rate = (digitiser_data.msg_count - digitiser_data.last_msg_count)
                as f64
                / recent_message_lifetime as f64;
            digitiser_data.last_msg_count = digitiser_data.msg_count;
        }
    }
}

/// Poll kafka messages and update digitiser data.
async fn poll_kafka_msg(consumer: StreamConsumer, mut common_dig_data_map: DigitiserDataHashMap) {
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
                    if digitizer_analog_trace_message_buffer_has_identifier(payload) {
                        match root_as_digitizer_analog_trace_message(payload) {
                            Ok(data) => {
                                process_digitizer_analog_trace_message(
                                    &data,
                                    &mut common_dig_data_map,
                                );
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
                    } else {
                        warn!("Unexpected message type on topic \"{}\"", msg.topic());
                    }
                }

                let _ = consumer.commit_message(&msg, CommitMode::Async);
            }
        };
    }
}

// Main Logic
fn process_digitizer_analog_trace_message(
    data: &DigitizerAnalogTraceMessage<'_>,
    common_dig_data_map: &mut DigitiserDataHashMap,
) {
    let frame_number = data.metadata().frame_number();

    let num_channels_present = data.channels().map(|c| c.len()).unwrap_or_default();

    let num_samples_in_first_channel = data
        .channels()
        .map(|c| c.get(0).voltage().map(|c| c.len()).unwrap_or_default())
        .unwrap_or_default();

    let is_num_samples_identical = data
        .channels()
        .map(|c| {
            c.iter().all(|trace| {
                let num_samples = trace.voltage().map(|c| c.len()).unwrap_or_default();
                num_samples == num_samples_in_first_channel
            })
        })
        .unwrap_or(false);

    let timestamp = data
        .metadata()
        .timestamp()
        .copied()
        .and_then(|t| t.try_into().ok());

    let id = data.digitizer_id();

    common_dig_data_map
        .lock()
        .expect("sound be able to lock common data")
        .entry(id)
        .and_modify(|d| {
            d.update(
                timestamp,
                frame_number,
                num_channels_present,
                num_samples_in_first_channel,
                is_num_samples_identical,
            );
            if let Some(channels) = data.channels() {
                let c = channels.get(d.channel_data.index);
                d.channel_data.id = c.channel();
                if let Some(voltage) = c.voltage() {
                    let max = voltage.iter().max().unwrap_or(Intensity::MIN);
                    let min = voltage.iter().min().unwrap_or(Intensity::MAX);
                    d.channel_data.max = Intensity::max(d.channel_data.max, max);
                    d.channel_data.min = Intensity::min(d.channel_data.min, min);
                }
            }
        })
        .or_insert(DigitiserData::new(
            timestamp,
            frame_number,
            num_channels_present,
            num_samples_in_first_channel,
            is_num_samples_identical,
        ));
}
