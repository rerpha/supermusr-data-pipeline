use crate::{
    parameters::{
        AdvancedMuonDetectorParameters, DetectorSettings,
        DifferentialThresholdDiscriminatorParameters, FixedThresholdDiscriminatorParameters, Mode,
        Polarity,
    },
    pulse_detection::{
        AssembleFilter, EventFilter, Real,
        advanced_muon_detector::{AdvancedMuonAssembler, AdvancedMuonDetector},
        detectors::differential_threshold_detector::DifferentialThresholdDetector,
        threshold_detector::{ThresholdDetector, ThresholdDuration},
        window::{Baseline, FiniteDifferences, SmoothingWindow, WindowFilter},
    },
};
use digital_muon_common::{Intensity, Time};
use digital_muon_streaming_types::dat2_digitizer_analog_trace_v2_generated::ChannelTrace;

#[tracing::instrument(skip_all, fields(channel = trace.channel(), num_pulses))]
pub(crate) fn find_channel_events(
    trace: &ChannelTrace,
    sample_time: Real,
    detector_settings: &DetectorSettings,
) -> (Vec<Time>, Vec<Intensity>) {
    let result = match &detector_settings.mode {
        Mode::FixedThresholdDiscriminator(parameters) => find_fixed_threshold_events(
            trace,
            sample_time,
            detector_settings.polarity,
            detector_settings.baseline as Real,
            parameters,
        ),
        Mode::DifferentialThresholdDiscriminator(parameters) => find_differential_threshold_events(
            trace,
            sample_time,
            detector_settings.polarity,
            detector_settings.baseline as Real,
            parameters,
        ),
        Mode::AdvancedMuonDetector(parameters) => find_advanced_events(
            trace,
            sample_time,
            detector_settings.polarity,
            detector_settings.baseline as Real,
            parameters,
        ),
    };
    tracing::Span::current().record("num_pulses", result.0.len());
    result
}

#[tracing::instrument(skip_all, level = "trace")]
fn find_fixed_threshold_events(
    trace: &ChannelTrace,
    sample_time: Real,
    polarity: &Polarity,
    baseline: Real,
    parameters: &FixedThresholdDiscriminatorParameters,
) -> (Vec<Time>, Vec<Intensity>) {
    let sign = match polarity {
        Polarity::Positive => 1.0,
        Polarity::Negative => -1.0,
    };
    let raw = trace
        .voltage()
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(i, v)| (i as Real * sample_time, sign * (v as Real - baseline)));

    let pulses = raw
        .clone()
        .events(ThresholdDetector::new(&ThresholdDuration {
            threshold: parameters.threshold,
            duration: parameters.duration,
            cool_off: parameters.cool_off,
        }));

    let mut time = Vec::<Time>::new();
    let mut voltage = Vec::<Intensity>::new();
    for pulse in pulses {
        time.push(pulse.0 as Time);
        voltage.push(pulse.1.pulse_height as Intensity);
    }
    (time, voltage)
}

#[tracing::instrument(skip_all, level = "trace")]
fn find_differential_threshold_events(
    trace: &ChannelTrace,
    sample_time: Real,
    polarity: &Polarity,
    baseline: Real,
    parameters: &DifferentialThresholdDiscriminatorParameters,
) -> (Vec<Time>, Vec<Intensity>) {
    let sign = match polarity {
        Polarity::Positive => 1.0,
        Polarity::Negative => -1.0,
    };
    let raw = trace
        .voltage()
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(i, v)| (i as Real * sample_time, sign * (v as Real - baseline)));

    let pulses = raw.clone().window(FiniteDifferences::<2>::new()).events(
        DifferentialThresholdDetector::new(
            &ThresholdDuration {
                threshold: parameters.threshold,
                duration: parameters.duration,
                cool_off: parameters.cool_off,
            },
            parameters.constant_multiple,
        ),
    );

    let mut time = Vec::<Time>::new();
    let mut voltage = Vec::<Intensity>::new();
    for pulse in pulses {
        time.push(pulse.0 as Time);
        voltage.push(pulse.1.pulse_height as Intensity);
    }
    (time, voltage)
}

#[tracing::instrument(skip_all, level = "trace")]
fn find_advanced_events(
    trace: &ChannelTrace,
    sample_time: Real,
    polarity: &Polarity,
    baseline: Real,
    parameters: &AdvancedMuonDetectorParameters,
) -> (Vec<Time>, Vec<Intensity>) {
    let sign = match polarity {
        Polarity::Positive => 1.0,
        Polarity::Negative => -1.0,
    };
    let raw = trace
        .voltage()
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(i, v)| (i as Real * sample_time, sign * (v as Real - baseline)));

    let smoothed = raw
        .clone()
        .window(Baseline::new(parameters.baseline_length.unwrap_or(0), 0.1))
        .window(SmoothingWindow::new(
            parameters.smoothing_window_size.unwrap_or(1),
        ))
        .map(|(i, stats)| (i, stats.mean));

    let events = smoothed
        .clone()
        .window(FiniteDifferences::<2>::new())
        .events(AdvancedMuonDetector::new(
            parameters.muon_onset,
            parameters.muon_fall,
            parameters.muon_termination,
            parameters.duration,
        ));

    let pulses = events
        .clone()
        .assemble(AdvancedMuonAssembler::default())
        .filter(|pulse| {
            Option::zip(parameters.min_amplitude, pulse.peak.value)
                .map(|(min, val)| min <= val)
                .unwrap_or(true)
        })
        .filter(|pulse| {
            Option::zip(parameters.max_amplitude, pulse.peak.value)
                .map(|(max, val)| max >= val)
                .unwrap_or(true)
        });

    let mut time = Vec::<Time>::new();
    let mut voltage = Vec::<Intensity>::new();
    for pulse in pulses {
        time.push(pulse.steepest_rise.time.unwrap_or_default() as Time);
        voltage.push(pulse.peak.value.unwrap_or_default() as Intensity);
    }
    (time, voltage)
}
