use crate::pulse_detection::Real;
use clap::{Parser, Subcommand, ValueEnum};
use digital_muon_common::Intensity;

#[derive(Debug)]
pub(crate) struct DetectorSettings<'a> {
    pub(crate) mode: &'a Mode,
    pub(crate) polarity: &'a Polarity,
    pub(crate) baseline: Intensity,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum Polarity {
    Positive,
    Negative,
}

#[derive(Default, Debug, Clone, Parser)]
pub(crate) struct FixedThresholdDiscriminatorParameters {
    /// If the detector is armed, an event is registered when the trace passes this value for the given duration.
    #[clap(long)]
    pub(crate) threshold: Real,

    /// The duration, in samples, that the trace must exceed the threshold for.
    #[clap(long, default_value = "1")]
    pub(crate) duration: i32,

    /// After an event is registered, the detector disarms for this many samples.
    #[clap(long, default_value = "0")]
    pub(crate) cool_off: i32,
}

#[derive(Default, Debug, Clone, Parser)]
pub(crate) struct DifferentialThresholdDiscriminatorParameters {
    /// If the detector is armed, an event is registered when the trace passes this value for the given duration.
    #[clap(long)]
    pub(crate) threshold: Real,

    /// The duration, in samples, that the trace must exceed the threshold for.
    #[clap(long, default_value = "1")]
    pub(crate) duration: i32,

    /// After an event is registered, the detector disarms for this many samples.
    #[clap(long, default_value = "0")]
    pub(crate) cool_off: i32,

    /// If set, the pulse height is the value of the rising edge, scaled by this factor,
    /// otherwise the maximum trace value is used for the pulse height.
    pub(crate) constant_multiple: Option<Real>,
}

#[derive(Default, Debug, Clone, Parser)]
pub(crate) struct AdvancedMuonDetectorParameters {
    /// Differential threshold for detecting muon onset. See README.md.
    #[clap(long)]
    pub(crate) muon_onset: Real,

    /// Differential threshold for detecting muon peak. See README.md.
    #[clap(long)]
    pub(crate) muon_fall: Real,

    /// Differential threshold for detecting muon termination. See README.md.
    #[clap(long)]
    pub(crate) muon_termination: Real,

    /// Length of time a threshold must be passed to register. See README.md.
    #[clap(long)]
    pub(crate) duration: Real,

    /// Size of initial portion of the trace to use for determining the baseline. Initial portion should be event free.
    #[clap(long)]
    pub(crate) baseline_length: Option<usize>,

    /// Size of the moving average window to use for the lopass filter.
    #[clap(long)]
    pub(crate) smoothing_window_size: Option<usize>,

    /// Optional parameter which (if set) filters out events whose peak is greater than the given value.
    #[clap(long)]
    pub(crate) max_amplitude: Option<Real>,

    /// Optional parameter which (if set) filters out events whose peak is less than the given value.
    #[clap(long)]
    pub(crate) min_amplitude: Option<Real>,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Mode {
    /// Detects events using a fixed threshold discriminator. Event lists consist of time and voltage values.
    FixedThresholdDiscriminator(FixedThresholdDiscriminatorParameters),
    /// Detects events using a differential threshold discriminator. Event lists consist of time and voltage values.
    DifferentialThresholdDiscriminator(DifferentialThresholdDiscriminatorParameters),
    /// Detects events using differential discriminators. Event lists consist of time and voltage values.
    AdvancedMuonDetector(AdvancedMuonDetectorParameters),
}
