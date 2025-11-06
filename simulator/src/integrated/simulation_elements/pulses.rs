use super::{FloatRandomDistribution, utils::JsonFloatError};
use digital_muon_common::{Intensity, Time};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "kebab-case", tag = "pulse-type")]
pub(crate) enum PulseTemplate {
    Flat {
        start: FloatRandomDistribution,
        width: FloatRandomDistribution,
        height: FloatRandomDistribution,
    },
    Triangular {
        start: FloatRandomDistribution,
        peak_time: FloatRandomDistribution,
        width: FloatRandomDistribution,
        height: FloatRandomDistribution,
    },
    Gaussian {
        height: FloatRandomDistribution,
        peak_time: FloatRandomDistribution,
        sd: FloatRandomDistribution,
    },
    Biexp {
        start: FloatRandomDistribution,
        decay: FloatRandomDistribution,
        rise: FloatRandomDistribution,
        height: FloatRandomDistribution,
    },
}

#[derive(Debug)]
pub(crate) enum PulseEvent {
    Flat {
        start: f64,
        stop: f64,
        amplitude: f64,
    },
    Triangular {
        start: f64,
        peak_time: f64,
        stop: f64,
        amplitude: f64,
    },
    Gaussian {
        start: f64,
        stop: f64,
        mean: f64,
        sd: f64,
        peak_amplitude: f64,
    },
    Biexp {
        start: f64,
        stop: f64,
        decay: f64,
        rise: f64,
        peak_height: f64,
        coef: f64,
        peak_time: f64,
    },
}

impl PulseEvent {
    pub(crate) fn sample(template: &PulseTemplate, frame: usize) -> Result<Self, JsonFloatError> {
        match template {
            PulseTemplate::Flat {
                start,
                width,
                height,
            } => {
                let start = start.sample(frame)?;
                Ok(Self::Flat {
                    start,
                    stop: start + width.sample(frame)?,
                    amplitude: height.sample(frame)?,
                })
            }
            PulseTemplate::Triangular {
                start,
                peak_time,
                width,
                height,
            } => {
                let start = start.sample(frame)?;
                let width = width.sample(frame)?;
                Ok(Self::Triangular {
                    start,
                    peak_time: start + peak_time.sample(frame)? * width,
                    stop: start + width,
                    amplitude: height.sample(frame)?,
                })
            }
            PulseTemplate::Gaussian {
                height,
                peak_time,
                sd,
            } => {
                let mean = peak_time.sample(frame)?;
                let sd = sd.sample(frame)?;
                Ok(Self::Gaussian {
                    start: mean - 4.0 * sd,
                    stop: mean + 4.0 * sd,
                    mean,
                    sd,
                    peak_amplitude: height.sample(frame)?,
                })
            }
            PulseTemplate::Biexp {
                start,
                decay,
                rise,
                height,
            } => {
                let start = start.sample(frame)?;
                let decay = decay.sample(frame)?;
                let rise = rise.sample(frame)?;
                let peak_height = height.sample(frame)?;
                let ratio = decay / rise;
                let coef = peak_height
                    / (f64::powf(ratio, 1.0 / ratio - 1.0) - f64::powf(ratio, 1.0 - ratio));
                let peak_time = f64::ln(f64::powf(ratio, decay * rise / (decay - rise)));
                let stop = f64::MAX; // This needs to be changed
                Ok(Self::Biexp {
                    start,
                    stop,
                    decay,
                    rise,
                    peak_height,
                    coef,
                    peak_time,
                })
            }
        }
    }

    pub(crate) fn get_start(&self) -> Time {
        (match self {
            Self::Flat { start, .. } => *start,
            Self::Triangular { start, .. } => *start,
            Self::Gaussian { start, .. } => *start,
            Self::Biexp { start, .. } => *start,
        }) as Time
    }

    pub(crate) fn get_end(&self) -> Time {
        (match self {
            Self::Flat { stop, .. } => *stop,
            Self::Triangular { stop, .. } => *stop,
            Self::Gaussian { stop, .. } => *stop,
            Self::Biexp { stop, .. } => *stop,
        }) as Time
    }

    pub(crate) fn time(&self) -> Time {
        (match self {
            Self::Flat { start, .. } => *start,
            Self::Triangular { peak_time, .. } => *peak_time,
            Self::Gaussian { mean, .. } => *mean,
            Self::Biexp {
                start, peak_time, ..
            } => *start + *peak_time / 2.0,
        }) as Time
    }

    pub(crate) fn intensity(&self) -> Intensity {
        *match self {
            Self::Flat { amplitude, .. } => amplitude,
            Self::Triangular { amplitude, .. } => amplitude,
            Self::Gaussian { peak_amplitude, .. } => peak_amplitude,
            Self::Biexp { peak_height, .. } => peak_height,
        } as Intensity
    }

    pub(crate) fn get_value_at(&self, time: f64) -> f64 {
        match *self {
            Self::Flat {
                start,
                stop,
                amplitude,
            } => {
                if start <= time && time < stop {
                    amplitude
                } else {
                    f64::default()
                }
            }
            Self::Triangular {
                start,
                peak_time,
                stop,
                amplitude,
            } => {
                if start <= time && time < peak_time {
                    amplitude * (time - start) / (peak_time - start)
                } else if peak_time <= time && time < stop {
                    amplitude * (stop - time) / (stop - peak_time)
                } else {
                    f64::default()
                }
            }
            Self::Gaussian {
                mean,
                sd,
                peak_amplitude,
                ..
            } => {
                if mean - 6.0 * sd > time || time > mean + 6.0 * sd {
                    f64::default()
                } else {
                    peak_amplitude * f64::exp(-f64::powi(0.5 * (time - mean) / sd, 2))
                }
            }
            Self::Biexp {
                start,
                decay,
                rise,
                coef,
                ..
            } => {
                if time < start {
                    f64::default()
                } else {
                    let time = time - start;
                    coef * (f64::exp(-time / decay) - f64::exp(-time / rise))
                }
            }
        }
    }
}
