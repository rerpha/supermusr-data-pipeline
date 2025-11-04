use super::utils::JsonFloatError;
use crate::integrated::{
    active_pulses::ActivePulses,
    simulation::{Simulation, SimulationError},
    simulation_elements::{
        IntRandomDistribution,
        noise::{Noise, NoiseSource},
        pulses::PulseEvent,
    },
};
use digital_muon_common::{
    FrameNumber, Intensity,
    spanned::{SpanOnce, Spanned},
};
use rand::distr::weighted::WeightedIndex;
use serde::Deserialize;
use tracing::instrument;

pub(crate) struct Trace {
    span: SpanOnce,
    intensities: Vec<Intensity>,
}

impl Trace {
    #[instrument(
        skip_all,
        level = "debug",
        follows_from = [event_list
            .span()
            .get()
            .expect("Span should be initialised, this never fails")
        ],
        name = "New Trace",
        err(level = "error")
    )]
    pub(crate) fn new(
        simulation: &Simulation,
        frame_number: FrameNumber,
        event_list: &EventList<'_>,
    ) -> Result<Self, JsonFloatError> {
        let mut noise = event_list.noises.iter().map(Noise::new).collect::<Vec<_>>();
        let mut active_pulses = ActivePulses::new(&event_list.pulses);
        let sample_time = 1_000_000_000.0 / simulation.sample_rate as f64;
        Ok(Self {
            span: SpanOnce::Spanned(tracing::Span::current()),
            intensities: (0..simulation.time_bins)
                .map(|time| {
                    //  Remove any expired muons
                    active_pulses.drop_spent_muons(time);
                    //  Append any new muons
                    active_pulses.push_new_muons(time);

                    //  Sum the signal of the currenty active muons
                    let signal = active_pulses
                        .iter()
                        .map(|p| p.get_value_at(time as f64 * sample_time))
                        .sum::<f64>();
                    let val = noise.iter_mut().try_fold(signal, |signal, n| {
                        n.noisify(signal, time, frame_number as usize)
                    })?;
                    Ok(simulation.voltage_transformation.transform(val) as Intensity)
                })
                .collect::<Result<_, JsonFloatError>>()?,
        })
    }

    pub(crate) fn get_intensities(&self) -> &[Intensity] {
        &self.intensities
    }
}

impl Spanned for Trace {
    fn span(&self) -> &SpanOnce {
        &self.span
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "kebab-case", tag = "pulse-type")]
pub(crate) struct EventPulseTemplate {
    pub(crate) weight: f64,
    pub(crate) pulse_index: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct EventListTemplate {
    pub(crate) pulses: Vec<EventPulseTemplate>,
    pub(crate) noises: Vec<NoiseSource>,
    pub(crate) num_pulses: IntRandomDistribution,
}

#[derive(Default)]
pub(crate) struct EventList<'a> {
    pub(crate) span: SpanOnce,
    pub(crate) pulses: Vec<PulseEvent>,
    pub(crate) noises: &'a [NoiseSource],
}

impl<'a> EventList<'a> {
    #[instrument(skip_all, level = "debug", "New Event List", err(level = "error"))]
    pub(crate) fn new(
        simulator: &Simulation,
        frame_number: FrameNumber,
        source: &'a EventListTemplate,
    ) -> Result<Self, SimulationError> {
        let pulses = {
            let weighted_distribution = if source.pulses.is_empty() {
                None
            } else {
                // This will never panic
                Some(
                    WeightedIndex::new(source.pulses.iter().map(|p| p.weight))
                        .expect("Pulse should be non-empty, this never fails"),
                )
            };
            // Creates a unique template for each channel
            let mut pulses = (0..source.num_pulses.sample(frame_number as usize)? as usize)
                .map(|_| {
                    //  The below is only ever called when weighted_distribution is Some()
                    let weighted_distribution = weighted_distribution
                        .as_ref()
                        .expect("Pulse should be non-empty, this never fails");
                    Ok(PulseEvent::sample(
                        simulator.get_random_pulse_template(source, weighted_distribution)?,
                        frame_number as usize,
                    )?)
                })
                .collect::<Result<Vec<_>, SimulationError>>()?;
            pulses.sort_by_key(|a| a.get_start());
            pulses
        };
        Ok(Self {
            span: SpanOnce::Spanned(tracing::Span::current()),
            pulses,
            noises: &source.noises,
        })
    }
}

impl Spanned for EventList<'_> {
    fn span(&self) -> &SpanOnce {
        &self.span
    }
}
