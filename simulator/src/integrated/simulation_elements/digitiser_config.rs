use crate::integrated::{
    simulation_elements::{
        Interval,
        utils::{IntConstant, JsonIntError},
    },
    simulation_engine::engine::SimulationEngineDigitiser,
};
use digital_muon_common::{Channel, DigitizerId};
use serde::Deserialize;
use tracing::instrument;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum DigitiserConfig {
    #[serde(rename_all = "kebab-case")]
    AutoAggregatedFrame { num_channels: IntConstant },
    #[serde(rename_all = "kebab-case")]
    ManualAggregatedFrame { channels: Vec<Channel> },
    #[serde(rename_all = "kebab-case")]
    AutoDigitisers {
        num_digitisers: IntConstant,
        num_channels_per_digitiser: IntConstant,
    },
    #[serde(rename_all = "kebab-case")]
    ManualDigitisers(Vec<Digitiser>),
}

impl DigitiserConfig {
    #[instrument(skip_all)]
    pub(crate) fn generate_channels(&self) -> Result<Vec<Channel>, JsonIntError> {
        let channels = match self {
            DigitiserConfig::AutoAggregatedFrame { num_channels } => {
                (0..num_channels.value()? as Channel).collect()
            }
            DigitiserConfig::ManualAggregatedFrame { channels } => channels.clone(),
            DigitiserConfig::AutoDigitisers {
                num_digitisers,
                num_channels_per_digitiser,
            } => (0..((num_digitisers.value()? * num_channels_per_digitiser.value()?) as Channel))
                .collect(),
            DigitiserConfig::ManualDigitisers(digitisers) => digitisers
                .iter()
                .flat_map(|digitiser| digitiser.channels.range_inclusive())
                .collect(),
        };
        Ok(channels)
    }

    #[instrument(skip_all)]
    pub(crate) fn generate_digitisers(
        &self,
    ) -> Result<Vec<SimulationEngineDigitiser>, JsonIntError> {
        let digitisers = match self {
            DigitiserConfig::AutoAggregatedFrame { .. } => Default::default(),
            DigitiserConfig::ManualAggregatedFrame { .. } => Default::default(),
            DigitiserConfig::AutoDigitisers {
                num_digitisers,
                num_channels_per_digitiser,
            } => (0..num_digitisers.value()?)
                .map(|d| {
                    Ok(SimulationEngineDigitiser::new(
                        d as DigitizerId,
                        ((d as usize * num_channels_per_digitiser.value()? as usize)
                            ..((d as usize + 1) * num_channels_per_digitiser.value()? as usize))
                            .collect(),
                    ))
                })
                .collect::<Result<_, JsonIntError>>()?,
            DigitiserConfig::ManualDigitisers(digitisers) => digitisers
                .iter()
                .map(|digitiser| SimulationEngineDigitiser {
                    id: digitiser.id,
                    channel_indices: Vec::<_>::new(), //TODO
                })
                .collect(),
        };
        Ok(digitisers)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct Digitiser {
    pub(crate) id: DigitizerId,
    pub(crate) channels: Interval<Channel>,
}
