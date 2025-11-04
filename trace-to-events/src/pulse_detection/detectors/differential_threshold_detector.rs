use super::{Detector, EventData, Real};
use crate::pulse_detection::{
    datatype::tracevalue::TraceArray, threshold_detector::ThresholdDuration,
};
use std::fmt::Display;

#[derive(Default, Debug, Clone, PartialEq)]
pub(crate) struct Data {
    pub(crate) pulse_height: Real,
}

impl Display for Data {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.pulse_height)
    }
}

impl EventData for Data {}

#[derive(Default, Clone)]
pub(crate) struct DifferentialThresholdDetector {
    trigger: ThresholdDuration,

    time_of_last_return: Option<Real>,
    /// If provided, the pulse height is the height of the rising edge, scaled by this value,
    /// otherwise, the pulse height is the maximum value of the trace, during the event detection.
    constant_multiple: Option<Real>,
    time_crossed: Option<Real>,
    temp_time: Option<Real>,
    max_derivative: TraceArray<2, Real>,
}

impl DifferentialThresholdDetector {
    pub(crate) fn new(trigger: &ThresholdDuration, constant_multiple: Option<Real>) -> Self {
        Self {
            trigger: trigger.clone(),
            constant_multiple,
            ..Default::default()
        }
    }
}

pub(crate) type ThresholdEvent = (Real, Data);

impl Detector for DifferentialThresholdDetector {
    type TracePointType = (Real, TraceArray<2, Real>);
    type EventPointType = (Real, Data);

    fn signal(&mut self, time: Real, value: TraceArray<2, Real>) -> Option<ThresholdEvent> {
        match self.time_crossed {
            Some(time_crossed) => {
                // If we are already over the threshold
                if self.constant_multiple.is_some() {
                    if self.max_derivative[1] < value[1] {
                        // Set update the max derivative if the current derivative is higher.
                        self.max_derivative = value;
                        if self.temp_time.is_some() {
                            self.temp_time = Some(time);
                        }
                    }
                } else {
                    self.max_derivative[0] = self.max_derivative[0].max(value[0]);
                }

                if time - time_crossed == self.trigger.duration as Real {
                    // If the current value is below the threshold
                    self.temp_time = Some(time_crossed);
                }

                if value[1] <= 0.0 {
                    // If the current differential is non-positive
                    self.time_crossed = None;
                    if time - time_crossed >= self.trigger.duration as Real {
                        self.time_of_last_return = Some(time);

                        if let Some(time) = &self.temp_time {
                            let pulse_height = self
                                .constant_multiple
                                .map(|mul| self.max_derivative[0] * mul)
                                .unwrap_or(self.max_derivative[0]);
                            let result = (*time, Data { pulse_height });
                            self.temp_time = None;
                            Some(result)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            None => {
                //  If we are under the threshold
                if value[1] > self.trigger.threshold {
                    // If the current value as over the threshold
                    // If we have a "time_of_last_return", then test if we have passed the cool-down time
                    match self.time_of_last_return {
                        Some(time_of_last_return) => {
                            if time - time_of_last_return >= self.trigger.cool_off as Real {
                                self.max_derivative = value;
                                self.time_crossed = Some(time);
                            }
                        }
                        None => {
                            self.max_derivative = value;
                            self.time_crossed = Some(time);
                        }
                    }
                }
                None
            }
        }
    }

    fn finish(&mut self) -> Option<Self::EventPointType> {
        let result = self.temp_time;
        self.temp_time = None;
        result.map(|time| {
            let pulse_height = self
                .constant_multiple
                .map(|mul| self.max_derivative[0] * mul)
                .unwrap_or(self.max_derivative[0]);
            (time, Data { pulse_height })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pulse_detection::{EventFilter, Real, WindowFilter, window::FiniteDifferences};
    use digital_muon_common::Intensity;

    #[test]
    fn zero_data() {
        let data: [Real; 0] = [];
        let detector = DifferentialThresholdDetector::new(
            &ThresholdDuration {
                threshold: 2.0,
                cool_off: 0,
                duration: 2,
            },
            Some(2.0),
        );
        let mut iter = data
            .into_iter()
            .enumerate()
            .map(|(i, v)| (i as Real, v as Real))
            .window(FiniteDifferences::<2>::new())
            .events(detector);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_positive_threshold() {
        let data = [4, 3, 2, 5, 6, 1, 5, 7, 2, 4];
        let detector = DifferentialThresholdDetector::new(
            &ThresholdDuration {
                threshold: 2.0,
                cool_off: 0,
                duration: 2,
            },
            Some(2.0),
        );
        let mut iter = data
            .into_iter()
            .enumerate()
            .map(|(i, v)| (i as Real, v as Real))
            .window(FiniteDifferences::<2>::new())
            .events(detector);
        assert_eq!(iter.next(), Some((3.0, Data { pulse_height: 10.0 })));
        assert_eq!(iter.next(), Some((6.0, Data { pulse_height: 10.0 })));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_positive_threshold_no_constant_multiple() {
        let data = [4, 3, 2, 5, 6, 1, 5, 7, 2, 4];
        let detector = DifferentialThresholdDetector::new(
            &ThresholdDuration {
                threshold: 2.0,
                cool_off: 0,
                duration: 2,
            },
            None,
        );
        let mut iter = data
            .into_iter()
            .enumerate()
            .map(|(i, v)| (i as Real, v as Real))
            .window(FiniteDifferences::<2>::new())
            .events(detector);
        assert_eq!(iter.next(), Some((3.0, Data { pulse_height: 6.0 })));
        assert_eq!(iter.next(), Some((6.0, Data { pulse_height: 7.0 })));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_zero_duration() {
        let data = [4, 3, 2, 5, 2, 1, 5, 7, 2, 2];
        let detector = DifferentialThresholdDetector::new(
            &ThresholdDuration {
                threshold: -2.5,
                cool_off: 0,
                duration: 0,
            },
            Some(2.0),
        );
        let mut iter = data
            .into_iter()
            .enumerate()
            .map(|(i, v)| (i as Real, -v as Real))
            .window(FiniteDifferences::<2>::new())
            .events(detector);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_cool_off() {
        // With a 1 sample cool-off the detector triggers at the following points
        //          .  .  .  x  .  .  x  .  .  x  .  x  .  x
        // With a 2 sample cool-off the detector triggers at the following points
        //          .  .  .  x  .  .  x  .  .  .  .  x  .  .
        // With a 3 sample cool-off the detector triggers at the following points
        //          .  .  .  x  .  .  .  .  .  x  .  .  .  x
        let data = [4, 3, 2, 5, 2, 1, 5, 7, 2, 6, 5, 8, 8, 11, 0];
        let detector2 = DifferentialThresholdDetector::new(
            &ThresholdDuration {
                threshold: 2.5,
                cool_off: 3,
                duration: 1,
            },
            Some(2.0),
        );
        let mut iter = data
            .iter()
            .copied()
            .enumerate()
            .map(|(i, v)| (i as Real, v as Real))
            .window(FiniteDifferences::<2>::new())
            .events(detector2);
        assert_eq!(iter.next(), Some((3.0, Data { pulse_height: 10.0 })));
        assert_eq!(iter.next(), Some((9.0, Data { pulse_height: 12.0 })));
        assert_eq!(iter.next(), Some((13.0, Data { pulse_height: 22.0 })));
        assert_eq!(iter.next(), None);

        let detector1 = DifferentialThresholdDetector::new(
            &ThresholdDuration {
                threshold: 2.5,
                cool_off: 2,
                duration: 1,
            },
            Some(2.0),
        );

        let mut iter = data
            .into_iter()
            .enumerate()
            .map(|(i, v)| (i as Real, v as Real))
            .window(FiniteDifferences::<2>::new())
            .events(detector1);
        assert_eq!(iter.next(), Some((3.0, Data { pulse_height: 10.0 })));
        assert_eq!(iter.next(), Some((6.0, Data { pulse_height: 10.0 })));
        assert_eq!(iter.next(), Some((11.0, Data { pulse_height: 16.0 })));
        assert_eq!(iter.next(), None);

        let detector0 = DifferentialThresholdDetector::new(
            &ThresholdDuration {
                threshold: 2.5,
                cool_off: 1,
                duration: 1,
            },
            Some(2.0),
        );

        let mut iter = data
            .into_iter()
            .enumerate()
            .map(|(i, v)| (i as Real, v as Real))
            .window(FiniteDifferences::<2>::new())
            .events(detector0);
        assert_eq!(iter.next(), Some((3.0, Data { pulse_height: 10.0 })));
        assert_eq!(iter.next(), Some((6.0, Data { pulse_height: 10.0 })));
        assert_eq!(iter.next(), Some((9.0, Data { pulse_height: 12.0 })));
        assert_eq!(iter.next(), Some((11.0, Data { pulse_height: 16.0 })));
        assert_eq!(iter.next(), Some((13.0, Data { pulse_height: 22.0 })));
        assert_eq!(iter.next(), None);
    }

    fn b2bexp(
        x: Real,
        ampl: Real,
        spread: Real,
        x0: Real,
        rising: Real,
        falling: Real,
    ) -> Intensity {
        let normalising_factor = ampl * 0.5 * (rising * falling) / (rising + falling);
        let rising_spread = rising * spread.powi(2);
        let falling_spread = falling * spread.powi(2);
        let x_shift = x - x0;
        let rising_exp = Real::exp(rising * 0.5 * (rising_spread + 2.0 * x_shift));
        let rising_erfc = libm::erfc((rising_spread + x_shift) / (Real::sqrt(2.0) * spread));
        let falling_exp = Real::exp(falling * 0.5 * (falling_spread - 2.0 * x_shift));
        let falling_erfc = libm::erfc((falling_spread - x_shift) / (Real::sqrt(2.0) * spread));
        (normalising_factor * (rising_exp * rising_erfc + falling_exp * falling_erfc)) as Intensity
    }

    #[test]
    fn test_b2bexp() {
        let range = 0..100;
        let data = range
            .clone()
            .map(|x| {
                b2bexp(x as Real, 1000.0, 3.5, 20.0, 3.5, 2.25)
                    + b2bexp(x as Real, 1000.0, 3.5, 54.0, 4.5, 5.5)
                    + b2bexp(x as Real, 1000.0, 3.5, 81.0, 1.5, 3.25)
            })
            .collect::<Vec<_>>();

        let detector = DifferentialThresholdDetector::new(
            &ThresholdDuration {
                threshold: 3.0,
                cool_off: 0,
                duration: 1,
            },
            Some(2.0),
        );
        let mut iter = data
            .into_iter()
            .enumerate()
            .map(|(i, v)| (i as Real, v as Real))
            .window(FiniteDifferences::<2>::new())
            .events(detector);
        let result = Some((
            17.0,
            Data {
                pulse_height: 150.0,
            },
        ));
        assert_eq!(iter.next(), result);
        let result = Some((
            50.0,
            Data {
                pulse_height: 120.0,
            },
        ));
        assert_eq!(iter.next(), result);
        let result = Some((
            77.0,
            Data {
                pulse_height: 132.0,
            },
        ));
        assert_eq!(iter.next(), result);
        assert_eq!(iter.next(), None);
    }
}
