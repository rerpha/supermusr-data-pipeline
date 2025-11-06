use super::{Real, RealArray, Window};
use num::integer::binomial;
use std::collections::VecDeque;

#[derive(Default, Clone)]
pub(crate) struct FiniteDifferences<const N: usize> {
    coefficients: Vec<Vec<Real>>,
    values: VecDeque<Real>,
    diffs: Vec<Real>,
}

impl<const N: usize> FiniteDifferences<N> {
    pub(crate) fn new() -> Self {
        FiniteDifferences {
            values: VecDeque::<Real>::with_capacity(N),
            coefficients: (0..N)
                .map(|n| {
                    (0..=n)
                        .map(|k| (if k & 1 == 1 { -1. } else { 1. }) * (binomial(n, k) as Real))
                        .collect()
                })
                .collect(),
            diffs: vec![Real::default(); N],
        }
    }

    fn nth_difference(&self, n: usize) -> Real {
        (0..=n)
            .map(|k| self.coefficients[n][k] * self.values[k])
            .sum()
    }
}

impl<const N: usize> Window for FiniteDifferences<N> {
    type TimeType = Real;
    type InputType = Real;
    type OutputType = RealArray<N>;

    fn push(&mut self, value: Self::InputType) -> bool {
        if self.values.len() + 1 < N {
            self.values.push_front(value);
            false
        } else {
            self.values.push_front(value);
            for n in 0..N {
                self.diffs[n] = self.nth_difference(n);
            }
            self.values.pop_back();
            true
        }
    }

    fn output(&self) -> Option<Self::OutputType> {
        (self.values.len() + 1 == N)
            .then_some(RealArray::new(self.diffs.as_slice().try_into().ok()?))
    }

    fn apply_time_shift(&self, time: Self::TimeType) -> Self::TimeType {
        time
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pulse_detection::window::WindowFilter;
    use digital_muon_common::Intensity;

    #[test]
    fn sample_data() {
        let input: Vec<Intensity> = vec![0, 6, 2, 1, 3, 1, 0];
        let mut output = input
            .into_iter()
            .enumerate()
            .map(|(i, v)| (i as Real, v as Real))
            .window(FiniteDifferences::<3>::new())
            .map(|(_, x)| x);

        assert_eq!(output.next(), Some(RealArray::new([2., -4., -10.])));
        assert_eq!(output.next(), Some(RealArray::new([1., -1., 3.])));
        assert_eq!(output.next(), Some(RealArray::new([3., 2., 3.])));
        assert_eq!(output.next(), Some(RealArray::new([1., -2., -4.])));
        assert_eq!(output.next(), Some(RealArray::new([0., -1., 1.])));
        assert!(output.next().is_none());
    }
}
