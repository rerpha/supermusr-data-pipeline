use crate::integrated::simulation_elements::pulses::PulseEvent;
use digital_muon_common::Time;
use std::{collections::VecDeque, slice::Iter};

pub(super) struct ActivePulses<'a> {
    active: VecDeque<&'a PulseEvent>,
    muon_iter: Iter<'a, PulseEvent>,
}

impl<'a> ActivePulses<'a> {
    pub(super) fn new(source: &'a [PulseEvent]) -> Self {
        Self {
            active: Default::default(),
            muon_iter: source.iter(),
        }
    }

    pub(super) fn drop_spent_muons(&mut self, time: Time) {
        while self
            .active
            .front()
            .and_then(|m| (m.get_end() < time).then_some(m))
            .is_some()
        {
            self.active.pop_front();
        }
    }

    pub(super) fn push_new_muons(&mut self, time: Time) {
        while let Some(iter) = self
            .muon_iter
            .next()
            .and_then(|iter| (iter.get_start() > time).then_some(iter))
        {
            self.active.push_back(iter)
        }
    }

    pub(super) fn iter(&self) -> std::collections::vec_deque::Iter<'_, &PulseEvent> {
        self.active.iter()
    }
}
