use super::Real;
use digital_muon_common::Intensity;
use std::fmt::{Debug, Display};

pub(crate) mod eventdata;
pub(crate) mod eventpoint;
pub(crate) mod tracepoint;
pub(crate) mod tracevalue;

pub(crate) use eventdata::EventData;
pub(crate) use eventpoint::EventPoint;
pub(crate) use tracepoint::TracePoint;
pub(crate) use tracevalue::{RealArray, Stats, TraceValue};

/// This trait abstracts any type used as a time variable
pub(crate) trait Temporal: Default + Copy + Debug + Display + PartialEq {}

impl Temporal for Intensity {}

impl Temporal for Real {}
