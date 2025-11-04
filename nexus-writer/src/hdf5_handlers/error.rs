//! This module defines the [NexusHDF5Error] enum to encapsulate all errors that
//! can occur in the [crate::nexus_structure] module.
use crate::error::{FlatBufferInvalidDataTypeContext, FlatBufferMissingError};
use chrono::TimeDelta;
use digital_muon_streaming_types::time_conversions::GpsTimeConversionError;
use hdf5::{Attribute, Dataset, Group, types::TypeDescriptor};
use std::{error::Error, num::TryFromIntError};
use thiserror::Error;

/// Alias type to avoid writing out [Result<T, NexusHDF5Error>] everytime in other modules.
pub(crate) type NexusHDF5Result<T> = Result<T, NexusHDF5Error>;

/// Default string for when no location is set in an [NexusHDF5Error] instance.
const NO_HDF5_PATH_SET: &str = "[No HDF5 Path Set]";

/// Encapsulates all errors which can occur in the [crate::nexus_structure] module.
/// Each variant has an `hdf5_path` field which is initialised to be `None` but can be
/// set by the caller via the `with_hdf5_path` method. This adds additional context,
/// telling the user where in the NeXus file structure the error occurred.
#[derive(Debug, Error)]
pub(crate) enum NexusHDF5Error {
    /// A general error from the hdf5 module.
    #[error("HDF5 Error: {error} at {0}", hdf5_path.as_deref().unwrap_or(NO_HDF5_PATH_SET))]
    HDF5 {
        /// Underlying error.
        error: hdf5::Error,
        /// HDF5 path of the error.
        hdf5_path: Option<String>,
    },
    /// An error converting a string to a [NexusDateTime].
    ///
    /// [NexusDateTime]: super::NexusDateTime
    #[error("DateTime Error: {error} at {0}", hdf5_path.as_deref().unwrap_or(NO_HDF5_PATH_SET))]
    DateTimeConversion {
        /// Underlying error.
        error: chrono::ParseError,
        /// HDF5 path of the error.
        hdf5_path: Option<String>,
    },
    /// A general string error from the hdf5 module.
    #[error("HDF5String Error: {error} at {0}", hdf5_path.as_deref().unwrap_or(NO_HDF5_PATH_SET))]
    HDF5String {
        /// Underlying error.
        error: hdf5::types::StringError,
        /// HDF5 path of the error.
        hdf5_path: Option<String>,
    },
    /// An error converting the [GpsTime] type to a [NexusDateTime].
    ///
    /// [GpsTime]: digital_muon_streaming_types::frame_metadata_v2_generated::GpsTime
    /// [NexusDateTime]: [super::NexusDateTime]
    #[error("Flatbuffer Timestamp Conversion Error {error} at {0}", hdf5_path.as_deref().unwrap_or(NO_HDF5_PATH_SET))]
    FlatBufferTimestampConversion {
        /// Underlying error.
        error: GpsTimeConversionError,
        /// HDF5 path of the error.
        hdf5_path: Option<String>,
    },
    /// An error converting the a [TimeDelta] to nanoseconds from epoch.
    #[error("TimeDelta Error Converting to Nanoseconds at {0}", hdf5_path.as_deref().unwrap_or(NO_HDF5_PATH_SET))]
    TimeDeltaConvertToNanoseconds {
        /// Source timedelta.
        timedelta: TimeDelta,
        /// HDF5 path of the error.
        hdf5_path: Option<String>,
    },
    /// An error resulting from a missing field in a Flatbuffer message.
    #[error("FlatBuffer Missing {error} at {0}", hdf5_path.as_deref().unwrap_or(NO_HDF5_PATH_SET))]
    FlatBufferMissing {
        /// Underlying error.
        error: FlatBufferMissingError,
        /// HDF5 path of the error.
        hdf5_path: Option<String>,
    },
    /// An error resulting from invalid data in a Flatbuffer message.
    #[error("Invalid FlatBuffer {context} Data Type {error} at {0}", hdf5_path.as_deref().unwrap_or(NO_HDF5_PATH_SET))]
    FlatBufferInvalidDataType {
        /// Determines which flatbuffer message type caused the error.
        context: FlatBufferInvalidDataTypeContext,
        /// The data type of the invalid data.
        error: String,
        /// HDF5 path of the error.
        hdf5_path: Option<String>,
    },
    /// Error caused by a sample environment log having inconsistant number of `times` values and `logs` values.
    #[error("Inconsistent Numbers of Sample Environment Log Times and Values {0} != {1} at {2}", sizes.0, sizes.1, hdf5_path.as_deref().unwrap_or(NO_HDF5_PATH_SET))]
    FlatBufferInconsistentSELogTimeValueSizes {
        /// (length of `times` values, length of `logs` values)
        sizes: (usize, usize),
        /// HDF5 path of the error.
        hdf5_path: Option<String>,
    },
    #[error("Invalid HDF5 Type {error} at {0}", hdf5_path.as_deref().unwrap_or(NO_HDF5_PATH_SET))]
    InvalidHDF5Type {
        /// Source `TypeDescriptor`.
        error: TypeDescriptor,
        /// HDF5 path of the error.
        hdf5_path: Option<String>,
    },
    #[error("Invalid HDF5 Conversion {error} at {0}", hdf5_path.as_deref().unwrap_or(NO_HDF5_PATH_SET))]
    InvalidHDF5TypeConversion {
        error: TypeDescriptor,
        /// HDF5 path of the error.
        hdf5_path: Option<String>,
    },
    #[error("IO Error {error} at {0}", hdf5_path.as_deref().unwrap_or(NO_HDF5_PATH_SET))]
    IO {
        /// Underlying error.
        error: std::io::Error,
        /// HDF5 path of the error.
        hdf5_path: Option<String>,
    },
    #[error("Integer Conversion From String Error")]
    ParseInt {
        /// Underlying error.
        error: std::num::ParseIntError,
        /// HDF5 path of the error.
        hdf5_path: Option<String>,
    },
    #[error("Integer Conversion Error")]
    IntConversion {
        /// Underlying error.
        error: TryFromIntError,
        /// HDF5 path of the error.
        hdf5_path: Option<String>,
    },
}

impl NexusHDF5Error {
    /// Attaches a hdf5 path to the error.
    /// # Parameters
    /// - path: the hdf5 path to add.
    /// # Return
    /// If self.hdf5_path is `None`, returns `self` with `self.hdf5_path` as `Some(path)`,
    /// otherwise returns `self` unchanged.
    fn with_hdf5_path(self, path: String) -> Self {
        match self {
            Self::HDF5 {
                error,
                hdf5_path: None,
            } => Self::HDF5 {
                error,
                hdf5_path: Some(path),
            },
            Self::DateTimeConversion {
                error,
                hdf5_path: None,
            } => Self::DateTimeConversion {
                error,
                hdf5_path: Some(path),
            },
            Self::HDF5String {
                error,
                hdf5_path: None,
            } => Self::HDF5String {
                error,
                hdf5_path: Some(path),
            },
            Self::FlatBufferTimestampConversion {
                error,
                hdf5_path: None,
            } => Self::FlatBufferTimestampConversion {
                error,
                hdf5_path: Some(path),
            },
            Self::TimeDeltaConvertToNanoseconds {
                timedelta,
                hdf5_path: None,
            } => Self::TimeDeltaConvertToNanoseconds {
                timedelta,
                hdf5_path: Some(path),
            },
            Self::FlatBufferMissing {
                error,
                hdf5_path: None,
            } => Self::FlatBufferMissing {
                error,
                hdf5_path: Some(path),
            },
            Self::FlatBufferInvalidDataType {
                context,
                error,
                hdf5_path: None,
            } => Self::FlatBufferInvalidDataType {
                context,
                error,
                hdf5_path: Some(path),
            },
            Self::FlatBufferInconsistentSELogTimeValueSizes {
                sizes,
                hdf5_path: None,
            } => Self::FlatBufferInconsistentSELogTimeValueSizes {
                sizes,
                hdf5_path: Some(path),
            },
            Self::InvalidHDF5Type {
                error,
                hdf5_path: None,
            } => Self::InvalidHDF5Type {
                error,
                hdf5_path: Some(path),
            },
            Self::InvalidHDF5TypeConversion {
                error,
                hdf5_path: None,
            } => Self::InvalidHDF5TypeConversion {
                error,
                hdf5_path: Some(path),
            },
            Self::IO {
                error,
                hdf5_path: None,
            } => Self::IO {
                error,
                hdf5_path: Some(path),
            },
            Self::ParseInt {
                error,
                hdf5_path: None,
            } => Self::ParseInt {
                error,
                hdf5_path: Some(path),
            },
            Self::IntConversion {
                error,
                hdf5_path: None,
            } => Self::IntConversion {
                error,
                hdf5_path: Some(path),
            },
            other => other,
        }
    }

    ///  Wraps a [TimeDelta] in a timedelta conversion error.
    /// # Parameters
    /// - timedelta: the [TimeDelta] causing the error.
    /// # Return
    /// A [NexusHDF5Error::TimeDeltaConvertToNanoseconds] error with unset `hdf5_path`.
    pub(crate) fn timedelta_convert_to_ns(timedelta: TimeDelta) -> Self {
        Self::TimeDeltaConvertToNanoseconds {
            timedelta,
            hdf5_path: None,
        }
    }

    ///  Creates a [NexusHDF5Error::FlatBufferInvalidDataType] variant with a given [FlatBufferInvalidDataTypeContext]
    /// and string indicating the data type.
    /// # Parameters
    /// - context: the flatbuffer message type causing the error.
    /// - error: string indicating the data type of the field with invalid data.
    /// # Return
    /// - A [NexusHDF5Error::FlatBufferInvalidDataType] error with unset `hdf5_path`.
    pub(crate) fn flatbuffer_invalid_data_type(
        context: FlatBufferInvalidDataTypeContext,
        error: String,
    ) -> Self {
        Self::FlatBufferInvalidDataType {
            context,
            error,
            hdf5_path: None,
        }
    }

    ///  Wraps a `TypeDescriptor` in a hdf5 type conversion error.
    /// # Parameters
    /// - error: the `TypeDescriptor` causing the error.
    /// # Return
    /// - A [NexusHDF5Error::InvalidHDF5TypeConversion] error with unset `hdf5_path`.
    pub(crate) fn invalid_hdf5_type_conversion(error: TypeDescriptor) -> Self {
        Self::InvalidHDF5TypeConversion {
            error,
            hdf5_path: None,
        }
    }
}

impl From<std::num::ParseIntError> for NexusHDF5Error {
    fn from(error: std::num::ParseIntError) -> Self {
        NexusHDF5Error::ParseInt {
            error,
            hdf5_path: None,
        }
    }
}

impl From<hdf5::Error> for NexusHDF5Error {
    fn from(error: hdf5::Error) -> Self {
        NexusHDF5Error::HDF5 {
            error,
            hdf5_path: None,
        }
    }
}

impl From<hdf5::types::StringError> for NexusHDF5Error {
    fn from(error: hdf5::types::StringError) -> Self {
        NexusHDF5Error::HDF5String {
            error,
            hdf5_path: None,
        }
    }
}

impl From<chrono::ParseError> for NexusHDF5Error {
    fn from(error: chrono::ParseError) -> Self {
        NexusHDF5Error::DateTimeConversion {
            error,
            hdf5_path: None,
        }
    }
}

impl From<GpsTimeConversionError> for NexusHDF5Error {
    fn from(error: GpsTimeConversionError) -> Self {
        NexusHDF5Error::FlatBufferTimestampConversion {
            error,
            hdf5_path: None,
        }
    }
}

impl From<FlatBufferMissingError> for NexusHDF5Error {
    fn from(error: FlatBufferMissingError) -> Self {
        NexusHDF5Error::FlatBufferMissing {
            error,
            hdf5_path: None,
        }
    }
}

impl From<std::io::Error> for NexusHDF5Error {
    fn from(error: std::io::Error) -> Self {
        NexusHDF5Error::IO {
            error,
            hdf5_path: None,
        }
    }
}

impl From<TryFromIntError> for NexusHDF5Error {
    fn from(error: TryFromIntError) -> Self {
        NexusHDF5Error::IntConversion {
            error,
            hdf5_path: None,
        }
    }
}

/// Used to allow errors which can be converted to [NexusHDF5Error] to be
/// appended with hdf5 paths.
/// This is implemented by [Result<_,E>] to enable errors to be conventiently managed.
pub(crate) trait ConvertResult<T, E>
where
    E: Error + Into<NexusHDF5Error>,
{
    /// Converts the error to [NexusHDF5Error] type and adds the path of `group` to it.
    fn err_group(self, group: &Group) -> NexusHDF5Result<T>;
    /// Converts the error to [NexusHDF5Error] type and adds the path of `dataset` to it.
    fn err_dataset(self, dataset: &Dataset) -> NexusHDF5Result<T>;
    /// Converts the error to [NexusHDF5Error] type and adds the path of `attribute` to it.
    fn err_attribute(self, attribute: &Attribute) -> NexusHDF5Result<T>;
}

impl<T, E> ConvertResult<T, E> for Result<T, E>
where
    E: Error + Into<NexusHDF5Error>,
{
    fn err_group(self, group: &Group) -> NexusHDF5Result<T> {
        self.map_err(|e| e.into().with_hdf5_path(group.name()))
    }

    fn err_dataset(self, dataset: &Dataset) -> NexusHDF5Result<T> {
        self.map_err(|e| e.into().with_hdf5_path(dataset.name()))
    }

    fn err_attribute(self, attribute: &Attribute) -> NexusHDF5Result<T> {
        self.map_err(|e| e.into().with_hdf5_path(attribute.name()))
    }
}
