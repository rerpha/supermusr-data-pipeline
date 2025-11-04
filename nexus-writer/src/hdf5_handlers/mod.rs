//! Defines traits which extend hdf5 types, provind more convenient and
//! robust methodsfor building and writing to NeXus files.
mod attribute;
mod dataset;
mod dataset_flatbuffers;
mod error;
mod group;

use crate::run_engine::NexusDateTime;
use digital_muon_streaming_types::{
    ecs_f144_logdata_generated::f144_LogData, ecs_se00_data_generated::se00_SampleEnvironmentData,
};
pub(crate) use error::{ConvertResult, NexusHDF5Error, NexusHDF5Result};
use hdf5::{Attribute, Dataset, Group, H5Type, types::TypeDescriptor};

/// This is implemented by hdf5 types [Group] and [Dataset], both can have attributes set
/// and this trait provides a common interface for them both.
pub(crate) trait HasAttributesExt: Sized {
    /// Creates a new attribute, with name as specified.
    /// # Parameters
    ///  - attr: name of the attribute to add.
    /// # Error
    /// Any errors are tagged with the relevant hdf5 path.
    fn add_attribute<T: H5Type>(&self, attr: &str) -> NexusHDF5Result<Attribute>;

    /// Creates a new string-typed attribute, with name and contents as specified.
    /// # Parameters
    ///  - attr: name of the attribute to add.
    /// # Error
    /// Any errors are tagged with the relevant hdf5 path.
    fn add_string_attribute(&self, attr: &str) -> NexusHDF5Result<Attribute>;

    /// Creates a new string-typed attribute, with name and contents as specified.
    /// # Parameters
    ///  - attr: name of the attribute to add.
    ///  - value: content of the attribute to add.
    /// # Error
    /// Any errors are tagged with the relevant hdf5 path.
    fn add_constant_string_attribute(&self, attr: &str, value: &str) -> NexusHDF5Result<Attribute>;

    /// Returns the attribute matching the given name.
    /// # Parameters
    ///  - attr: name of the attribute to get.
    /// # Error
    /// Any errors are tagged with the relevant hdf5 path.
    fn get_attribute(&self, attr: &str) -> NexusHDF5Result<Attribute>;

    /// Creates a new attribute, with name as specified, and return the original calling object.
    /// # Parameters
    ///  - attr: name of the attribute to add.
    /// # Error
    /// Any errors are tagged with the relevant hdf5 path.
    fn with_attribute<T: H5Type>(self, attr: &str) -> NexusHDF5Result<Self> {
        self.add_attribute::<T>(attr)?;
        Ok(self)
    }

    /// Creates a new string-typed attribute, with name as specified, and return the original calling object.
    /// # Parameters
    ///  - attr: name of the attribute to add.
    /// # Error
    /// Any errors are tagged with the relevant hdf5 path.
    fn with_string_attribute(self, attr: &str) -> NexusHDF5Result<Self> {
        self.add_string_attribute(attr)?;
        Ok(self)
    }

    /// Creates a new string-typed attribute, with name and content as specified, and return the original calling object.
    /// # Parameters
    ///  - attr: name of the attribute to add.
    /// # Error
    /// Any errors are tagged with the relevant hdf5 path.
    fn with_constant_string_attribute(self, attr: &str, value: &str) -> NexusHDF5Result<Self> {
        self.add_constant_string_attribute(attr, value)?;
        Ok(self)
    }
}

/// Provides methods to be called on the hdf5 `Group` type.
/// These provide additional guarantees that the resulting `Group`
/// is NeXus compliant.
pub(crate) trait GroupExt {
    /// Creates a new subgroup of this group, with name and class as specified.
    /// # Parameters
    ///  - name: name of the group to add.
    /// # Error
    /// Any errors are tagged with the relevant hdf5 path by [err_group].
    ///
    /// [err_group]: ConvertResult::err_group
    fn add_new_group(&self, name: &str, class: &str) -> NexusHDF5Result<Group>;

    /// Create an attribute in this group named "NX_class" and contents as specified.
    /// # Parameters
    /// - class: the name of the class to add to the group.
    /// # Error
    /// Any errors are tagged with the relevant hdf5 path by [err_group].
    ///
    /// [err_group]: ConvertResult::err_group
    fn set_nx_class(&self, class: &str) -> NexusHDF5Result<()>;

    /// Creates a new one-dimensional dataset in this group with static type `T`.
    /// # Parameters
    ///  - name: name of the dataset to add.
    /// # Error
    /// Any errors are tagged with the relevant hdf5 path by [err_group].
    ///
    /// [err_group]: ConvertResult::err_group
    fn create_resizable_empty_dataset<T: H5Type>(
        &self,
        name: &str,
        chunk_size: usize,
    ) -> NexusHDF5Result<Dataset>;

    /// Creates a new one-dimensional dataset in this group with type dynamically specified by `type_descriptor`.
    /// # Parameters
    ///  - name: name of the dataset to add.
    /// # Error
    /// Any errors are tagged with the relevant hdf5 path by [err_group].
    ///
    /// [err_group]: ConvertResult::err_group
    fn create_dynamic_resizable_empty_dataset(
        &self,
        name: &str,
        type_descriptor: &TypeDescriptor,
        chunk_size: usize,
    ) -> NexusHDF5Result<Dataset>;

    /// Creates a new scalar dataset in this group with static type `T`.
    /// # Parameters
    ///  - name: name of the dataset to add.
    /// # Error
    /// Any errors are tagged with the relevant hdf5 path by [err_group].
    ///
    /// [err_group]: ConvertResult::err_group
    fn create_scalar_dataset<T: H5Type>(&self, name: &str) -> NexusHDF5Result<Dataset>;

    /// Creates a new scalar dataset in this group with static type [VarLenUnicode].
    /// # Parameters
    ///  - name: name of the dataset to add.
    /// # Error
    /// Any errors are tagged with the relevant hdf5 path by [err_group].
    ///
    /// [VarLenUnicode]: hdf5::types::VarLenUnicode
    /// [err_group]: ConvertResult::err_group
    fn create_string_dataset(&self, name: &str) -> NexusHDF5Result<Dataset>;

    /// Creates a new scalar dataset in this group with static type `T`, and contents as specified.
    /// # Parameters
    ///  - name: name of the dataset to add.
    /// # Error
    /// Any errors are tagged with the relevant hdf5 path by [err_group].
    ///
    /// [err_group]: ConvertResult::err_group
    fn create_constant_scalar_dataset<T: H5Type>(
        &self,
        name: &str,
        value: &T,
    ) -> NexusHDF5Result<Dataset>;

    /// Creates a new scalar dataset in this group with static type [VarLenUnicode], and contents as specified.
    /// # Parameters
    ///  - name: name of the dataset to add.
    /// # Error
    /// Any errors are tagged with the relevant hdf5 path by [err_group].
    ///
    /// [VarLenUnicode]: hdf5::types::VarLenUnicode
    /// [err_group]: ConvertResult::err_group
    fn create_constant_string_dataset(&self, name: &str, value: &str) -> NexusHDF5Result<Dataset>;

    /// Returns the dataset in this group matching the given name.
    /// # Parameters
    ///  - name: name of the dataset to get.
    /// # Error
    /// Any errors are tagged with the relevant hdf5 path by [err_group].
    ///
    /// [err_group]: ConvertResult::err_group
    fn get_dataset(&self, name: &str) -> NexusHDF5Result<Dataset>;

    #[cfg(test)]
    fn get_dataset_or_else<F>(&self, name: &str, f: F) -> NexusHDF5Result<Dataset>
    where
        F: Fn(&Group) -> NexusHDF5Result<Dataset>;

    /// Returns the subgroup in this group matching the given name.
    /// # Parameters
    ///  - name: name of the group to get.
    /// # Error
    /// Any errors are tagged with the relevant hdf5 path by [err_group].
    ///
    /// [err_group]: ConvertResult::err_group
    fn get_group(&self, name: &str) -> NexusHDF5Result<Group>;

    #[cfg(test)]
    fn get_group_or_create_new(&self, name: &str, class: &str) -> NexusHDF5Result<Group>;
}

/// This trait provides methods to be called on the hdf5 [Dataset] type.
/// These methods provide additional guarantees that the resulting [Dataset]
/// is NeXus compliant.
pub(crate) trait DatasetExt {
    /// Sets the value of the dataset to the single value at the provided reference.
    /// # Parameters
    /// - value: value to set the dataset to.
    /// # Error
    /// Emits an error if either of the following requirements on the [Dataset] are violated:
    /// - was created with type `T`,
    /// - is scalar.
    ///
    /// Any errors are tagged with the relevant hdf5 path by [err_dataset].
    ///
    /// [err_dataset]: ConvertResult::err_dataset
    fn set_scalar<T: H5Type>(&self, value: &T) -> NexusHDF5Result<()>;

    /// Sets the value of the dataset to the given string slice value.
    /// # Parameters
    /// - value: slice to set the dataset to.
    /// # Error
    /// Emits an error if either of the following requirements on the [Dataset] are violated:
    /// - was created with type [VarLenUnicode],
    /// - is scalar.
    ///
    /// Any errors are tagged with the relevant hdf5 path by [err_dataset].
    ///
    /// [VarLenUnicode]: hdf5::types::VarLenUnicode
    /// [err_dataset]: ConvertResult::err_dataset
    fn set_string(&self, value: &str) -> NexusHDF5Result<()>;

    /// Sets the value of the dataset to the slice value.
    /// # Parameters
    /// - value: slice to set the dataset to.
    /// # Error
    /// Emits an error if either of the following requirements on the [Dataset] are violated:
    /// - was created with type `T`,
    /// - is one-dimentional.
    ///
    /// Any errors are tagged with the relevant hdf5 path by [err_dataset].
    ///
    /// [err_dataset]: ConvertResult::err_dataset
    fn set_slice<T: H5Type>(&self, value: &[T]) -> NexusHDF5Result<()>;

    /// Increases the size of the dataset by one, and sets the new value to value at the provided reference.
    /// # Parameters
    /// - value: value to set the dataset to.
    /// # Error
    /// Emits an error if either of the following requirements on the [Dataset] are violated:
    /// - was created with type `T`,
    /// - is one-dimentional.
    ///
    /// Any errors are tagged with the relevant hdf5 path by [err_dataset].
    ///
    /// [err_dataset]: ConvertResult::err_dataset
    fn append_value<T: H5Type>(&self, value: T) -> NexusHDF5Result<()>;

    /// Increases the size of the dataset by the size of the given slice, and sets the new values to ones in the provided slice.
    /// # Parameters
    /// - value: value to set the dataset to.
    /// # Error
    /// Emits an error if either of the following requirements on the [Dataset] are violated:
    /// - was created with type `T`,
    /// - is one-dimentional.
    ///
    /// Any errors are tagged with the relevant hdf5 path by [err_dataset].
    ///
    /// [err_dataset]: ConvertResult::err_dataset
    fn append_slice<T: H5Type>(&self, value: &[T]) -> NexusHDF5Result<()>;

    /// Return a [String] with the contents of the dataset.
    /// # Error
    /// Emits an error if either of the following requirements on the [Dataset] are violated:
    /// - was created with type [VarLenUnicode],
    /// - is scalar.
    ///
    /// Any errors are tagged with the relevant hdf5 path by [err_dataset].
    ///
    /// [VarLenUnicode]: hdf5::types::VarLenUnicode
    /// [err_dataset]: ConvertResult::err_dataset
    fn get_string(&self) -> NexusHDF5Result<String>;

    /// Return the timestamp from the dataset.
    /// # Return
    /// - The timestamp of the dataset.
    /// # Error
    /// Emits an error if any of the following requirements on the [Dataset] are violated:
    /// - was created with type [VarLenUnicode],
    /// - is scalar,
    /// - the contents can be parsed into a valid [NexusDateTime],
    ///
    /// Any errors are tagged with the relevant hdf5 path by [err_dataset].
    ///
    /// [VarLenUnicode]: hdf5::types::VarLenUnicode
    /// [err_dataset]: ConvertResult::err_dataset
    fn get_datetime(&self) -> NexusHDF5Result<NexusDateTime>;
}

/// Provides methods to be called on the hdf5 [Dataset] type,
/// for appending data from specific flatbuffer log messages.
///
/// These methods provide additional guarantees that the resulting [Dataset]
/// is NeXus compliant.
pub(crate) trait DatasetFlatbuffersExt {
    /// Appends values from the given flatbuffer [f144_LogData] message.
    /// # Parameters
    /// - data: [f144_LogData] message to take data from.
    /// # Error
    /// Emits an error if either of the following requirements on the [Dataset] are violated:
    /// - was created with type appropraite for the [f144_LogData] message,
    /// - is one-dimentional.
    ///
    /// Any errors are tagged with the relevant hdf5 path by [err_dataset].
    ///
    /// [err_dataset]: ConvertResult::err_dataset
    fn append_f144_value(&self, data: &f144_LogData<'_>) -> NexusHDF5Result<()>;

    /// Appends values from the given flatbuffer [se00_SampleEnvironmentData] message.
    /// # Parameters
    /// - data: [se00_SampleEnvironmentData] message to take data from.
    /// # Error
    /// Emits an error if either of the following requirements on the [Dataset] are violated:
    /// - was created with type appropraite for the [se00_SampleEnvironmentData] message,
    /// - is one-dimentional.
    ///
    /// Any errors are tagged with the relevant hdf5 path by [err_dataset].
    ///
    /// [err_dataset]: ConvertResult::err_dataset
    fn append_se00_value_slice(&self, data: &se00_SampleEnvironmentData<'_>)
    -> NexusHDF5Result<()>;
}

/// This trait provides methods to be called on the hdf5 [Attribute] type.
/// These methods provide additional guarantees that the resulting [Attribute]
/// is NeXus compliant.
pub(crate) trait AttributeExt {
    /// Sets the value of the [Attribute] to the given string slice value.
    /// # Parameters
    /// - value: slice to set the [Attribute] to.
    /// # Error
    /// Emits an error if either of the following requirements on the [Attribute] are violated:
    /// - was created with type [VarLenUnicode],
    /// - is scalar.
    ///
    /// Any errors are tagged with the relevant hdf5 path by [err_attribute].
    ///
    /// [VarLenUnicode]: hdf5::types::VarLenUnicode
    /// [err_attribute]: ConvertResult::err_attribute
    fn set_string(&self, value: &str) -> NexusHDF5Result<()>;

    /// Returns the timestamp from the attribute.
    /// # Error
    /// Emits an error if any of the following requirements on the [Attribute] are violated:
    /// - was created with type [VarLenUnicode],
    /// - is scalar,
    /// - the contents can be parsed into a valid [NexusDateTime].
    ///
    /// Any errors are tagged with the relevant hdf5 path by [err_attribute].
    ///
    /// [VarLenUnicode]: hdf5::types::VarLenUnicode
    /// [err_attribute]: ConvertResult::err_attribute
    fn get_datetime(&self) -> NexusHDF5Result<NexusDateTime>;

    /// Returns a [String] with the contents of the attribute.
    /// # Parameters
    /// - [String] with the contents of the attribute.
    /// # Error
    /// Emits an error if any of the following requirements on the [Attribute] are violated:
    /// - was created with type [VarLenUnicode],
    /// - is scalar.
    ///
    /// Any errors are tagged with the relevant hdf5 path by [err_attribute].
    ///
    /// [VarLenUnicode]: hdf5::types::VarLenUnicode
    /// [err_attribute]: ConvertResult::err_attribute
    fn get_string(&self) -> NexusHDF5Result<String>;
}

#[cfg(test)]
mod tests {
    use std::{env::temp_dir, ops::Deref, path::PathBuf};

    use super::*;

    // Helper struct to create and tidy-up a temp hdf5 file
    struct OneTempFile(Option<hdf5::File>, PathBuf);
    // Suitably long temp file name, unlikely to clash with anything else
    const TEMP_FILE_PREFIX: &str = "temp_digital_muon_pipeline_nexus_writer_file";

    impl OneTempFile {
        //  We need a different file for each test, so they can run in parallel
        fn new(test_name: &str) -> Self {
            let mut path = temp_dir();
            path.push(format!("{TEMP_FILE_PREFIX}_{test_name}.nxs"));
            Self(Some(hdf5::File::create(&path).unwrap()), path)
        }
    }

    //  Cleans up the temp directory after our test
    impl Drop for OneTempFile {
        fn drop(&mut self) {
            let file = self.0.take().unwrap();
            file.close().unwrap();
            std::fs::remove_file(&self.1).unwrap();
        }
    }

    //  So we can use our OneTempFile as an hdf5 file
    impl Deref for OneTempFile {
        type Target = hdf5::File;

        fn deref(&self) -> &Self::Target {
            self.0.as_ref().unwrap()
        }
    }

    #[test]
    fn create_group() {
        let file = OneTempFile::new("create_group");
        let maybe_group = file.get_group_or_create_new("my_group", "my_class");

        assert!(maybe_group.is_ok());
        assert_eq!(maybe_group.unwrap().name().as_str(), "/my_group");
    }

    #[test]
    fn create_nested_group() {
        let file = OneTempFile::new("create_nested_group");
        let group = file
            .get_group_or_create_new("my_group", "my_class")
            .unwrap();
        let maybe_subgroup = group.get_group_or_create_new("my_subgroup", "my_subclass");

        assert!(maybe_subgroup.is_ok());
        assert_eq!(
            maybe_subgroup.unwrap().name().as_str(),
            "/my_group/my_subgroup"
        );
    }

    #[test]
    fn create_dataset() {
        let file = OneTempFile::new("create_dataset");
        let maybe_dataset = file.get_dataset_or_else("my_dataset", |group| {
            group.create_scalar_dataset::<u8>("my_dataset")
        });

        assert!(maybe_dataset.is_ok());
        assert_eq!(maybe_dataset.unwrap().name().as_str(), "/my_dataset");
    }

    #[test]
    fn open_nonexistant_group() {
        let file = OneTempFile::new("open_nonexistant_group");
        let maybe_group = file.get_group("non_existant_group");

        assert!(maybe_group.is_err());

        const EXPECTED_ERR_MSG: &str = "HDF5 Error: H5Gopen2(): unable to synchronously open group: object 'non_existant_group' doesn't exist at /";
        assert_eq!(maybe_group.err().unwrap().to_string(), EXPECTED_ERR_MSG);
    }

    #[test]
    fn open_nonexistant_dataset() {
        let file = OneTempFile::new("open_nonexistant_dataset");
        let maybe_dataset = file.get_dataset("non_existant_dataset");

        assert!(maybe_dataset.is_err());

        const EXPECTED_ERR_MSG: &str = "HDF5 Error: H5Dopen2(): unable to synchronously open dataset: object 'non_existant_dataset' doesn't exist at /";
        assert_eq!(maybe_dataset.err().unwrap().to_string(), EXPECTED_ERR_MSG);
    }

    #[test]
    fn open_nonexistant_nested_dataset() {
        let file = OneTempFile::new("open_nonexistant_nested_dataset");
        let group = file
            .get_group_or_create_new("my_group", "my_class")
            .unwrap();
        let maybe_subgroup = group.get_dataset("my_subgroup");

        assert!(maybe_subgroup.is_err());

        const EXPECTED_ERR_MSG: &str = "HDF5 Error: H5Dopen2(): unable to synchronously open dataset: object 'my_subgroup' doesn't exist at /my_group";
        assert_eq!(maybe_subgroup.err().unwrap().to_string(), EXPECTED_ERR_MSG);
    }

    #[test]
    fn open_nonexistant_attribute() {
        let file = OneTempFile::new("open_nonexistant_attribute");
        let maybe_dataset = file.get_attribute("non_existant_attribute");

        assert!(maybe_dataset.is_err());

        const EXPECTED_ERR_MSG: &str = "HDF5 Error: H5Aopen(): unable to synchronously open attribute: can't locate attribute: 'non_existant_attribute' at /";
        assert_eq!(maybe_dataset.err().unwrap().to_string(), EXPECTED_ERR_MSG);
    }
}
