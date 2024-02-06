# trace-archiver

Simple tool to save all received trace messages to HDF5 files in a given directory.
Useful for diagnostics.

A file is created for each received trace message and saved in a file named
using the format `frame_{timestamp}_{digitizer_id}_{frame_number}.h5`.

The structure of the HDF5 file is as follows:
```
.
|- metadata
|  |- frame_timestamp
|  |  |- seconds
|  |  |- nanoseconds
|  |- digitizer_id
|  |- frame_number
|  |- channel_numbers   [n channels]
|- channel_data         [n channels, n time points]
```
