# diagnostics

A debugging tool for reporting diagnostics. This tool can be run in one of two modes: DAQ Trace (`daq-trace`) or Message Debug (`message-debug`).

## DAQ Trace Mode

A simple TUI tool that listens on the trace topic and reports in a table view the following for each DAQ that is seen to be sending messages:

- Number of messages received
- Timestamp of first message received since starting kafka-daq-stats
- Timestamp of last message received
- Frame number of the last message received
- Status of the number of channels:
   - Number of channels present in the last message received
   - A flag indicating if the number of channels has ever changed. The flag reports `stable` if the number of channels has never changed, and `unstable` if the number has changed at all whilst the tool has been running.
- Status of the number of samples:
   - Number of samples in the first channel of the last message received
   - A flag indicating if the number of samples is not identical in each channel. The flag reports `all channels` if all channels have the same number of samples, and `first channel only` if there are (or ever have been) differences in the number of samples.
   - A flag indicating if the number of samples has ever changed. The flag reports `stable` if the number of samples has never changed, and `unstable` if the number has changed at all whilst the tool has been running.
- Number of "bad" frames detected (i.e. frames with malformed timestamps).
- Status of the samples of a single channel:
   - The current index of the channel to monitor (this can be changed for each digitiser by the left/right key)
   - The channel id of the currently monitored channel
   - The range of samples values of the currently monitored channel (note the index/id is displayed as `index:id` with sample range as `(min:max)` beneath)

### Running in Podman

Please see the appropriate [documentation](https://podman.io/docs/installation) for installing Podman on your system.

#### Installation on Windows

When using a Windows system, please see [here](https://github.com/containers/podman/blob/main/docs/tutorials/podman-for-windows.md). Note that the Windows installation requires WSL.

- Download the [latest release](https://github.com/containers/podman/releases/latest) for Windows, and follow the installation instructions.
- In Powershell run `podman machine init` to download and set up virtual machine.
- When the above process is complete run `podman machine start`.
- To shut down the Podman VM run `podman machine stop`

Some issues may be caused by running Podman in rootless mode. To change to rootful mode run `podman machine set --rootful`. If a restart is required run `podman machine stop`, then `podman machine start`.

### Running

To run using Podman, execute the following command, substituting the broker, topic, and group arguments as appropriate.

```shell
podman run --rm -it \
    ghcr.io/isisneutronmuon/supermusr-diagnostics:main \
    daq-trace
    --broker 130.246.55.29:9092 \
    --topic daq-traces-in  \
    --group vis-3
```

## Message Debug Mode

Simple message dumping tool which parses kafka messages and logs the result.
