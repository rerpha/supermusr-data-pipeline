use std::path::{Path, PathBuf};

fn main() {
    let schema_dir = Path::new("../schemas/");

    let target_dir: PathBuf = std::env::var_os("OUT_DIR")
        .expect("OUT_DIR should be set")
        .into();
    let target_dir = target_dir.join("flatbuffer_generated");

    let inputs = [
        "aev2_frame_assembled_event_v2.fbs",
        "dat2_digitizer_analog_trace_v2.fbs",
        "dev2_digitizer_event_v2.fbs",
        "frame_metadata_v2.fbs",
        "ecs_6s4t_run_stop.fbs",
        "ecs_df12_det_spec_map.fbs",
        "ecs_pl72_run_start.fbs",
        "ecs_f144_logdata.fbs",
        "ecs_se00_data.fbs",
        "ecs_al00_alarm.fbs",
        "ecs_ev44_events.fbs",
        "dtdb_adc_pulse_debug.fbs",
    ];
    let inputs: Vec<PathBuf> = inputs.iter().map(|i| schema_dir.join(i)).collect();
    let inputs: Vec<&Path> = inputs.iter().map(|i| i.as_path()).collect();

    flatc_rust::run(flatc_rust::Args {
        inputs: &inputs,
        out_dir: &target_dir,
        ..Default::default()
    })
    .expect("flatbuffer schemas should be compiled");
}
