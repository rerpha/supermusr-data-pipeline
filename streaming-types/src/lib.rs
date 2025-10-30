mod frame_metadata;
pub mod time_conversions;

pub use crate::frame_metadata::FrameMetadata;
pub use flatbuffers;

macro_rules! schema {
    ($name:ident) => {
        pub mod $name {
            #![allow(
                non_camel_case_types,
                unsafe_op_in_unsafe_fn,
                unused_imports,
                clippy::derivable_impls,
                clippy::extra_unused_lifetimes,
                clippy::missing_safety_doc,
                clippy::unnecessary_cast,
                clippy::unwrap_used
            )]

            include!(concat!(
                env!("OUT_DIR"),
                "/flatbuffer_generated/",
                stringify!($name),
                ".rs"
            ));
        }
    };
}

schema!(frame_metadata_v2_generated);
schema!(aev2_frame_assembled_event_v2_generated);
schema!(dat2_digitizer_analog_trace_v2_generated);
schema!(dev2_digitizer_event_v2_generated);

schema!(ecs_6s4t_run_stop_generated);
schema!(ecs_al00_alarm_generated);
schema!(ecs_df12_det_spec_map_generated);
schema!(ecs_f144_logdata_generated);
schema!(ecs_pl72_run_start_generated);
schema!(ecs_se00_data_generated);
schema!(ecs_ev44_events_generated);
schema!(dtdb_adc_pulse_debug_generated);
