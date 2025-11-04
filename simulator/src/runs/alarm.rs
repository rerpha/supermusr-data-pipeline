use clap::ValueEnum;
use digital_muon_streaming_types::ecs_al00_alarm_generated::Severity;
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum SeverityLevel {
    Ok,
    Minor,
    Major,
    Invalid,
}

impl From<SeverityLevel> for Severity {
    fn from(source: SeverityLevel) -> Severity {
        match source {
            SeverityLevel::Ok => Severity::OK,
            SeverityLevel::Minor => Severity::MINOR,
            SeverityLevel::Major => Severity::MAJOR,
            SeverityLevel::Invalid => Severity::INVALID,
        }
    }
}
