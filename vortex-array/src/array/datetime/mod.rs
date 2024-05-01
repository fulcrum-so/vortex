use std::fmt::{Display, Formatter};

use lazy_static::lazy_static;
pub use localdatetime::*;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::{Deserialize, Serialize};
use vortex_dtype::ExtMetadata;

mod localdatetime;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    IntoPrimitive,
    TryFromPrimitive,
)]
#[repr(u8)]
pub enum TimeUnit {
    Ns,
    Us,
    Ms,
    S,
}

lazy_static! {
    static ref METADATA_NS: ExtMetadata = ExtMetadata::from([TimeUnit::Ns.into()].as_ref());
    static ref METADATA_US: ExtMetadata = ExtMetadata::from([TimeUnit::Us.into()].as_ref());
    static ref METADATA_MS: ExtMetadata = ExtMetadata::from([TimeUnit::Ms.into()].as_ref());
    static ref METADATA_S: ExtMetadata = ExtMetadata::from([TimeUnit::S.into()].as_ref());
}

impl TimeUnit {
    pub fn metadata(&self) -> &ExtMetadata {
        match self {
            TimeUnit::Ns => &METADATA_NS,
            TimeUnit::Us => &METADATA_US,
            TimeUnit::Ms => &METADATA_MS,
            TimeUnit::S => &METADATA_S,
        }
    }
}

impl Display for TimeUnit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeUnit::Ns => write!(f, "ns"),
            TimeUnit::Us => write!(f, "us"),
            TimeUnit::Ms => write!(f, "ms"),
            TimeUnit::S => write!(f, "s"),
        }
    }
}
