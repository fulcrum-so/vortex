use std::fmt::{Display, Formatter};

pub use localdatetime::*;
use vortex_error::VortexResult;

use crate::serde::BytesSerde;

mod localdatetime;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum TimeUnit {
    Ns,
    Us,
    Ms,
    S,
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

impl BytesSerde for TimeUnit {
    fn serialize(&self) -> Vec<u8> {
        vec![*self as u8]
    }

    fn deserialize(data: &[u8]) -> VortexResult<Self> {
        match data[0] {
            0x00 => Ok(TimeUnit::Ns),
            0x01 => Ok(TimeUnit::Us),
            0x02 => Ok(TimeUnit::Ms),
            0x03 => Ok(TimeUnit::S),
            _ => Err("Unknown timeunit variant".into()),
        }
    }
}
