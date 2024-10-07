#![allow(non_upper_case_globals, non_camel_case_types)]
pub mod func;
pub mod aa;
pub mod save;
pub mod roll;
pub mod types;
pub mod utils;
pub mod df;
pub mod log;
pub mod logger;

pub mod prelude {
    pub const estring: String = String::new();
    pub use super::{
        types::*,
        utils::*,
        roll::*,
        func::{*, ForCompare::*},
        save::*,
        aa::*,
        log::*,
        logger::*,
    };
    pub use itertools::{izip, Itertools};
    pub use serde::{Serialize, Deserialize};
    pub use crate::t;
}