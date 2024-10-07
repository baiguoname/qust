#![allow(non_upper_case_globals, non_camel_case_types)]

pub mod input {
    pub mod ticks;
    pub mod read_csv;
}

pub mod output {
    pub mod excel;
    pub mod plot;
    pub mod profile;
    pub mod array;
    pub mod color;
    pub mod show;
}

pub mod prelude {
    pub use crate::{
        input::{ ticks::*, read_csv::* },
        output::{
            excel::{IntoDf, ToIndex, ToValue, ToValueString, ToCsv, WithDi, ConcatDf},
            plot::*,
            profile::*,
            array::*,
            show::*,
        }
    };

}

#[macro_use]
extern crate lazy_static;