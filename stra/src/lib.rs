#![allow(non_upper_case_globals, non_camel_case_types)]
pub mod inters {
    pub mod inter_t;
    pub mod trivol;
}
pub mod c25;
pub mod c66;
pub mod c71;
pub mod cqqq;
pub mod para;
pub mod macd;
pub mod econd;
pub mod version;
pub mod teststra;
pub mod f20;
pub mod filter;
pub mod s3;
pub mod cstras;
pub mod ofs {
    pub mod of;
    // pub mod of30min;
    pub mod ofpro;
    pub mod oftick;
}
pub mod c71inter;
pub mod c25ops;
pub mod p01;
pub mod p02;
pub mod p03;
pub mod p05;

// #[allow(unused_variables, dead_code, unused_imports)]
// pub mod examples {
//     pub mod example1;
//     pub mod example2;
//     pub mod example3;
//     pub mod example4;
//     pub mod example5;
//     pub mod example6;
//     pub mod example7;
//     pub mod example8;
//     pub mod example9;
//     pub mod example10;
//     pub mod example11;
// }

pub mod prelude {
    pub use crate::{
        inters::inter_t::*,
        ofs::*,
        c25,
        c66,
        c71,
        cqqq,
        para,
        macd,
        econd::*,
        // version::*,
        teststra,
        f20,
        filter::*,
        s3,
        cstras::*,
        c71inter,
        p01,
        p02,
        p03,
    };
}

#[macro_use]
extern crate lazy_static;