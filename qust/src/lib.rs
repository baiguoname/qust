#![allow(non_upper_case_globals, non_camel_case_types)]
pub mod trade {
    pub mod di;
    pub mod idx;
    pub mod inter;
    pub mod ticker;
    pub(crate) mod version;

    pub mod prelude {
        pub use super::{di::*, idx::*, inter::*, ticker::*};
    }
}

pub mod idct {
    pub mod calc;
    pub mod dcon;
    pub mod fore;
    pub mod macros;
    pub mod part;
    pub mod pms;
    pub mod ta;

    pub mod prelude {
        pub use super::{
            calc::*,
            dcon::{Convert::*, *},
            fore::*,
            part::*,
            pms::*,
            ta::{Max as maxta, Min as minta, *},
        };
    }
}

pub mod sig {
    pub mod bt;
    pub mod cond;
    pub mod distra;
    pub mod livesig;
    pub mod pnl;
    pub mod posi;

    pub mod prelude {
        pub use super::{
            bt::*,
            cond::*,
            distra::*,
            livesig::*,
            pnl::*,
            posi::{Dire::*, *},
        };
        pub const and: LogicOps = LogicOps::And;
        pub const or: LogicOps = LogicOps::Or;
    }
}

pub mod live {
    pub mod order_types;
    pub mod live_ops;
    pub mod match_ops;
    pub mod algo;
    pub mod thread_manger;
    pub mod live_run;
    pub mod cross;
    pub mod trend;
    pub mod bt;

    pub mod prelude {
        pub use super::{
            bt::*,
            order_types::*,
            live_ops::*,
            match_ops::*,
            algo::*,
            live_run::*,
            cross::prelude::*,
            trend::prelude::*,
        };
    }
}

pub mod std_prelude {
    pub use std::{
        fmt::Debug,
        ops::{Add, Div, Mul, Sub},
        path::Path,
        sync::{Arc, Condvar, Mutex, RwLock},
        thread::{self, sleep, spawn},
        time::{Duration as dura, Instant},
    };
}

pub mod prelude {
    pub use crate::{
        gen_inter, 
        idct::prelude::*, 
        msig, 
        sig::prelude::*, 
        trade::prelude::*, 
        loge,
        live::prelude::*,
    };
    pub use qust_ds::prelude::*;
    pub use serde::{Deserialize, Serialize};
    pub const pct: RollTa<KlineType> = RollTa(KlineType::Close, RollFunc::Momentum, RollOps::N(2));
    pub const ono: Part = Part::ono;
    pub const oos: Part = Part::oos;
    pub const ori: Convert = Tf(0, 1);
    pub const vori: FillCon = FillCon(ori);
    pub const rank_day: Rank = Rank(400, 100);
    pub const ha: Convert = Ha(4);
    pub const cs1: CommSlip = CommSlip(1., 1.);
    pub const cs2: CommSlip = CommSlip(1., 0.);
    pub const cs_hf: (CommSlip,) = (CommSlip(1., 0.2),);
    pub fn ta_max_n(n: usize) -> RollTa<KlineType> {
        RollTa(KlineType::High, RollFunc::Max, RollOps::N(n))
    }
    pub fn ta_min_n(n: usize) -> RollTa<KlineType> {
        RollTa(KlineType::Low, RollFunc::Min, RollOps::N(n))
    }
    lazy_static! {
        pub static ref tickers3: Vec<Ticker> = vec![
            SFer, SMer, ier, eger, ler, pper, ruer, ver, fuer, aer, aler, zner, OIer, per, RMer,
            mer, SRer, sper, eber, cuer, SAer
        ];
        pub static ref tickers4: Vec<Ticker> =
            vec![SFer, SMer, ier, eger, ler, pper, ruer, fuer, aler, OIer, per, eber, cuer, SAer];
        pub static ref tickers30m: Vec<Ticker> = vec![
            fuer, SAer, aler, scer, FGer, pger, ier, sner, hcer, auer, OIer, cuer, sper, SMer,
            TAer, cser, aer, SRer, eber, MAer, SFer, ver, CFer, per, rber, cer
        ];
        pub static ref calc_ticker: fn(&DiStra) -> InfoPnlRes<Ticker, da> =
            |distra: &DiStra| InfoPnlRes(
                distra.di.pcon.ticker,
                distra.di.pnl(&distra.stra.ptm, cs2).da()
            );
        pub static ref rlast: Box<dyn Pri> = {
            let x: Box<dyn Inter> = Box::new(Rlast);
            Box::new(x)
        };
        pub static ref dayk: Convert = Event(rlast.clone());
        pub static ref dayn: Pre = ori + dayk.clone() + ono;
        pub static ref vol_ta: RollTa<Box<dyn Ta>> =
            RollTa(Box::new(pct), RollFunc::Std, RollOps::InitMiss(20));
        pub static ref vol_pms: Pms = dayk.clone() + ono + vol_ta.clone() + vori;
        pub static ref m1: Box<dyn Money> = Box::new(M1(1.));
    }
}

#[macro_use]
extern crate lazy_static;