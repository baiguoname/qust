use crate::idct::pms::Pms;
use crate::sig::posi::Dire;
use crate::trade::di::Di;
use chrono::Timelike;
use qust_ds::prelude::*;
use qust_derive::*;
use dyn_clone::{clone_trait_object, DynClone};
use std::ops::{BitAnd, BitOr, Not};

// pub type LoopSig<'a> = Box<dyn Fn(usize, usize) -> bool + Send + Sync + 'a>;
pub type LoopSig<'a> = Box<dyn Fn(usize, usize) -> bool + 'a>;

/* #region cond trait */
#[clone_trait]
pub trait Cond {
    fn calc_di(&self, _di: &Di) -> avv32 {
        Default::default()
    }
    fn calc_da<'a>(&self, _data: avv32, _di: &'a Di) -> LoopSig<'a> {
        Box::new(move |_e, _o| true)
    }
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data = self.calc_di(di);
        self.calc_da(data, di)
    }
    fn update(&mut self, _di: &Di) {}
    fn to_box(&self) -> Box<dyn Cond>
    where
        Self: Sized,
    {
        dyn_clone::clone_box(self)
    }
}

/* #endregion */

/* #region filter day */
#[ta_derive]
#[derive(PartialEq, Eq)]
pub struct Filterday;

#[typetag::serde]
impl Cond for Filterday {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data: Vec<tt> = di
            .t()
            .iter()
            .map(|x| tt::from_hms_opt(x.hour(), x.minute(), x.second()).unwrap())
            .collect();
        Box::new(move |e: usize, _o: usize| data[e] >= tt::from_hms_opt(14, 56, 50).unwrap())
    }
}

/* #endregion */

/* #region msig */
#[ta_derive]
#[derive(PartialEq, Eq, Copy)]
pub enum LogicOps {
    And,
    Or,
}

impl LogicOps {
    pub fn evaluate(self, x: bool, y: bool) -> bool {
        match self {
            LogicOps::And => x && y,
            LogicOps::Or => x || y,
        }
    }
}

#[ta_derive]
pub struct MsigType<T>(pub LogicOps, pub T, pub T);
pub type Msig = MsigType<CondBox>;

#[typetag::serde]
impl Cond for Msig {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let f = self.0;
        let cond1 = self.1.cond(di);
        let cond2 = self.2.cond(di);
        Box::new(move |e, o| f.evaluate(cond1(e, o), cond2(e, o)))
    }
}
/* #endregion */

/* #region  Iocond*/
#[ta_derive]
pub struct Iocond {
    pub pms: Pms,
    pub range: std::ops::Range<f32>,
}

#[typetag::serde]
impl Cond for Iocond {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data = di.calc(&self.pms)[0].clone();
        let range = self.range.clone();
        Box::new(move |e, _o| range.contains(&data[e]))
    }
}
/* #endregion */

pub trait CondLoop: Send + Sync + 'static {
    fn cond<'a>(&self, di: &'a Di) -> Box<dyn FnMut(usize) -> f32 + 'a>;
}

/* #region FilterdayTime */
#[ta_derive]
pub struct FilterdayTime(pub ForCompare<tt>);

#[typetag::serde]
impl Cond for FilterdayTime {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data = di.t();
        let f = self.0.clone();
        Box::new(move |e: usize, _o: usize| f.compare_time(&data[e]))
    }
}

/* #endregion */

impl BitAnd<Box<dyn Cond>> for Box<dyn Cond> {
    type Output = Box<dyn Cond>;
    fn bitand(self, rhs: Self) -> Self::Output {
        MsigType(LogicOps::And, self, rhs).to_box()
    }
}
impl BitOr<Box<dyn Cond>> for Box<dyn Cond> {
    type Output = Box<dyn Cond>;
    fn bitor(self, rhs: Self) -> Self::Output {
        MsigType(LogicOps::Or, self, rhs).to_box()
    }
}
impl<T: Cond + Clone> BitAnd<T> for Box<dyn Cond> {
    type Output = CondBox;
    fn bitand(self, rhs: T) -> Self::Output {
        MsigType(LogicOps::And, self, rhs.cond_box()).cond_box()
    }
}
impl<T: Cond + Clone> BitOr<T> for Box<dyn Cond> {
    type Output = CondBox;
    fn bitor(self, rhs: T) -> Self::Output {
        MsigType(LogicOps::Or, self, rhs.cond_box()).cond_box()
    }
}

#[ta_derive]
pub enum BandState {
    Action,
    Lieing,
}

#[ta_derive]
pub struct BandCond<T>(pub Dire, pub BandState, pub T);

#[typetag::serde]
impl Cond for BandCond<Pms> {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data = di.calc(&self.2);
        match data.len() {
            3 => {
                let (down_band, mid_band, up_band) =
                    (data[0].clone(), data[1].clone(), data[2].clone());
                match (&self.0, &self.1) {
                    (Dire::Lo, BandState::Lieing) => {
                        Box::new(move |e, _o| mid_band[e] > up_band[e])
                    }
                    (Dire::Sh, BandState::Lieing) => {
                        Box::new(move |e, _o| mid_band[e] < down_band[e])
                    }
                    (Dire::Lo, BandState::Action) => Box::new(move |e, _o| {
                        e >= 1 && mid_band[e] >= up_band[e] && mid_band[e - 1] < up_band[e - 1]
                    }),
                    (Dire::Sh, BandState::Action) => Box::new(move |e, _o| {
                        e >= 1 && mid_band[e] <= down_band[e] && mid_band[e - 1] > down_band[e - 1]
                    }),
                }
            }
            1 => {
                let mid_band = data[0].clone();
                match (&self.0, &self.1) {
                    (Dire::Lo, BandState::Lieing) => Box::new(move |e, _o| mid_band[e] > 0f32),
                    (Dire::Sh, BandState::Lieing) => Box::new(move |e, _o| mid_band[e] < 0f32),
                    (Dire::Lo, BandState::Action) => Box::new(move |e, _o| {
                        e >= 1 && mid_band[e] >= 0f32 && mid_band[e - 1] < 0f32
                    }),
                    (Dire::Sh, BandState::Action) => Box::new(move |e, _o| {
                        e >= 1 && mid_band[e] <= 0f32 && mid_band[e - 1] > 0f32
                    }),
                }
            }
            _ => panic!("no impelemented"),
        }
    }
}

#[ta_derive]
pub struct CrossCond(pub Dire, pub Pms);

#[typetag::serde]
impl Cond for CrossCond {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data = di.calc(&self.1);
        let short_line = data[0].clone();
        let long_line = data[1].clone();
        match self.0 {
            Dire::Lo => Box::new(move |e, _o| {
                e > 0 && short_line[e - 1] < long_line[e - 1] && short_line[e] >= long_line[e]
            }),
            Dire::Sh => Box::new(move |e, _o| {
                e > 0 && short_line[e - 1] > long_line[e - 1] && short_line[e] <= long_line[e]
            }),
        }
    }
}

#[ta_derive]
pub struct NotCond {
    pub cond: CondBox,
}

#[typetag::serde]
impl Cond for NotCond {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let f = self.cond.cond(di);
        Box::new(move |e, o| !f(e, o))
    }
}

impl Not for CondBox {
    type Output = CondBox;
    fn not(self) -> Self::Output {
        NotCond { cond: self }.cond_box()
    }
}