use qust_ds::prelude::*;
use qust_derive::*;
use std::collections::HashSet;
// use crate::prelude::{vol_pms, ori};

pub type TsigRes = (Vec<Open>, Vec<Exit>);
pub type StpRes = (
    Vec<PosiWeight<Hold>>,
    Vec<PosiWeight<Open>>,
    Vec<PosiWeight<Exit>>,
);
pub type PtmRes = (Vec<NormHold>, Vec<NormOpen>, Vec<NormExit>);
pub struct PtmResState {
    pub ptm_res: PtmRes,
    pub state: NormHold,
    pub open_i: Option<usize>,
}
impl PtmResState {
    pub fn new(len: usize) -> Self {
        let h_norm = Vec::with_capacity(len);
        let o_norm = Vec::with_capacity(len);
        let e_norm = Vec::with_capacity(len);
        Self {
            ptm_res: (h_norm, o_norm, e_norm),
            state: NormHold::No,
            open_i: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Trading {
    Open,
    Exit,
}

/* #region Holdi */
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Dire {
    Lo,
    Sh,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Hold {
    Lo(usize),
    Sh(usize),
    No,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Open {
    Lo(usize),
    Sh(usize),
    No,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Exit {
    Lo(HashSet<usize>),
    Sh(HashSet<usize>),
    No,
}

impl Dire {
    pub fn open(&self, i: usize) -> Open {
        match self {
            Self::Lo => Open::Lo(i),
            Self::Sh => Open::Sh(i),
        }
    }
    pub fn exit(&self, i: HashSet<usize>) -> Exit {
        match self {
            Self::Lo => Exit::Lo(i),
            Self::Sh => Exit::Sh(i),
        }
    }
}

use std::ops::Not;
impl Not for Dire {
    type Output = Dire;
    fn not(self) -> Self::Output {
        match self {
            Dire::Lo => Dire::Sh,
            Dire::Sh => Dire::Lo,
        }
    }
}

impl Hold {
    pub fn add_hold(&self, y: &Hold) -> Hold {
        match (self, y) {
            (Hold::No, Hold::No) => Hold::No,
            (Hold::Lo(i), Hold::No) => Hold::Lo(*i),
            (Hold::Sh(i), Hold::No) => Hold::Sh(*i),
            (Hold::No, Hold::Lo(i)) => Hold::Lo(*i),
            (Hold::No, Hold::Sh(i)) => Hold::Sh(*i),
            (Hold::Lo(i), Hold::Lo(_j)) => Hold::Lo(*i),
            (Hold::Sh(i), Hold::Sh(_j)) => Hold::Sh(*i),
            (Hold::Lo(_i), Hold::Sh(_j)) => Hold::No,
            (Hold::Sh(_i), Hold::Lo(_j)) => Hold::No,
        }
    }
    pub fn add_open(&self, y: &Open) -> (Hold, Open) {
        match (self, y) {
            (Hold::No, Open::No) => (Hold::No, Open::No),
            (Hold::Lo(i), Open::No) => (Hold::Lo(*i), Open::No),
            (Hold::Sh(i), Open::No) => (Hold::Sh(*i), Open::No),
            (Hold::No, Open::Lo(i)) => (Hold::Lo(*i), Open::Lo(*i)),
            (Hold::No, Open::Sh(i)) => (Hold::Sh(*i), Open::Sh(*i)),
            (Hold::Lo(i), Open::Lo(_j)) => (Hold::Lo(*i), Open::No),
            (Hold::Sh(i), Open::Sh(_j)) => (Hold::Sh(*i), Open::No),
            _ => (Hold::No, Open::No),
        }
    }
    pub fn add_exit(&self, y: &Exit) -> (Hold, Exit) {
        match (self, y) {
            (Hold::No, Exit::No) => (Hold::No, Exit::No),
            (Hold::Lo(i), Exit::No) => (Hold::Lo(*i), Exit::No),
            (Hold::Sh(i), Exit::No) => (Hold::Sh(*i), Exit::No),
            (Hold::Lo(i), Exit::Sh(j)) => {
                if j.contains(i) {
                    let mut exit_i = HashSet::new();
                    exit_i.insert(*i);
                    (Hold::No, Exit::Sh(exit_i))
                } else {
                    (Hold::Lo(*i), Exit::No)
                }
            }
            (Hold::Sh(i), Exit::Lo(j)) => {
                if j.contains(i) {
                    let mut exit_i = HashSet::new();
                    exit_i.insert(*i);
                    (Hold::No, Exit::Lo(exit_i))
                } else {
                    (Hold::Sh(*i), Exit::No)
                }
            }
            (_, _) => (Hold::No, Exit::No),
        }
    }
}

impl Open {
    pub fn add_open(&self, y: &Open) -> Open {
        match (self, y) {
            (Open::Lo(i), Open::No) => Open::Lo(*i),
            (Open::Sh(i), Open::No) => Open::Sh(*i),
            (Open::No, Open::Lo(i)) => Open::Lo(*i),
            (Open::No, Open::Sh(i)) => Open::Sh(*i),
            (Open::Lo(_i), Open::Sh(_j)) => Open::No,
            (Open::Sh(_i), Open::Lo(_j)) => Open::No,
            (_, _) => Open::No,
        }
    }
}

impl Exit {
    pub fn add_exit(&self, y: &Exit) -> Exit {
        match (self, y) {
            (Exit::No, Exit::No) => Exit::No,
            (Exit::Lo(i), Exit::No) => Exit::Lo(i.clone()),
            (Exit::Sh(i), Exit::No) => Exit::Sh(i.clone()),
            (Exit::No, Exit::Lo(i)) => Exit::Lo(i.clone()),
            (Exit::No, Exit::Sh(i)) => Exit::Sh(i.clone()),
            (Exit::Lo(_i), Exit::Sh(_j)) => Exit::No,
            (Exit::Sh(_i), Exit::Lo(_j)) => Exit::No,
            (_, _) => Exit::No,
        }
    }
}
/* #endregion */

/* #region NormHold */
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub enum NormHold {
    Lo(f32),
    Sh(f32),
    #[default]
    No,
}
#[derive(Debug, Clone, PartialEq)]
pub enum NormOpen {
    Lo(f32),
    Sh(f32),
    No,
}
#[derive(Debug, Clone, PartialEq)]
pub enum NormExit {
    Lo(f32),
    Sh(f32),
    No,
}

impl NormHold {
    pub fn add_norm_hold(&self, y: &NormHold) -> NormHold {
        match (self, y) {
            (NormHold::No, NormHold::No) => NormHold::No,
            (NormHold::Lo(i), NormHold::No) => NormHold::Lo(*i),
            (NormHold::Sh(i), NormHold::No) => NormHold::Sh(*i),
            (NormHold::No, NormHold::Lo(i)) => NormHold::Lo(*i),
            (NormHold::No, NormHold::Sh(i)) => NormHold::Sh(*i),
            (NormHold::Lo(i), NormHold::Sh(j)) => {
                let res = i - j;
                if res > 0f32 {
                    NormHold::Lo(res)
                } else {
                    NormHold::Sh(-res)
                }
            }
            (NormHold::Sh(i), NormHold::Lo(j)) => {
                let res = i - j;
                if res > 0f32 {
                    NormHold::Sh(res)
                } else {
                    NormHold::Lo(-res)
                }
            }
            (NormHold::Lo(i), NormHold::Lo(j)) => NormHold::Lo(i + j),
            (NormHold::Sh(i), NormHold::Sh(j)) => NormHold::Sh(i + j),
        }
    }

    pub fn sub_norm_hold(&self, y: &NormHold) -> (NormOpen, NormExit) {
        match (self, y) {
            (NormHold::No, NormHold::No) => (NormOpen::No, NormExit::No),
            (NormHold::Lo(i), NormHold::No) => (NormOpen::Lo(*i), NormExit::No),
            (NormHold::Sh(i), NormHold::No) => (NormOpen::Sh(*i), NormExit::No),
            (NormHold::No, NormHold::Lo(i)) => (NormOpen::No, NormExit::Sh(*i)),
            (NormHold::No, NormHold::Sh(i)) => (NormOpen::No, NormExit::Lo(*i)),
            (NormHold::Lo(i), NormHold::Lo(j)) => {
                let res = i - j;
                if res > 0. {
                    (NormOpen::Lo(res), NormExit::No)
                } else {
                    (NormOpen::No, NormExit::Sh(-res))
                }
            }
            (NormHold::Sh(i), NormHold::Sh(j)) => {
                let res = i - j;
                if res > 0. {
                    (NormOpen::Sh(res), NormExit::No)
                } else {
                    (NormOpen::No, NormExit::Lo(-res))
                }
            }
            (NormHold::Lo(i), NormHold::Sh(j)) => (NormOpen::Lo(*i), NormExit::Lo(*j)),
            (NormHold::Sh(i), NormHold::Lo(j)) => (NormOpen::Sh(*i), NormExit::Lo(*j)),
        }
    }
}

use std::ops::Mul;
impl Mul<f32> for &NormHold {
    type Output = NormHold;
    fn mul(self, rhs: f32) -> Self::Output {
        match *self {
            NormHold::Lo(i) => NormHold::Lo(i * rhs),
            NormHold::Sh(i) => NormHold::Sh(i * rhs),
            NormHold::No => NormHold::No,
        }
    }
}

impl NormOpen {
    pub fn add_norm_open(&self, y: &NormOpen) -> NormOpen {
        match (self, y) {
            (NormOpen::No, NormOpen::No) => NormOpen::No,
            (NormOpen::Lo(i), NormOpen::No) => NormOpen::Lo(*i),
            (NormOpen::Sh(i), NormOpen::No) => NormOpen::Sh(*i),
            (NormOpen::No, NormOpen::Lo(i)) => NormOpen::Lo(*i),
            (NormOpen::No, NormOpen::Sh(i)) => NormOpen::Sh(*i),
            (NormOpen::Lo(i), NormOpen::Lo(j)) => NormOpen::Lo(i + j),
            (NormOpen::Sh(i), NormOpen::Sh(j)) => NormOpen::Sh(i + j),
            (NormOpen::Lo(i), NormOpen::Sh(j)) => {
                let res = i - j;
                if res > 0f32 {
                    NormOpen::Lo(res)
                } else {
                    NormOpen::Sh(res)
                }
            }
            (NormOpen::Sh(i), NormOpen::Lo(j)) => {
                let res = j - i;
                if res > 0f32 {
                    NormOpen::Lo(res)
                } else {
                    NormOpen::Sh(res)
                }
            }
        }
    }
}

impl NormExit {
    pub fn add_norm_exit(&self, y: &NormExit) -> NormExit {
        match (self, y) {
            (NormExit::No, NormExit::No) => NormExit::No,
            (NormExit::Lo(i), NormExit::No) => NormExit::Lo(*i),
            (NormExit::Sh(i), NormExit::No) => NormExit::Sh(*i),
            (NormExit::No, NormExit::Lo(i)) => NormExit::Lo(*i),
            (NormExit::No, NormExit::Sh(i)) => NormExit::Sh(*i),
            (NormExit::Lo(i), NormExit::Lo(j)) => NormExit::Lo(i + j),
            (NormExit::Sh(i), NormExit::Sh(j)) => NormExit::Sh(i + j),
            (NormExit::Lo(i), NormExit::Sh(j)) => {
                let res = i - j;
                if res > 0f32 {
                    NormExit::Lo(res)
                } else {
                    NormExit::Sh(res)
                }
            }
            (NormExit::Sh(i), NormExit::Lo(j)) => {
                let res = j - i;
                if res > 0f32 {
                    NormExit::Lo(res)
                } else {
                    NormExit::Sh(res)
                }
            }
        }
    }
}

pub trait ToNorm<T> {
    fn to_norm(&self) -> T;
}
impl ToNorm<NormHold> for Hold {
    fn to_norm(&self) -> NormHold {
        match *self {
            Hold::Lo(_i) => NormHold::Lo(1.0),
            Hold::Sh(_i) => NormHold::Sh(1.0),
            Hold::No => NormHold::No,
        }
    }
}
impl ToNorm<NormOpen> for Open {
    fn to_norm(&self) -> NormOpen {
        match *self {
            Open::Lo(_i) => NormOpen::Lo(1.0),
            Open::Sh(_i) => NormOpen::Sh(1.0),
            Open::No => NormOpen::No,
        }
    }
}
impl ToNorm<NormExit> for Exit {
    fn to_norm(&self) -> NormExit {
        match self {
            Exit::Lo(i) => NormExit::Lo(i.len() as f32),
            Exit::Sh(i) => NormExit::Sh(i.len() as f32),
            Exit::No => NormExit::No,
        }
    }
}

pub trait ToNum {
    fn to_num(&self) -> f32;
}

impl ToNum for NormHold {
    fn to_num(&self) -> f32 {
        match *self {
            NormHold::Lo(i) => i,
            NormHold::Sh(i) => -i,
            NormHold::No => 0.,
        }
    }
}

impl ToNum for NormOpen {
    fn to_num(&self) -> f32 {
        match *self {
            NormOpen::Lo(i) => i,
            NormOpen::Sh(i) => -i,
            NormOpen::No => 0.,
        }
    }
}

impl ToNum for NormExit {
    fn to_num(&self) -> f32 {
        match *self {
            NormExit::Lo(i) => i,
            NormExit::Sh(i) => -i,
            NormExit::No => 0.,
        }
    }
}

/* #endregion */

/* #region  Open Ing */
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OpenIng {
    Lo(usize),
    Sh(usize),
}

impl OpenIng {
    pub fn inner_i(&self) -> usize {
        match self {
            Self::Lo(i) => *i,
            Self::Sh(i) => *i,
        }
    }
}

impl Dire {
    pub fn open_ing(&self, i: usize) -> OpenIng {
        match self {
            Self::Lo => OpenIng::Lo(i),
            Self::Sh => OpenIng::Sh(i),
        }
    }
}
/* #endregion */

/* #region PowiWeight */
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PosiWeight<T>(pub T, pub f32);

impl ToNorm<NormHold> for PosiWeight<Hold> {
    fn to_norm(&self) -> NormHold {
        if self.1 == 0. {
            return NormHold::No;
        }
        match self.0 {
            Hold::Lo(_i) => NormHold::Lo(1.0 * self.1),
            Hold::Sh(_i) => NormHold::Sh(1.0 * self.1),
            Hold::No => NormHold::No,
        }
    }
}
impl ToNorm<NormOpen> for PosiWeight<Open> {
    fn to_norm(&self) -> NormOpen {
        if self.1 == 0. {
            return NormOpen::No;
        }
        match &self.0 {
            Open::Lo(_i) => NormOpen::Lo(1.0 * self.1),
            Open::Sh(_i) => NormOpen::Sh(1.0 * self.1),
            Open::No => NormOpen::No,
        }
    }
}
impl ToNorm<NormExit> for PosiWeight<Exit> {
    fn to_norm(&self) -> NormExit {
        if self.1 == 0. {
            return NormExit::No;
        }
        match &self.0 {
            Exit::Lo(i) => NormExit::Lo(i.len() as f32 * self.1),
            Exit::Sh(i) => NormExit::Sh(i.len() as f32 * self.1),
            Exit::No => NormExit::No,
        }
    }
}
/* #endregion */

use crate::trade::di::Di;
use dyn_clone::{clone_trait_object, DynClone};

pub type PosiFunc<'a> = Box<dyn Fn(&NormHold, usize) -> NormHold + 'a>;

#[typetag::serde(tag = "Money")]
pub trait Money: DynClone + Send + Sync + std::fmt::Debug + 'static {
    fn register<'a>(&'a self, di: &'a Di) -> PosiFunc<'a>;
    fn get_init_weight(&self) -> f32 {
        1.
    }
    fn change_weight(&self, weight: f32) -> Box<dyn Money>;
}
clone_trait_object!(Money);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct M1(pub f32);

#[typetag::serde]
impl Money for M1 {
    fn register<'a>(&'a self, _di: &'a Di) -> PosiFunc<'a> {
        Box::new(move |x, _y| x * self.0)
    }
    fn get_init_weight(&self) -> f32 {
        self.0
    }
    fn change_weight(&self, weight: f32) -> Box<dyn Money> {
        Box::new(M1(self.0 * weight))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct M2(pub f32);

#[typetag::serde]
impl Money for M2 {
    fn register<'a>(&'a self, di: &'a Di) -> PosiFunc<'a> {
        let c = di.c();
        let pv = di.pcon.ticker.info().pv;
        let multi = self.0 / pv;
        Box::new(move |x, y| x * (multi / c[y]))
    }
    fn change_weight(&self, weight: f32) -> Box<dyn Money> {
        Box::new(M2(self.0 * weight))
    }
}

#[ta_derive]
pub struct M3(pub f32);

#[typetag::serde]
impl Money for M3 {
    fn register<'a>(&'a self, di: &'a Di) -> PosiFunc<'a> {
        let c = di.c();
        let pv = di.pcon.ticker.info().pv;
        let multi = self.0 / pv;
        let vol = di.calc(crate::prelude::vol_pms.clone())[0].clone();
        Box::new(move |x, y| {
            let v = &vol[y];
            if v.is_nan() {
                NormHold::No
            } else {
                x * (multi / c[y] / v)
            }
        })
    }
    fn change_weight(&self, weight: f32) -> Box<dyn Money> {
        Box::new(M3(self.0 * weight))
    }
}

impl Mul<f32> for Box<dyn Money> {
    type Output = Box<dyn Money>;
    fn mul(self, rhs: f32) -> Self::Output {
        self.change_weight(rhs)
    }
}
