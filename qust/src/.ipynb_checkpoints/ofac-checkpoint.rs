#![allow(unused_imports, dead_code)]
use crate::aa::lag;
use crate::cond::Cond;
use crate::di::{Di, Dil};
use crate::pnl::PnlRes;
use crate::types::*;
use itertools::izip;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::marker::{Send, Sync};
use std::thread;


pub trait SigClone<A>: Send + Sync {
    fn sig_clone(&self) -> Box<dyn Sig<A>>;
}

impl<T: Clone + Sig<A> + 'static, A> SigClone<A> for T {
    fn sig_clone(&self) -> Box<dyn Sig<A>> {
        Box::new(self.clone())
    }
}

impl<A> Clone for Box<dyn Sig<A>> {
    fn clone(&self) -> Self {
        self.sig_clone()
    }
}

pub trait Sig<A>: SigClone<A> {
    fn init(&self, di: &mut Di) -> A;
    fn di(&self, di: &mut Di) -> A;
}

/* #region Holdi */
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
    fn open(&self, i: usize) -> Open {
        match self {
            Self::Lo => Open::Lo(i),
            Self::Sh => Open::Sh(i),
        }
    }
    fn exit(&self, i: HashSet<usize>) -> Exit {
        match self {
            Self::Lo => Exit::Lo(i),
            Self::Sh => Exit::Sh(i),
        }
    }
}

impl Hold {
    fn add_hold(&self, y: &Hold) -> Hold {
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
    fn add_open(&self, y: &Open) -> (Hold, Open) {
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
    fn add_exit(&self, y: &Exit) -> (Hold, Exit) {
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
    fn add_open(&self, y: &Open) -> Open {
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
    fn add_exit(&self, y: &Exit) -> Exit {
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
#[derive(Debug, Clone, PartialEq)]
pub enum NormHold {
    Lo(f32),
    Sh(f32),
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
    fn add_norm_hold(&self, y: &NormHold) -> NormHold {
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
                    NormHold::Sh(res)
                }
            }
            (_, _) => NormHold::No,
        }
    }
}
impl NormOpen {
    fn add_norm_open(&self, y: &NormOpen) -> NormOpen {
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
    fn add_norm_exit(&self, y: &NormExit) -> NormExit {
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

trait ToNorm<T> {
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
            NormHold::No => 0f32,
        }
    }
}
/* #endregion */

/* #region Tsig */
#[derive(Clone, Serialize, Deserialize)]
pub struct Tsig<T, N> {
    pub o_dire: Dire,
    pub e_dire: Dire,
    pub o_sig: T,
    pub e_sig: N,
}

impl<T, N> Tsig<T, N> {
    pub fn new<K: Cond + Clone, M: Cond + Clone>(dire: Dire, o_sig: &K, e_sig: &M) -> Tsig<K, M> {
        let dire_ops = if let Dire::Lo = dire { Dire::Sh } else { Dire::Lo };
        Tsig {
            o_dire: dire,
            e_dire: dire_ops,
            o_sig: o_sig.clone(),
            e_sig: e_sig.clone()
        }
    }

    pub fn to_ptm(&self) -> Ptm1<Stp<Self>>
    where
        Self: Sig<TsigRes> + Clone + 'static,
    {
        let stp = Stp(self.clone());
        Ptm1(stp)
    }
    pub fn to_pnl(&self) -> Pnl 
    where
        Self: Sig<TsigRes> + Clone + 'static,
    {
        let ptm_box: PtmBox = Box::new(self.to_ptm());
        Pnl(ptm_box)
    }

    pub fn to_pnls<T2, N2>(&self, tsig: &Tsig<T2, N2>) -> Pnl
    where
        Self: Sig<TsigRes> + Clone + 'static,
        Tsig<T2, N2>: Sig<TsigRes> + Clone + 'static,
    {
        let stp1 = Stp(self.clone());
        let stp2 = Stp(tsig.clone());
        let ptm = Ptm2(stp1, stp2);
        let ptm_box: PtmBox = Box::new(ptm);
        Pnl(ptm_box)
    }
}

pub type TsigRes = (Vec<Open>, Vec<Exit>);


impl<T: Cond + Clone, N: Cond + Clone> Sig<TsigRes> for Tsig<T, N> {
    fn init(&self, di: &mut Di) -> TsigRes {
        di.calc_init(&di.last_dcon());
        self.o_sig.calc_init(di);
        self.e_sig.calc_init(di);
        let len = di.len();
        let o_vec: Vec<Open> = vec![Open::No; len];
        let e_vec: Vec<Exit> = vec![Exit::No; len];
        (o_vec, e_vec)
    }
    fn di(&self, di: &mut Di) -> TsigRes {
        let mut res = self.init(di);
        let time_vec = di.t();
        let o_fn = self.o_sig.cond(di);
        let e_fn = self.e_sig.cond(di);
        let mut hold_set: HashSet<usize> = HashSet::new();

        for i in 0..res.0.len() {
            if o_fn(i, i) {
                hold_set.insert(i);
                res.0[i] = self.o_dire.open(i);
            }
            let mut stop_set: HashSet<usize> = HashSet::new();
            for x in hold_set.iter() {
                let open_time = time_vec[*x];
                for j in (0..i).rev() {
                    if time_vec[j] <= open_time {
                        if e_fn(i, j) {
                            stop_set.insert(*x);
                        }
                        break;
                    }
                }
            }
            if !stop_set.is_empty() {
                for stop in stop_set.iter() {
                    hold_set.remove(stop);
                }
                res.1[i] = self.e_dire.exit(stop_set);
            }
        }
        res
    }
}
/* #endregion */

/* #region Stp */
#[derive(Clone)]
pub struct Stp<T>(pub T);
type StpRes = (Vec<Hold>, Vec<Open>, Vec<Exit>);
impl<T: Sig<TsigRes> + Clone + 'static> Sig<StpRes> for Stp<T> {
    fn init(&self, di: &mut Di) -> StpRes {
        let len = di.len();
        let h_res = vec![Hold::No; len];
        let o_res = vec![Open::No; len];
        let e_res = vec![Exit::No; len];
        (h_res, o_res, e_res)
    }
    fn di(&self, di: &mut Di) -> StpRes {
        let mut res = self.init(di);
        let oe_vec = self.0.di(di);
        let o_vec = oe_vec.0;
        let e_vec = oe_vec.1;
        for i in 0..o_vec.len() {
            let hold = if i > 1 { &res.0[i - 1] } else { &Hold::No };
            let hold_open = hold.add_open(&o_vec[i]);
            let hold = hold_open.0;
            let open = hold_open.1;
            let hold_exit = hold.add_exit(&e_vec[i]);
            let hold = hold_exit.0;
            let exit = hold_exit.1;
            res.0[i] = hold;
            res.1[i] = open;
            res.2[i] = exit;
        }
        res
    }
}
/* #endregion */

/* #region Ptm */
pub type PtmRes = (Vec<NormHold>, Vec<NormOpen>, Vec<NormExit>);
pub type PtmBox = Box<dyn Sig<PtmRes>>;
#[derive(Clone)]
pub struct Ptm1<T>(pub T);


impl<T: Sig<StpRes> + Clone + 'static> Sig<PtmRes> for Ptm1<T> {
    fn init(&self, di: &mut Di) -> PtmRes {
        let len = di.len();
        let h_norm = vec![NormHold::No; len];
        let o_norm = vec![NormOpen::No; len];
        let e_norm = vec![NormExit::No; len];
        (h_norm, o_norm, e_norm)
    }
    fn di(&self, di: &mut Di) -> PtmRes {
        let mut res = self.init(di);
        let sigr = self.0.di(di);
        for i in 0..di.len() {
            res.0[i] = sigr.0[i].to_norm();
            res.1[i] = sigr.1[i].to_norm();
            res.2[i] = sigr.2[i].to_norm();
        }
        res
    }
}

#[derive(Clone)]
pub struct Ptm2<T, N>(pub T, pub N);
impl<T: Sig<StpRes> + Clone + 'static, N: Sig<StpRes> + Clone + 'static> Sig<PtmRes> for Ptm2<T, N> {
    fn init(&self, _di: &mut Di) -> PtmRes {
        (vec![], vec![], vec![])
    }
    fn di(&self, di: &mut Di) -> PtmRes {
        let ptm1_1 = Ptm1(self.0.clone());
        let ptm1_2 = Ptm1(self.1.clone());
        let res1 = di.sig(&ptm1_1);
        let res2 = di.sig(&ptm1_2);
        (
            res1.0
                .iter()
                .zip(res2.0.iter())
                .map(|(x, y)| x.add_norm_hold(y))
                .collect(),
            res1.1
                .iter()
                .zip(res2.1.iter())
                .map(|(x, y)| x.add_norm_open(y))
                .collect(),
            res1.2
                .iter()
                .zip(res2.2.iter())
                .map(|(x, y)| x.add_norm_exit(y))
                .collect(),
        )
    }
}
/* #endregion */



/* #region Pnl */
#[derive(Clone)]
pub struct Pnl(pub PtmBox);

impl Sig<PnlRes<dt>> for Pnl{
    fn init(&self, _di: &mut Di) -> PnlRes<dt> {
        PnlRes(vec![], vec![])
    }
    fn di(&self, di: &mut Di) -> PnlRes<dt> {
        let sigr = self.0.di(di);
        let profit = di.profit();
        let hold: Vec<f32> = sigr.0.iter().map(|x| x.to_num()).collect();
        let mut hold_lag = lag(&hold, 1);
        hold_lag[0] = 0f32;
        let res = izip!(profit.iter(), hold_lag.iter())
            .map(|(a, b)| a * b)
            .collect();
        PnlRes(di.t().clone(), res)
    }
}
/* #endregion */

/* #region impl for Di */
impl Di {
    pub fn sig<N>(&mut self, sig: &dyn Sig<N>) -> N {
        sig.di(self)
    }
}

impl Dil {
    pub fn sig<N: Send + Sync>(&mut self, sig: &(dyn Sig<N> + Send + Sync)) -> Vec<N> {
        thread::scope(|scope| {
            let mut handles = Vec::new();
            for di in &mut self.dil {
                let handle = scope.spawn(move || di.sig(sig));
                handles.push(handle);
            }
            handles.into_iter().map(|x| x.join().unwrap()).collect()
        })
    }
}
/* #endregion */

