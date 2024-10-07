#![allow(clippy::map_entry, clippy::borrowed_box, unused_macros, non_snake_case)]
use crate::{
    idct::prelude::*, prelude::{BtDiKline, KtnVar, PtmResRecord}, sig::{
        cond::*,
        posi::{Open, *},
    }, trade::prelude::*
};
use qust_ds::prelude::*;
use qust_derive::*;
use dyn_clone::clone_trait_object;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::RwLock;
use crate::live::cond_ops::*;

type LiveSigUpDate<'a> = Box<dyn FnMut(&'a Di) + 'a>;

/* #region Trait LiveSig */
///object implemented LiveSig cann't keep its running state, the state
/// must be reserved in di's data_save.
pub trait LiveSig: Calc<Arc<BoxAny>> {
    type R;
    fn get_data(&self, di: &Di) -> RwLock<Self::R>;
    fn update(&self, di: &Di, data: &RwLock<Self::R>);
    fn update2<'a>(&'a self, _di: &'a Di, _data: &'a RwLock<Self::R>) -> LiveSigUpDate<'a> {
        todo!()
    }
}

clone_trait_object!(<R> LiveSig<R = R>);

/* #endregion */

/* #region  Tsig */
#[ta_derive]
pub enum Tsig {
    Tsig(Dire, Dire, Box<dyn Cond>, Box<dyn Cond>),
    TsigFilter(Box<Tsig>, Iocond),
    TsigQ(Dire, Dire, Box<dyn Cond>, Box<dyn Cond>),
}

impl Tsig {
    pub fn new<T, N>(dire: Dire, o_sig: &T, e_sig: &N) -> Self
    where
        T: Cond + Clone,
        N: Cond + Clone,
    {
        Tsig::Tsig(dire, !dire, o_sig.to_box(), e_sig.to_box())
    }
}

impl LiveSig for Tsig {
    type R = (HashSet<OpenIng>, TsigRes);
    fn get_data(&self, di: &Di) -> RwLock<(HashSet<OpenIng>, TsigRes)> {
        let len = di.len() + 500;
        let o_vec = Vec::with_capacity(len);
        let e_vec = Vec::with_capacity(len);
        RwLock::new((HashSet::new(), (o_vec, e_vec)))
    }
    fn update(&self, di: &Di, data: &RwLock<(HashSet<OpenIng>, TsigRes)>) {
        match self {
            Tsig::Tsig(o_dire, e_dire, o_sig, e_sig) => {
                let time_vec = di.t();
                let o_fn = o_sig.cond(di);
                let e_fn = e_sig.cond(di);
                let mut data = data.write().unwrap();
                for i in data.1 .1.len()..di.len() {
                    if o_fn(i, i) {
                        // println!("{i}");
                        data.0.insert(o_dire.open_ing(i));
                        data.1 .0.push(o_dire.open(i));
                    } else {
                        data.1 .0.push(Open::No);
                    }
                    let mut stop_set: HashSet<OpenIng> = HashSet::new();
                    let mut stop_set_i: HashSet<usize> = HashSet::new();
                    for &x in data.0.iter() {
                        let inner_i = x.inner_i();
                        let open_time = time_vec[inner_i];
                        for j in (0..i + 1).rev() {
                            if time_vec[j] <= open_time {
                                if e_fn(i, j) {
                                    // println!("{i}, {j}");
                                    stop_set.insert(x);
                                    stop_set_i.insert(inner_i);
                                }
                                break;
                            }
                        }
                    }
                    if !stop_set.is_empty() {
                        for stop in stop_set.iter() {
                            data.0.remove(stop);
                        }
                        data.1 .1.push(e_dire.exit(stop_set_i));
                    } else {
                        data.1 .1.push(Exit::No);
                    }
                }
            }
            Tsig::TsigFilter(tsig, io_cond) => {
                let pms_res = &di.calc(&io_cond.pms)[0];
                let b = di.calc(&**tsig);
                let tsig_res = b
                    .downcast_ref::<RwLock<(HashSet<OpenIng>, TsigRes)>>()
                    .unwrap()
                    .read()
                    .unwrap();
                let mut data = data.write().unwrap();
                for (i, tsig_res_) in pms_res
                    .iter()
                    .enumerate()
                    .take(di.len())
                    .skip(data.1 .0.len())
                {
                    if io_cond.range.contains(tsig_res_) {
                        data.1 .0.push(tsig_res.1 .0[i].clone());
                    } else {
                        data.1 .0.push(Open::No);
                    }
                    data.1 .1.push(tsig_res.1 .1[i].clone());
                }
                data.0.clear();
                for x in tsig_res.0.iter() {
                    if io_cond.range.contains(&pms_res[x.inner_i()]) {
                        data.0.insert(*x);
                    }
                }
            }
            //this is only for backtest!!!!!!
            Tsig::TsigQ(o_dire, e_dire, o_sig, e_sig) => {
                let o_fn = o_sig.cond(di);
                let e_fn = e_sig.cond(di);
                let mut data = data.write().unwrap();
                let mut is_on = false;
                let mut o = 0usize;
                for i in data.1 .1.len()..di.len() {
                    if !is_on && o_fn(i, i) {
                        data.1 .0.push(o_dire.open(i));
                        is_on = true;
                        o = i;
                    } else {
                        data.1 .0.push(Open::No);
                    }
                    if is_on {
                        if e_fn(i, o) {
                            let mut k = HashSet::new();
                            k.insert(o);
                            data.1 .1.push(e_dire.exit(k));
                            is_on = false;
                        } else {
                            data.1 .1.push(Exit::No);
                        }
                    } else {
                        data.1 .1.push(Exit::No);
                    }
                }
            }
        }
    }
}

/* #endregion */

/* #region Stp */
#[ta_derive]
pub enum Stp {
    Stp(Tsig),
    StpWeight(Box<Stp>, CondWeight),
}

impl LiveSig for Stp {
    type R = StpRes;

    fn get_data(&self, di: &Di) -> RwLock<Self::R> {
        let len = di.len();
        let h_res = Vec::with_capacity(len);
        let o_res = Vec::with_capacity(len);
        let e_res = Vec::with_capacity(len);
        RwLock::new((h_res, o_res, e_res))
    }

    fn update(&self, di: &Di, data: &RwLock<Self::R>) {
        let mut res = data.write().unwrap();
        match self {
            Stp::Stp(tsig) => {
                let b = di.calc(tsig);
                let (o_vec, e_vec) = &b
                    .downcast_ref::<RwLock<(HashSet<OpenIng>, TsigRes)>>()
                    .unwrap()
                    .read()
                    .unwrap()
                    .1;
                for i in res.0.len()..o_vec.len() {
                    let hold = if i > 1 { &res.0[i - 1].0 } else { &Hold::No };
                    let hold_open = hold.add_open(&o_vec[i]);
                    let hold = hold_open.0;
                    let open = hold_open.1;
                    let hold_exit = hold.add_exit(&e_vec[i]);
                    let hold = hold_exit.0;
                    let exit = hold_exit.1;
                    res.0.push(PosiWeight(hold, 1f32));
                    res.1.push(PosiWeight(open, 1f32));
                    res.2.push(PosiWeight(exit, 1f32));
                }
            }
            Stp::StpWeight(stp, cond_weight) => {
                let b0 = di.calc(&**stp);
                let b1 = di.calc(cond_weight);
                let tsig_res = b0.downcast_ref::<RwLock<StpRes>>().unwrap().read().unwrap();
                let weight = b1.downcast_ref::<RwLock<v32>>().unwrap().read().unwrap();
                for i in res.0.len()..tsig_res.1.len() {
                    let w = weight[i];
                    res.0.push(PosiWeight(tsig_res.0[i].0.clone(), w));
                    res.1.push(PosiWeight(tsig_res.1[i].0.clone(), w));
                    res.2.push(PosiWeight(tsig_res.2[i].0.clone(), w));
                }
            }
        }
    }
}

/* #endregion */

/* #region  CondWeight */
#[ta_derive]
pub struct CondWeight(pub Vec<(Box<dyn Cond>, f32)>);

impl LiveSig for CondWeight {
    type R = v32;

    fn get_data(&self, di: &Di) -> RwLock<Self::R> {
        let init_len = di.len() + 500;
        RwLock::new(Vec::with_capacity(init_len))
    }

    fn update(&self, di: &Di, data: &RwLock<Self::R>) {
        let mut data = data.write().unwrap();
        let mut f_vec = self.0.iter().map(|(x, y)| (x.cond(di), y)).collect_vec();
        for i in data.len()..di.len() {
            let w_now = f_vec
                .iter_mut()
                .map(|(x, y)| if x(i, i) { **y } else { 0.0 })
                .sum::<f32>();
            data.push(w_now);
        }
    }
}
/* #endregion */

/* #region Ptm */
#[ta_derive]
pub enum Ptm {
    Ptm1(Box<dyn Money>, Stp),
    Ptm2(Box<dyn Money>, Stp, Stp),
    Ptm3(Box<dyn Money>, Dire, Box<dyn Cond>, Box<dyn Cond>),
    Ptm4(Box<Ptm>, Box<Ptm>),
    Ptm5(Box<Ptm>),
    Ptm6(Box<dyn CondType2>),
    Ptm7(KtnVar),
}

impl LiveSig for Ptm {
    type R = PtmResState;

    fn get_data(&self, di: &Di) -> RwLock<Self::R> {
        let res = Self::R::new(di.len());
        RwLock::new(res)
    }

    fn update(&self, di: &Di, data: &RwLock<Self::R>) {
        let mut res = data.write().unwrap();
        match self {
            Ptm::Ptm1(money, stp) => {
                let b = di.calc(stp);
                let sigr = b.downcast_ref::<RwLock<StpRes>>().unwrap().read().unwrap();
                let f = money.register(di);
                for i in res.ptm_res.0.len()..sigr.0.len() {
                    let h = sigr.0[i].to_norm();
                    let hold_now = f(&h, i);
                    let (open_now, exit_now) = hold_now.sub_norm_hold(&res.state);
                    res.ptm_res.0.push(hold_now.clone());
                    res.ptm_res.1.push(open_now);
                    res.ptm_res.2.push(exit_now);
                    res.state = hold_now;
                }
            }
            Ptm::Ptm2(money, stp1, stp2) => {
                let b0 = di.calc(stp1);
                let b1 = di.calc(stp2);
                let sigr0 = b0.downcast_ref::<RwLock<StpRes>>().unwrap().read().unwrap();
                let sigr1 = b1.downcast_ref::<RwLock<StpRes>>().unwrap().read().unwrap();
                let f = money.register(di);
                for i in res.ptm_res.0.len()..sigr0.0.len() {
                    let h = sigr0.0[i].to_norm().add_norm_hold(&sigr1.0[i].to_norm());
                    let hold_now = f(&h, i);
                    let (open_now, exit_now) = hold_now.sub_norm_hold(&res.state);
                    res.ptm_res.0.push(hold_now.clone());
                    res.ptm_res.1.push(open_now);
                    res.ptm_res.2.push(exit_now);
                    res.state = hold_now;
                }
            }
            Ptm::Ptm3(money, dire, o_sig, e_sig) => {
                let f = money.register(di);
                let o_fn = o_sig.cond(di);
                let e_fn = e_sig.cond(di);
                let hold_unit = match dire {
                    Dire::Lo => NormHold::Lo(1.),
                    Dire::Sh => NormHold::Sh(1.),
                };
                for i in res.ptm_res.0.len()..di.len() {
                    let (hold_now, open_now, exit_now) = match res.open_i {
                        Some(o) => {
                            if e_fn(i, o) {
                                let hold_now = f(&NormHold::No, i);
                                let (open_now, exit_now) = hold_now.sub_norm_hold(&res.state);
                                res.state = hold_now.clone();
                                res.open_i = None;
                                (hold_now, open_now, exit_now)
                            } else {
                                (res.state.clone(), NormOpen::No, NormExit::No)
                            }
                        }
                        None => {
                            if o_fn(i, i) && !e_fn(i, i) {
                                let hold_now = f(&hold_unit, i);
                                let (open_now, exit_now) = hold_now.sub_norm_hold(&res.state);
                                res.state = hold_now.clone();
                                res.open_i = i.into();
                                (hold_now, open_now, exit_now)
                            } else {
                                (res.state.clone(), NormOpen::No, NormExit::No)
                            }
                        }
                    };
                    res.ptm_res.0.push(hold_now);
                    res.ptm_res.1.push(open_now);
                    res.ptm_res.2.push(exit_now);
                }
            }
            Ptm::Ptm4(ptm0, ptm1) => {
                let b0 = di.calc(ptm0);
                let b1 = di.calc(ptm1);
                let sigr0 = &b0
                    .downcast_ref::<RwLock<PtmResState>>()
                    .unwrap()
                    .read()
                    .unwrap()
                    .ptm_res
                    .0;
                let sigr1 = &b1
                    .downcast_ref::<RwLock<PtmResState>>()
                    .unwrap()
                    .read()
                    .unwrap()
                    .ptm_res
                    .0;
                for i in res.ptm_res.0.len()..sigr0.len() {
                    let hold_now = sigr0[i].add_norm_hold(&sigr1[i]);
                    let (open_now, exit_now) = hold_now.sub_norm_hold(&res.state);
                    res.ptm_res.0.push(hold_now.clone());
                    res.ptm_res.1.push(open_now);
                    res.ptm_res.2.push(exit_now);
                    res.state = hold_now;
                }
            }
            Ptm::Ptm5(ptm) => {
                let b = di.calc(ptm);
                let sigr = &b
                    .downcast_ref::<RwLock<PtmResState>>()
                    .unwrap()
                    .read()
                    .unwrap()
                    .ptm_res;
                // println!("ooooo {:?}  {:?}", res.ptm_res.0.len(), sigr.1.len());
                for i in res.ptm_res.0.len()..sigr.1.len() {
                    let sig_now = match sigr.1[i] {
                        NormOpen::Lo(i) => NormHold::Lo(i),
                        NormOpen::Sh(i) => NormHold::Sh(i),
                        NormOpen::No => NormHold::No,
                    };
                    // println!("{:?} --- {:?}", sigr.1[i], sig_now);
                    res.ptm_res.0.push(sig_now);
                    res.ptm_res.1.push(sigr.1[i].clone());
                    res.ptm_res.2.push(sigr.2[i].clone());
                }
            }
            Ptm::Ptm7(ptm) => {
                if !res.ptm_res.0.is_empty() {
                    return;
                }
                ptm.bt_di_kline(di)
                    .into_iter()
                    .for_each(|PtmResRecord { norm_hold, norm_open, norm_exit }| {
                        res.ptm_res.0.push(norm_hold);
                        res.ptm_res.1.push(norm_open);
                        res.ptm_res.2.push(norm_exit);
                    })
            }
            _ => panic!(),
        }
    }
}

impl PartialEq for Ptm {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Ptm::Ptm1(m1, a1), Ptm::Ptm1(m2, a2)) => {
                m1.debug_string() == m2.debug_string() && a1.debug_string() == a2.debug_string()
            }
            (Ptm::Ptm2(m1, a1, b1), Ptm::Ptm2(m2, a2, b2)) => {
                m1.debug_string() == m2.debug_string()
                    && a1.debug_string() == a2.debug_string()
                    && b1.debug_string() == b2.debug_string()
            }
            (Ptm::Ptm3(m1, a1, b1, c1), Ptm::Ptm3(m2, a2, b2, c2)) => {
                a1 == a2
                    && m1.debug_string() == m2.debug_string()
                    && b1.debug_string() == b2.debug_string()
                    && c1.debug_string() == c2.debug_string()
            }
            (Ptm::Ptm4(a1, b1), Ptm::Ptm4(a2, b2)) => a1 == a2 && b1 == b2,
            _ => false,
        }
    }
}

impl Ptm {
    pub fn from_bare<T, N, K>(dire: Dire, o_sig: &T, e_sig: &N, m: &K) -> Self
    where
        T: Cond + Clone,
        N: Cond + Clone,
        K: Money + Clone,
    {
        let tsig = Tsig::new(dire, o_sig, e_sig);
        let stp = Stp::Stp(tsig);
        Ptm::Ptm1(Box::new(m.clone()), stp)
    }

    pub fn from_two_ptms(ptm1: &Ptm, ptm2: &Ptm) -> Ptm {
        match (&ptm1, &ptm2) {
            (&Ptm::Ptm1(m1, stp1), &Ptm::Ptm1(_, stp2)) => {
                Ptm::Ptm2(m1.clone(), stp1.clone(), stp2.clone())
            }
            _ => panic!("these two ptm can't be made into one ptm"),
        }
    }

    pub fn split(&self) -> (Ptm, Ptm) {
        match self {
            Ptm::Ptm2(m, stp1, stp2) => (
                Ptm::Ptm1(m.clone(), stp1.clone()),
                Ptm::Ptm1(m.clone(), stp2.clone()),
            ),
            Ptm::Ptm4(ptm1, ptm2) => ((**ptm1).clone(), (**ptm2).clone()),
            _ => panic!("this ptm can't not be splited"),
        }
    }

    pub fn get_money_fn(&self) -> Box<dyn Money> {
        match self {
            Ptm::Ptm1(ref m, _) => m.clone(),
            Ptm::Ptm2(ref m, _, _) => m.clone(),
            Ptm::Ptm3(ref m, _, _, _) => m.clone(),
            Ptm::Ptm4(ptm1, _) => (*ptm1).get_money_fn(),
            Ptm::Ptm5(ptm) => ptm.get_money_fn(),
            _ => panic!(),
        }
    }

    pub fn change_money<T: Money>(&self, x: T) -> Ptm {
        self.change_money_box(Box::new(x))
    }

    pub fn change_money_box(&self, y: Box<dyn Money>) -> Ptm {
        match self {
            Ptm::Ptm1(_, stp) => Ptm::Ptm1(y, stp.clone()),
            Ptm::Ptm2(_, stp1, stp2) => Ptm::Ptm2(y, stp1.clone(), stp2.clone()),
            Ptm::Ptm3(_, dire, o_sig, e_sig) => Ptm::Ptm3(y, *dire, o_sig.clone(), e_sig.clone()),
            Ptm::Ptm4(ptm1, ptm2) => Ptm::Ptm4(
                (**ptm1).change_money_box(y.clone()).into(),
                (**ptm2).change_money_box(y).into(),
            ),
            Ptm::Ptm5(ptm) => Ptm::Ptm5(Box::new((*ptm).change_money_box(y))),
            _ => panic!(),
        }
    }

    pub fn convert_ptm(self) -> Self {
        match self {
            Ptm::Ptm1(m, Stp::Stp(Tsig::Tsig(o_dire, _e_dire, o_sig, e_sig))) => {
                Ptm::Ptm3(m, o_dire, o_sig, e_sig)
            }
            Ptm::Ptm2(m, stp1, stp2) => Ptm::Ptm4(
                Ptm::Ptm1(m.clone(), stp1).convert_ptm().pip(Box::new),
                Ptm::Ptm1(m, stp2).convert_ptm().pip(Box::new),
            ),
            Ptm::Ptm3(m, dire, osig, esig) => {
                let tsig = Tsig::Tsig(dire, !dire, osig, esig);
                let stp = Stp::Stp(tsig);
                Ptm::Ptm1(m, stp)
            }
            Ptm::Ptm4(ptm1, ptm2) => match (*ptm1, *ptm2) {
                (Ptm::Ptm3(m1, dire1, osig1, esig1), Ptm::Ptm3(_, dire2, osig2, esig2)) => {
                    let stp1 = Tsig::Tsig(dire1, !dire1, osig1, esig1).pip(Stp::Stp);
                    let stp2 = Tsig::Tsig(dire2, !dire2, osig2, esig2).pip(Stp::Stp);
                    Ptm::Ptm2(m1, stp1, stp2)
                }
                other => Ptm::Ptm4(Box::new(other.0), Box::new(other.1)),
            },
            _ => panic!("this type of Ptm not implement converting"),
        }
    }
}

pub trait ToPtm {
    fn to_ptm(self) -> Ptm;
}

impl<T, N, K> ToPtm for (T, Dire, N, K)
where
    T: Money,
    N: Cond,
    K: Cond,
{
    fn to_ptm(self) -> Ptm {
        Ptm::Ptm3(Box::new(self.0), self.1, self.2.to_box(), self.3.to_box())
    }
}
impl ToPtm for (Box<dyn Money>, Dire, Box<dyn Cond>, Box<dyn Cond>) {
    fn to_ptm(self) -> Ptm {
        Ptm::Ptm3(self.0, self.1, self.2, self.3)
    }
}
impl<T, N> ToPtm for (Dire, T, N)
where
    T: Cond,
    N: Cond,
{
    fn to_ptm(self) -> Ptm {
        (M1(1.), self.0, self.1, self.2).to_ptm()
    }
}

impl<T, N> ToPtm for (T, N)
where
    T: Cond,
    N: Cond,
{
    fn to_ptm(self) -> Ptm {
        (Dire::Lo, self.0, self.1).to_ptm()
    }
}

impl ToPtm for Ptm {
    fn to_ptm(self) -> Ptm {
        match self {
            Ptm::Ptm1(m, Stp::Stp(Tsig::Tsig(o_dire, _, o_sig, e_sig))) => {
                Ptm::Ptm3(m, o_dire, o_sig, e_sig)
            }
            Ptm::Ptm2(m, stp1, stp2) => Ptm::Ptm4(
                Box::new(Ptm::Ptm1(m.clone(), stp1).to_ptm()),
                Box::new(Ptm::Ptm1(m, stp2).to_ptm()),
            ),
            other => other,
        }
    }
}

impl ToPtm for (Ptm, Ptm) {
    fn to_ptm(self) -> Ptm {
        Ptm::Ptm4(Box::new(self.0), Box::new(self.1))
    }
}

/* #endregion */
