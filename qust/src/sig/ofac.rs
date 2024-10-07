use std::collections::HashSet;
use std::marker::{Send, Sync};
use std::thread;
use crate::ds::aa::Lag;
use crate::sig::cond::{Cond, CondLoop};
use crate::trade::di::{Di, Dil};
use crate::stats::pnl::PnlRes;
use crate::ds::types::*;
use crate::sig::posi::*;

/* #region  trait Sig */
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
/* #endregion */

/* #region Tsig */
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tsig<T, N> {
    pub o_dire: Dire,
    pub e_dire: Dire,
    pub o_sig: T,
    pub e_sig: N,
}

impl<T, N> Tsig<T, N> {
    pub fn new(dire: Dire, o_sig: &T, e_sig: &N) -> Self
    where
        T: Clone,
        N: Clone,
    {
        let dire_ops = if let Dire::Lo = dire {
            Dire::Sh
        } else {
            Dire::Lo
        };
        Tsig {
            o_dire: dire,
            e_dire: dire_ops,
            o_sig: o_sig.clone(),
            e_sig: e_sig.clone(),
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

    pub fn to_ptm2<T2, N2>(&self, tsig: &Tsig<T2, N2>) -> Ptm2<Stp<Self>, Stp<Tsig<T2, N2>>>
    where
        Self: Sig<TsigRes> + Clone + 'static,
        Tsig<T2, N2>: Sig<TsigRes> + Clone + 'static,
    {
        Ptm2(Stp(self.clone()), Stp(tsig.clone()))
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stp<T>(pub T);
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
pub type PtmBox = Box<dyn Sig<PtmRes>>;
#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ptm2<T, N>(pub T, pub N);
impl<T: Sig<StpRes> + Clone + 'static, N: Sig<StpRes> + Clone + 'static> Sig<PtmRes> for Ptm2<T, N>
{
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

impl Sig<PnlRes<dt>> for Pnl {
    fn init(&self, _di: &mut Di) -> PnlRes<dt> {
        PnlRes(vec![], vec![])
    }
    fn di(&self, di: &mut Di) -> PnlRes<dt> {
        let sigr = self.0.di(di);
        let profit = di.profit();
        let hold: Vec<f32> = sigr.0.iter().map(|x| x.to_num()).collect();
        let mut hold_lag = hold.lag(1);
        hold_lag[0] = 0f32;
        let res = izip!(profit.iter(), hold_lag.iter())
            .map(|(a, b)| a * b)
            .collect();
        PnlRes(di.t().clone(), res)
    }
}
/* #endregion */


/* #region Ksig */
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ksig<T>(pub T);

impl<T: CondLoop + Clone> Sig<PtmRes> for Ksig<T> {
    fn init(&self, di: &mut Di) -> PtmRes {
        self.0.calc_init(di);
        let len = di.len();
        let h_norm = vec![NormHold::No; len];
        let o_norm = vec![NormOpen::No; len];
        let e_norm = vec![NormExit::No; len];
        (h_norm, o_norm, e_norm)
    }
    fn di(&self, di: &mut Di) -> PtmRes {
        let mut res = self.init(di);
        let mut loop_fn = self.0.cond(di);
        let mut posi_last = 0f32;
        let mut posi_target;
        for i in 0..res.0.len() {
            posi_target = loop_fn(i);
            res.0[i] = if posi_target > 0. {
                NormHold::Lo(posi_target)
            } else if posi_target < 0. {
                NormHold::Sh(-posi_target)
            } else {
                NormHold::No
            };
            let posi_gap = posi_target - posi_last;
            if posi_target > 0. && posi_last > 0. && posi_gap > 0. {
                res.1[i] = NormOpen::Lo(posi_gap);
            } else if posi_target > 0. && posi_last >= 0. && posi_gap < 0. {
                res.2[i] = NormExit::Sh(-posi_gap);
            } else if posi_target < 0. && posi_last <= 0. && posi_gap < 0. {
                res.1[i] = NormOpen::Sh(-posi_gap);
            } else if posi_target < 0. && posi_last <= 0. && posi_gap > 0. {
                res.2[i] = NormExit::Lo(posi_gap);
            } else if posi_target > 0. && posi_last < 0. {
                res.1[i] = NormOpen::Lo(posi_target);
                res.2[i] = NormExit::Lo(-posi_last);
            } else if posi_target < 0. && posi_last > 0. {
                res.1[i] = NormOpen::Sh(-posi_target);
                res.2[i] = NormExit::Sh(posi_last);
            }
            posi_last = posi_target;
        }
        res
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
    pub fn sig<N: Send>(&mut self, sig: &(dyn Sig<N> + Send)) -> Vec<N> {
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
