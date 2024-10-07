#![allow(dead_code)]
use qust::prelude::*;
use crate::prelude::ToArray;
use ndarray_stats::CorrelationExt;
use ndarray::{Array2, Axis};

#[derive(Clone)]
pub struct StatsRes {
    pub ret: f32,
    pub sr: f32,
    pub cratio: f32,
    pub profit: f32,
    pub comm: f32,
    pub slip: f32,
    pub to_day: f32,
    pub to_sum: f32,
    pub hold: f32,
    pub std: f32,
    pub mdd: f32,
}

impl std::fmt::Debug for StatsRes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Display>::fmt(self, f)
    }
}
impl std::fmt::Display for StatsRes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f, 
            "
ret......{:.2}
sr.......{:.2}
cratio...{:.2}
profit...{:.2}
comm.....{:.2}
slip.....{:.2}
to_day...{:.2}
to_sum...{:.0}
hold.....{:.2}
std......{:.2}
mdd......{:.2}",
            self.ret, self.sr, self.cratio, self.profit, self.comm, self.slip, self.to_day, 
            self.to_sum, self.hold, self.std, self.mdd,
        )
    }
}

pub trait Stats {
    fn stats(&self) -> StatsRes;
}

impl Stats for  PnlRes<da> {
    fn stats(&self) -> StatsRes {
        let tover = izip!(self.1[3].iter(), self.1[2].iter())
            .map(|(x, y)| if *y == 0. { 0. } else { x / 2. / y })
            .collect_vec();
        let money_trade = self.1[3].agg(RollFunc::Sum) / 2.;
        let ret = ret_annu(self) * 100f32;
        let sr = self.sr();
        let cratio = cratio(&self.1[0]);
        StatsRes {
            ret,
            sr,
            cratio,
            profit: 10_000. * self.1[1].agg(RollFunc::Sum) / money_trade,
            comm: 10_000. * self.1[5].agg(RollFunc::Sum) / money_trade,
            slip: 10_000. * self.1[6].agg(RollFunc::Sum) / money_trade,
            hold: self.1[7].agg(RollFunc::Sum) / money_trade,
            to_day: tover.agg(RollFunc::Mean),
            to_sum: tover.agg(RollFunc::Sum),
            std: ret / sr,
            mdd: ret / cratio,
        }
    }
}

pub trait Stats2 {
    fn stats2(&self) -> StatsRes;
}

impl Stats2 for PnlRes<da> {
    fn stats2(&self) -> StatsRes {
        let x = 360_0000f32;
        let tover = izip!(self.1[3].iter(), self.1[2].iter())
            .map(|(x, y)| if *y == 0. { 0. } else { x / 2. / y })
            .collect_vec();
        let money_trade = self.1[3].agg(RollFunc::Sum) / 2.;
        let year_num = (*self.0.last().unwrap() - self.0[0]).num_days() as f32 / 365.;
        let ret = 100. * self.1[0].sum() / year_num / x;
        let sr = self.sr();
        let mdd_x = max_drawdown(&self.1[0]);
        let mdd = 100. * mdd_x / x;
        let cratio = ret / mdd;
        StatsRes {
            ret,
            sr,
            cratio,
            profit: 10_000. * self.1[1].agg(RollFunc::Sum) / money_trade,
            comm: 10_000. * self.1[5].agg(RollFunc::Sum) / money_trade,
            slip: 10_000. * self.1[6].agg(RollFunc::Sum) / money_trade,
            hold: self.1[7].agg(RollFunc::Sum) / money_trade,
            to_day: tover.agg(RollFunc::Mean),
            to_sum: tover.agg(RollFunc::Sum),
            std: self.1[0].agg(RollFunc::Std),
            mdd,
        }
    }
}

impl<T> Stats2 for InfoPnlRes<T, da> {
    fn stats2(&self) -> StatsRes {
        self.1.stats2()
    }
}

impl Stats for PnlRes<dt> {
    fn stats(&self) -> StatsRes {
        self.da().stats()
    }
}
impl<T, N> Stats for InfoPnlRes<T, N>
where
    PnlRes<N>: Stats,
{
    fn stats(&self) -> StatsRes {
        self.1.stats()
    }
}


/* #region Sr */
pub trait Sr {
    fn sr(&self) -> f32;
}

impl Sr for [f32] {
    fn sr(&self) -> f32 {
        240f32.sqrt() * self.agg(RollFunc::Mean) / self.agg(RollFunc::Std)
    }
}

impl Sr for PnlRes<da> {
    fn sr(&self) -> f32 {
        self.1[0].sr()
    }
}

impl Sr for PnlRes<dt> {
    fn sr(&self) -> f32 {
        self.da().sr()
    }
}
/* #endregion */
pub fn ret_annu(pnl: &PnlRes<da>) -> f32 {
    let thre_ = pnl.1[2].quantile(0.02);
    let mul = pnl.1[2]
        .rolling(150)
        .map(|x| {
            let q = x.quantile(0.9);
            if q <= thre_ { 0f32 } else { 100f32 / q }
        })
        .collect_vec()
        .lag(1);
    let mut pnl_new = pnl * &mul;
    pnl_new.1[2] = vec![100f32; pnl_new.1[2].len()];
    let pnl_sum = pnl_new.1[0].iter()
        .map(|x| x / 100f32)
        .sum::<f32>();
    let k = (*pnl_new.0.last().unwrap() - *pnl_new.0.first().unwrap())
        .num_days() as f32 / 365f32;
    pnl_sum / k
}

fn max_drawdown(x: &[f32]) -> f32 {
    let pnl_cum = x.cumsum();
    let t =  pnl_cum.iter()
        .scan(pnl_cum[0], |accu, x| {
            *accu = x.max(*accu);
            Some(*accu)
        });
    let max_spread = izip!(t, pnl_cum.iter())
        .map(|(x, y)| x - y)
        .collect_vec();
    // let x = max_spread.max();
    // let i = max_spread.iter().position(|y| y >= &x).unwrap();
    // println!("{x} - {:?}", i);
    max_spread.max()
}

pub fn cratio(data: &[f32]) -> f32 {
    240f32 * data.mean() / max_drawdown(data)
}

pub struct Acc;

impl CalcStra for Acc {
    type Output = (f32, f32);
    fn calc_stra(&self, distra: &DiStra) -> Self::Output {
        let stat = distra.di.pnl(&distra.stra.ptm, CommSlip(1f32, 0f32)).stats();
        (stat.profit, stat.to_sum)
    }
}



trait GetPnlRes<T> {
    fn get_pnl_res(&self) -> &PnlRes<da>;
}

impl<T> GetPnlRes<PnlRes<da>> for T
where
    T: AsRef<PnlRes<da>>,
{
    fn get_pnl_res(&self) -> &PnlRes<da> {
        self.as_ref()
    }
}
impl<T, N> GetPnlRes<InfoPnlRes<N, da>> for T
where
    T: AsRef<InfoPnlRes<N, da>>,
    N: 'static,
{
    fn get_pnl_res(&self) -> &PnlRes<da> {
        &self.as_ref().1
    }
}

pub trait Corr2<T>: AsRef<[PnlRes<T>]> {
    fn e(&self) -> v32 {
        self.as_ref().map(|x| x.1[0].agg(RollFunc::Mean))
    }
    fn corr(&self) -> Array2<f32> {
        self
            .as_ref()
            .iter()
            .map(|x| &x.1[0])
            .collect_vec()
            .to_array()
            .pearson_correlation()
            .unwrap()
    }
    fn cov(&self) -> Array2<f32> {
        self
            .as_ref()
            .iter()
            .map(|x| &x.1[0])
            .collect_vec()
            .to_array()
            .cov(1.)
            .unwrap()
    }
    fn delta(&self) -> vv32 {
        let k = self.cov();
        k.dot(&k)
            .axis_iter(Axis(0))
            .map(|x| x.to_vec())
            .collect_vec()
    }
}
impl<T> Corr2<T> for [PnlRes<T>] {}

pub trait CorrFilter {
    fn corr_filter(&self, thre: f32) -> vuz;
}

impl CorrFilter for [PnlRes<da>] {
    fn corr_filter(&self, thre: f32) -> vuz {
        let corr_matrix = self.corr();
        let l = corr_matrix.shape()[0];
        let mut res = vec![0];
        'a: for i in 1..l-1 {
            for j in res.iter() {
                if corr_matrix[[*j, i]] > thre {
                    continue 'a;
                }
            }
            res.push(i);
        }
        res
    }
}

pub trait PnlModify {
    type Output;
    fn pnl_modify(&self, n: usize, i: f32) -> Self::Output;
    fn pnl_multi(&self, i: f32) -> Self::Output;
}

impl PnlModify for Vec<InfoPnlRes<Stra, dt>> {
    type Output = Vec<InfoPnlRes<Stra, dt>>;
    fn pnl_modify(&self, i: usize, n: f32) -> Self::Output {
        let m = self
            .groupby(&|x: &Stra| -> Ticker { x.ident.ticker }, |x| x.sum())
            .pnl_sum_between_ticker()
            .da()
            .1[2]
            .nlast(i)
            .quantile(0.85);
        let m = n / m;
        self.map(|x| InfoPnlRes(x.0.clone(), &x.1 * m))
    }

    fn pnl_multi(&self, i: f32) -> Self::Output {
        self.map(|x| InfoPnlRes(x.0.clone(), &x.1 * i))
    }
}

pub trait PnlStd {
    type Output;
    fn pnl_std(&self) -> Self::Output;
}

impl PnlStd for [PnlRes<da>] {
    type Output = Vec<PnlRes<da>>;
    fn pnl_std(&self) -> Self::Output {
        self.map(|x| {
            let std = x.1[0].agg(RollFunc::Std);
            x * (1. /  std)
        })
    }
}

impl<T: Clone> PnlStd for [InfoPnlRes<T, da>] {
    type Output = Vec<InfoPnlRes<T, da>>; 
    fn pnl_std(&self) -> Self::Output {
        self.map(|x| {
            let std = x.1.1[0].agg(RollFunc::Std);
            InfoPnlRes(x.0.clone(), &x.1 * (1. / std))
        })
    }
}

pub trait CheckRes {
    fn check_res(&self, to_check: &[StatsRes]) -> bool;
}

pub struct CheckList1 {
    pub ori_res: Vec<StatsRes>,
    pub min_trade_num: usize,
    pub promotion_thre: f32,
    pub promotion_percent: f32, 
}


impl CheckRes for CheckList1 {
    fn check_res(&self, to_check: &[StatsRes]) -> bool {
        let promotion_size = izip!(self.ori_res.iter(), to_check.iter())
            .fold(0usize, |mut accu, (pre, now)| {
                if now.to_sum as usize > self.min_trade_num && 
                    ((pre.profit < 0. && now.profit > 0.) || 
                    (now.profit / pre.profit - 1. > self.promotion_thre)) {
                    accu += 1;
                }
                accu
            });
        promotion_size as f32 / self.ori_res.len() as f32 > self.promotion_percent
    }
}

pub fn check_info_ticker(data: &[InfoPnlRes<Stra, da>]) -> f32 {
    let mut kk = data.iter().map(|x| &x.1).collect_vec();
    kk.sort_by(|a, b| a.sr().partial_cmp(&b.sr()).unwrap());
    kk.nlast(20).sum().sr()
}


pub fn corr_month(pnl1: &PnlRes<da>, pnl2: &PnlRes<da>) -> f32 {
    let t_vec = pnl1.0.map(|x| x.format("%Y%m").to_string());
    let grp = Grp(t_vec);
    let p1 = grp.apply(&pnl1.1[0], |x| x.iter().sum::<f32>()).1;
    let p2 = grp.apply(&pnl2.1[0], |x| x.iter().sum::<f32>()).1;
    [p1, p2]
        .to_array()
        .pearson_correlation()
        .unwrap()[[0, 1]]
}

pub fn corr_weeks(pnl1: &PnlRes<da>, pnl2: &PnlRes<da>, n: usize) -> f32 {
    let l = pnl1.0.len();
    let t_vec = (0..l)
        .step_by(n)
        .enumerate()
        .fold(Vec::with_capacity(l), |mut accu, (i, _)| {
            let l_ = n.min(l - accu.len());
            let mut v = vec![i; l_];
            accu.append(&mut v);
            accu
        });
    let grp = Grp(t_vec);
    let p1 = grp.apply(&pnl1.1[0], |x| x.iter().sum::<f32>()).1;
    let p2 = grp.apply(&pnl2.1[0], |x| x.iter().sum::<f32>()).1;
    [p1, p2]
        .to_array()
        .pearson_correlation()
        .unwrap()[[0, 1]]
}