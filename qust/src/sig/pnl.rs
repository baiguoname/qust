#![allow(clippy::op_ref)]
use crate::{
    idct::ta::CommSlip,
    prelude::{Stra, ToNum, ToStralBare},
    sig::livesig::LiveSig,
    sig::posi::PtmResState,
    std_prelude::*,
    trade::prelude::*,
};
use qust_ds::prelude::*;
use qust_derive::*;
use super::prelude::PtmRes;

impl Di {
    pub fn pnl<T: LiveSig<R = PtmResState> + AsRef<T>>(
        &self,
        sig: &T,
        comm: CommSlip,
    ) -> PnlRes<dt> {
        let b = self.calc(sig);
        let ptm_res = &b
            .downcast_ref::<RwLock<PtmResState>>()
            .unwrap()
            .read()
            .unwrap()
            .ptm_res;
        let pnl_res_pre_info = PnlResPreInfo {
            c: self.c(),
            ticker: self.pcon.ticker,
            t: self.t().to_vec(),
            profit: self.profit(),
            comm,
            pass_num: self.pcon.price.ki.iter().skip(1).map(|ki| ((ki.pass_last + ki.pass_this) as f32 / 120.)).collect_vec(),
            ptm_res,
        };
        pnl_res_pre_info.convert_to_pnl()
    }
}

pub struct PnlResPreInfo<'a> {
    pub ticker: Ticker,
    pub t: vdt,
    pub c: av32,
    pub profit: v32,
    pub comm: CommSlip,
    pub pass_num: v32,
    pub ptm_res: &'a PtmRes,
}

impl PnlResPreInfo<'_> {
    pub fn convert_to_pnl(self) -> PnlRes<dt> {
        let c = self.c;
        let comm = self.comm;
        let ticker_info = self.ticker.info();
        let tz = ticker_info.tz;
        let pv = ticker_info.pv;
        let comm_percent = match ticker_info.comm {
            Comm::F(i) => c.iter().map(|x| (comm.0 * i) / (x * pv)).collect_vec(),
            Comm::P(i) => vec![comm.0 * i; c.len()],
        };
        let slip_percent = c
            .iter()
            .map(|x| comm.1 * ticker_info.slip * tz / x)
            .collect_vec();
        let ptm_res = self.ptm_res;
        let money_hold = izip!(ptm_res.0.iter(), c.iter())
            .map(|(x, cl)| x.to_num() * cl * pv)
            .collect_vec();
        let money_open = izip!(ptm_res.1.iter(), c.iter())
            .map(|(x, y)| x.to_num() * y * pv)
            .collect_vec();
        let money_exit = izip!(ptm_res.2.iter(), c.iter())
            .map(|(x, y)| x.to_num() * y * pv)
            .collect_vec();
        let pr = self.profit;
        let hold_lag = money_hold.lag((1, 0f32));
        let profit = pr
            .iter()
            .zip(hold_lag.iter())
            .map(|(x, y)| x * y)
            .collect_vec();
        let money_trade = money_open.iter().zip(money_exit.iter()).map(|(x, y)| x + y);
        let comm_open = money_open
            .iter()
            .zip(comm_percent.iter())
            .map(|(x, y)| x.abs() * y)
            .collect_vec();
        let comm_exit = money_exit
            .iter()
            .zip(comm_percent.iter())
            .map(|(x, y)| x.abs() * y)
            .collect_vec();
        let slip_open = money_open
            .iter()
            .zip(slip_percent.iter())
            .map(|(x, y)| x.abs() * y)
            .collect_vec();
        let slip_exit = money_exit
            .iter()
            .zip(slip_percent.iter())
            .map(|(x, y)| x.abs() * y)
            .collect_vec();
        let comm_all = comm_open
            .iter()
            .zip(comm_exit.iter())
            .map(|(x, y)| x + y)
            .collect_vec();
        let slip_all = slip_open
            .iter()
            .zip(slip_exit.iter())
            .map(|(x, y)| x + y)
            .collect_vec();
        let cost_all = comm_all
            .iter()
            .zip(slip_all.iter())
            .map(|(x, y)| x + y)
            .collect_vec();
        let pnl_all = profit
            .iter()
            .zip(cost_all.iter())
            .map(|(x, y)| x - y)
            .collect_vec();
        let mut hold = izip!(money_hold.iter(), self.pass_num.iter())
            .map(|(m, ki)| m.abs() * ki)
            .collect_vec();
        hold.push(0.);
        //[pnl, profit, money, money_trade, cost, comm ,slip, hold]
        PnlRes(
            self.t,
            vec![
                pnl_all,
                profit,
                money_hold,
                money_trade.collect_vec(),
                cost_all,
                comm_all,
                slip_all,
                hold,
            ],
        )
    }
}

#[derive(Clone, Serialize, Deserialize, AsRef)]
pub struct PnlRes<T>(pub Vec<T>, pub vv32);

impl<T> PnlRes<T> {
    pub fn concat(&mut self, mut other: Self) {
        self.0.append(&mut other.0);
        izip!(self.1.iter_mut(), other.1.iter_mut()).for_each(|(x, y)| {
            x.append(y);
        })
    }
}

pub trait PnlSumInnerDay<T> {
    type Output;
    fn da(&self) -> Self::Output;
}

impl PnlSumInnerDay<dt> for PnlRes<dt> {
    type Output = PnlRes<da>;
    fn da(&self) -> Self::Output {
        let grp = Grp(self.0.map(|x| x.date()));
        let pnl_res_value = self
            .1
            .iter()
            .enumerate()
            .map(|(i, x)| {
                if i == 2 {
                    grp.apply(x, |data| data.map(|x| x.abs()).agg(RollFunc::Max))
                        .1
                } else if i == 3 {
                    grp.apply(x, |data| data.map(|x| x.abs()).agg(RollFunc::Sum))
                        .1
                } else {
                    grp.sum(x).1
                }
            })
            .collect_vec();
        PnlRes(grp.0.unique(), pnl_res_value)
    }
}

impl<T, N, K> PnlSumInnerDay<PnlRes<N>> for [T]
where
    T: AsRef<PnlRes<N>>,
    PnlRes<N>: PnlSumInnerDay<N, Output = PnlRes<K>>,
{
    type Output = Vec<PnlRes<K>>;
    fn da(&self) -> Self::Output {
        self.map(|x| x.as_ref().da())
    }
}

impl<T, N, K, G> PnlSumInnerDay<InfoPnlRes<N, K>> for [T]
where
    T: AsRef<InfoPnlRes<N, K>>,
    N: Clone,
    PnlRes<K>: PnlSumInnerDay<K, Output = PnlRes<G>>,
{
    type Output = Vec<InfoPnlRes<N, G>>;
    fn da(&self) -> Self::Output {
        self.map(|x| {
            let x = x.as_ref();
            InfoPnlRes(x.0.clone(), x.1.da())
        })
    }
}

pub trait PnlSumBetweenTicker<T> {
    fn pnl_sum_between_ticker(&self) -> PnlRes<dt>;
}

impl<T> PnlSumBetweenTicker<PnlRes<dt>> for [T]
where
    T: AsRef<PnlRes<dt>>,
{
    fn pnl_sum_between_ticker(&self) -> PnlRes<dt> {
        let pnl_vec_union = self
            .iter()
            .map(|x| x.as_ref())
            .collect_vec()
            .union_pnl_res();
        let pnl_vec_value = pnl_vec_union.iter().fold(
            vec![vec![0f32; pnl_vec_union[0].0.len()]; pnl_vec_union[0].1.len()],
            |mut res, row| {
                izip!(res.iter_mut(), row.1.iter())
                    .enumerate()
                    .for_each(|(i, (x, y))| {
                        let kk = x.iter_mut().zip(y.iter());
                        if i == 3 || i == 2 {
                            kk.for_each(|(x, y)| *x += y.abs());
                        } else {
                            kk.for_each(|(x, y)| *x += y);
                        }
                    });
                res
            },
        );
        PnlRes(pnl_vec_union[0].0.clone(), pnl_vec_value)
    }
}

impl<T, N> PnlSumBetweenTicker<InfoPnlRes<N, dt>> for [T]
where
    T: AsRef<InfoPnlRes<N, dt>>,
{
    fn pnl_sum_between_ticker(&self) -> PnlRes<dt> {
        self.iter()
            .map(|x| &x.as_ref().1)
            .collect_vec()
            .pnl_sum_between_ticker()
    }
}

pub trait UnionPnlRes<T> {
    type Output;
    fn union_pnl_res(&self) -> Self::Output;
}

impl<T> UnionPnlRes<PnlRes<dt>> for [T]
where
    T: AsRef<PnlRes<dt>>,
{
    type Output = Vec<PnlRes<dt>>;
    fn union_pnl_res(&self) -> Self::Output {
        let time_vec = self
            .iter()
            .map(|x| &x.as_ref().0)
            .collect_vec()
            .union_vecs();
        self.map(|x| {
            let ri = Reindex::new(&x.as_ref().0, &time_vec);
            let value_vec = x
                .as_ref()
                .1
                // .par_iter()
                .iter()
                .enumerate()
                .map(|(i, data)| {
                    let g = ri.reindex(data);
                    if i == 2 {
                        let mut z = g.ffill(0f32);
                        z.fillna(0f32);
                        z
                    } else {
                        g.fillna(0f32)
                    }
                })
                .collect::<Vec<_>>();
            // .collect_vec();
            PnlRes(time_vec.clone(), value_vec)
        })
    }
}

impl<T> UnionPnlRes<PnlRes<da>> for [T]
where
    T: AsRef<PnlRes<da>>,
{
    type Output = Vec<PnlRes<da>>;
    fn union_pnl_res(&self) -> Self::Output {
        let time_vec = self
            .iter()
            .map(|x| &x.as_ref().0)
            .collect_vec()
            .union_vecs();
        self.map(|x| {
            let ri = Reindex::new(&x.as_ref().0, &time_vec);
            let value_vec = x.as_ref().1.map(|x| ri.reindex(x).fillna(0f32));
            PnlRes(time_vec.clone(), value_vec)
        })
    }
}

impl<T, N, K> UnionPnlRes<InfoPnlRes<N, K>> for [T]
where
    N: Clone,
    T: AsRef<InfoPnlRes<N, K>>,
    for<'a> [&'a PnlRes<K>]: UnionPnlRes<PnlRes<K>, Output = Vec<PnlRes<K>>>,
{
    type Output = Vec<InfoPnlRes<N, K>>;
    fn union_pnl_res(&self) -> Self::Output {
        let (k_vec, v_vec) = self.iter().fold((vec![], vec![]), |mut accu, x| {
            accu.0.push(x.as_ref().0.clone());
            accu.1.push(&x.as_ref().1);
            accu
        });
        let v_vec_new = v_vec.union_pnl_res();
        izip!(k_vec, v_vec_new)
            .map(|(x, y)| InfoPnlRes(x, y))
            .collect_vec()
    }
}

/* #region PnlSum */
pub trait PnlSum<T> {
    type Output;
    fn sum(&self) -> Self::Output;
}

impl<T, N> PnlSum<PnlRes<N>> for [T]
where
    T: AsRef<PnlRes<N>>,
    for<'a> [&'a PnlRes<N>]: UnionPnlRes<PnlRes<N>, Output = Vec<PnlRes<N>>>,
    N: Clone + 'static,
{
    type Output = PnlRes<N>;
    fn sum(&self) -> Self::Output {
        let pnl_vec_union = self
            .iter()
            .map(|x| x.as_ref())
            .collect_vec()
            .union_pnl_res();
        let pnl_vec_value = pnl_vec_union.iter().fold(
            vec![vec![0f32; pnl_vec_union[0].0.len()]; pnl_vec_union[0].1.len()],
            |mut res, row| {
                izip!(res.iter_mut(), row.1.iter()).for_each(|(x, y)| {
                    izip!(x.iter_mut(), y.iter()).for_each(|(x_i, y_i)| *x_i += y_i);
                });
                res
            },
        );
        PnlRes(pnl_vec_union[0].0.clone(), pnl_vec_value)
    }
}

impl<T, N, K> PnlSum<InfoPnlRes<N, K>> for [T]
where
    T: AsRef<InfoPnlRes<N, K>>,
    for<'a> [&'a PnlRes<K>]: PnlSum<PnlRes<K>, Output = PnlRes<K>>,
    K: Clone + 'static,
{
    type Output = PnlRes<K>;
    fn sum(&self) -> Self::Output {
        let g = self.iter().map(|x| &x.as_ref().1).collect_vec();
        g.sum()
    }
}
/* #endregion */

impl<T> From<&Vec<T>> for PnlRes<da>
where
    T: AsRef<PnlRes<da>>,
{
    fn from(value: &Vec<T>) -> Self {
        value.iter().map(|x| x.as_ref()).collect_vec().sum()
    }
}
impl From<&Vec<PnlRes<dt>>> for PnlRes<da> {
    fn from(value: &Vec<PnlRes<dt>>) -> Self {
        value.da().iter().collect_vec().sum()
    }
}

impl<T: Clone> Mul<f32> for &PnlRes<T> {
    type Output = PnlRes<T>;
    fn mul(self, rhs: f32) -> Self::Output {
        PnlRes(
            self.0.clone(),
            self.1
                .iter()
                .map(|x| x.iter().map(|x| x * rhs).collect_vec())
                .collect_vec(),
        )
    }
}

impl<T: Clone> Mul<&Vec<f32>> for &PnlRes<T> {
    type Output = PnlRes<T>;
    fn mul(self, rhs: &Vec<f32>) -> Self::Output {
        PnlRes(
            self.0.clone(),
            self.1
                .iter()
                .map(|x| {
                    itertools::izip!(x.iter(), rhs.iter())
                        .map(|(x, y)| x * y)
                        .collect_vec()
                })
                .collect_vec(),
        )
    }
}

impl<T, N, K> Add<T> for &PnlRes<N>
where
    T: AsRef<PnlRes<N>>,
    N: Clone,
    for<'a> [&'a PnlRes<N>]: PnlSum<u16, Output = K>,
{
    type Output = K;
    fn add(self, rhs: T) -> Self::Output {
        [self, rhs.as_ref()].sum()
    }
}

#[derive(Clone, AsRef)]
pub struct InfoPnlRes<T, N>(pub T, pub PnlRes<N>);

impl<T> ToStralBare for Vec<InfoPnlRes<Stra, T>> {
    fn to_stral_bare(&self) -> crate::prelude::Stral {
        self.map(|x| x.0.clone()).to_stral_bare()
    }
}

pub trait GroupbyPnl<'a, T> {
    type Output1;
    type Output2;
    fn groupby<'b, N>(&'b self, s: T, f: N) -> Vec<InfoPnlRes<Self::Output1, Self::Output2>>
    where
        'b: 'a,
        N: Fn(&[&PnlRes<Self::Output2>]) -> PnlRes<Self::Output2>;
}

impl<'a, T, G> GroupbyPnl<'a, G> for PnlRes<T>
where
    Self: GetPart<ForCompare<dt>>,
    G: AsRef<[ForCompare<dt>]>,
{
    type Output1 = ForCompare<dt>;
    type Output2 = T;
    fn groupby<'b, N>(&'b self, s: G, _f: N) -> Vec<InfoPnlRes<Self::Output1, Self::Output2>>
    where
        'b: 'a,
        N: Fn(&[&PnlRes<Self::Output2>]) -> PnlRes<Self::Output2>,
    {
        s.as_ref()
            .iter()
            .map(|x| InfoPnlRes(x.clone(), self.get_part(x.clone())))
            .collect_vec()
    }
}

impl<'a, T, K, G, Z> GroupbyPnl<'a, T> for Vec<InfoPnlRes<K, G>>
where
    T: Fn(&K) -> Z,
    Z: Clone + PartialOrd,
    G: 'static,
{
    type Output1 = Z;
    type Output2 = G;
    fn groupby<'b, N>(&'b self, s: T, f: N) -> Vec<InfoPnlRes<Self::Output1, Self::Output2>>
    where
        'b: 'a,
        N: Fn(&[&PnlRes<Self::Output2>]) -> PnlRes<Self::Output2>,
    {
        let k_vec = self.iter().map(|x| s(&x.0)).collect_vec();
        let v_vec = self.iter().map(|x| &x.1).collect_vec();
        let grp_order = Grp::new_without_order(&k_vec);
        let res = grp_order.apply_without_order(&v_vec, |x| f(x));
        izip!(res.0.into_iter(), res.1.into_iter())
            .map(|(x, y)| InfoPnlRes(x, y))
            .collect_vec()
    }
}

// impl<T, N, K> IndexCompare<InfoPnlRes<K, N>> for T
// where
//     T: IndexCompare<K>,
// {
//     fn eq(&self, other: &InfoPnlRes<K, N>) -> bool {
//         self.eq(&other.0)
//     }
// }

// impl<T: Clone, N: Clone> GetIndex<InfoPnlRes<T, N>> for Vec<InfoPnlRes<T, N>> {
//     fn get_index<K>(&self, other: K) -> Self
//     where
//         K: IndexCompare<InfoPnlRes<T, N>>,
//     {
//         self.iter()
//             .filter_map(|x| if other.eq(x) { Some(x.clone()) } else { None })
//             .collect_vec()
//     }
// }

impl<T> From<&Vec<InfoPnlRes<T, da>>> for PnlRes<da> {
    fn from(value: &Vec<InfoPnlRes<T, da>>) -> Self {
        value.iter().map(|x| &x.1).collect_vec().sum()
    }
}

// impl From<&Vec<&PnlRes<da>>> for PnlRes<da> {
//     fn from(value: &Vec<&PnlRes<da>>) -> Self {
//         value.sum()
//     }
// }

pub trait ConcatPnl {
    type Output;
    fn concat_pnl(self) -> Self::Output;
}

impl<T: Clone> ConcatPnl for (PnlRes<T>, PnlRes<T>) {
    type Output = PnlRes<T>;
    fn concat_pnl(self) -> Self::Output {
        PnlRes(
            [self.0.0, self.1.0].concat(),
            izip!(self.0.1.into_iter(), self.1.1.into_iter())
                .map(|(x, y)| [x, y].concat())
                .collect_vec()
        )
    }
}

impl<T, N: Clone> ConcatPnl for (InfoPnlRes<T, N>, InfoPnlRes<T, N>) {
    type Output = InfoPnlRes<T, N>;
    fn concat_pnl(self) -> Self::Output {
        InfoPnlRes(
            self.0.0,
            (self.0.1, self.1.1).concat_pnl(),
        )
    }
}

impl<T> ConcatPnl for (Vec<T>, Vec<T>)
where
    (T, T): ConcatPnl<Output = T>,
{
    type Output = Vec<T>;
    fn concat_pnl(self) -> Self::Output {
        izip!(self.0.into_iter(), self.1.into_iter())
            .map(|(x, y)| (x, y).concat_pnl())
            .collect_vec()
    }
}