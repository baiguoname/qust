use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::RwLock;
use itertools::Itertools;
use crate::sig::livesig::LiveSig;
use crate::sig::posi::*;
use crate::trade::di::Di;
use crate::sig::cond::Cond;
use crate::idct::calc::{BoxAny, Calc};
use crate::ds::types::*;


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TradeIndex {
    Lo(usize, usize),
    Sh(usize, usize),
}

impl TradeIndex {
    pub fn inner_index(&self) -> (usize, usize) {
        match &self {
            TradeIndex::Lo(i, j) => (*i, *j),
            TradeIndex::Sh(i, j) => (*i, *j),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Oei<T>(pub T);
pub type OeiRes = (usize, usize, Vec<TradeIndex>);

impl<T> LiveSig for Oei<T>
where
    T: LiveSig<R = (HashSet<OpenIng>, TsigRes)> + Clone + Debug,
{
    type R = OeiRes;
    fn init(&self, di: &mut Di) {
        di.calc_init(&self.0);
    }

    fn get_data(&self, di: &Di) -> RwLock<Self::R> {
        RwLock::new((di.len(), 0, vec![]))
    }

    fn update(&self, di: &Di, data: &RwLock<Self::R>) {
        let mut res = data.write().unwrap();
        let sigr = di.calc_sig(&self.0);
        for _i in 0..res.0 {
            res.2.pop();
        }
        for i in res.1..sigr.1 .1.len() {
            let exit_set = &sigr.1 .1[i];
            match &exit_set {
                Exit::Sh(s) => {
                    for &s_ in s.iter().sorted() {
                        res.2.push(TradeIndex::Lo(s_, i));
                    }
                }
                Exit::Lo(s) => {
                    for &s_ in s.iter().sorted() {
                        res.2.push(TradeIndex::Sh(s_, i));
                    }
                }
                _ => {}
            }
        }
        res.0 = res.2.len();
        res.1 = sigr.1 .1.len();
        for &i in sigr.0.iter() {
            match i {
                OpenIng::Lo(j) => {
                    res.2.push(TradeIndex::Lo(j, sigr.1 .1.len()));
                }
                OpenIng::Sh(j) => {
                    res.2.push(TradeIndex::Sh(j, sigr.1 .1.len()));
                }
            }
        }
    }
}

/* #endregion */

/* #region Oei Filter */
use crate::sig::cond::Iocond;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OeiFilter<T, N>(pub T, pub N);

impl<T, N> LiveSig for OeiFilter<T, Iocond<N>>
where
    T: LiveSig<R = (usize, usize, Vec<TradeIndex>)> + Clone + Debug,
    Iocond<N>: Cond,
    N: Calc<vv32> + Clone + 'static,
{
    type R = Vec<TradeIndex>;
    fn init(&self, di: &mut Di) {
        di.calc_init(&self.0);
        di.calc_init(&self.1.pms);
    }

    fn get_data(&self, _di: &Di) -> RwLock<Self::R> {
        RwLock::new(vec![])
    }

    fn update(&self, di: &Di, data: &RwLock<Self::R>) {
        let mut res = data.write().unwrap();
        res.clear();
        let oei_res = &di.calc_sig(&self.0).2;
        let f_cond = self.1.cond(di);
        for trade_index in oei_res.iter() {
            let (i, _j) = trade_index.inner_index();
            if f_cond(i, i) {
                res.push(trade_index.clone());
            }
        }
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CondWeight<T, N>(pub T, pub N);

impl<T> LiveSig for CondWeight<T, f32>
where
    T: Cond + Clone,
{
    type R = v32;
    fn init(&self, di: &mut Di) {
        self.0.calc_init(di);
    }

    fn get_data(&self, di: &Di) -> RwLock<Self::R> {
        let init_len = di.len() + 500;
        RwLock::new(Vec::with_capacity(init_len))
    }

    fn update(&self, di: &Di, data: &RwLock<Self::R>) {
        let mut data = data.write().unwrap();
        let f = self.0.cond(di);
        for i in data.len()..di.len() {
            if f(i, i) {
                data.push(self.1);
            } else {
                data.push(0f32);
            }
        }
    }
}

impl<T, N> LiveSig for CondWeight<T, N>
where
    T: LiveSig<R = v32> + Calc<BoxAny> + Clone,
    N: LiveSig<R = v32> + Calc<BoxAny> + Clone,
{
    type R = v32;
    fn init(&self, di: &mut Di) {
        di.calc_init(&self.0);
        di.calc_init(&self.1);
    }
    fn get_data(&self, _di: &Di) -> RwLock<Self::R> {
        RwLock::new(vec![])
    }

    fn update(&self, di: &Di, data: &RwLock<Self::R>) {
        let mut data_res = data.write().unwrap();
        let data_pre = di.calc_sig(&self.0);
        let data_now = di.calc_sig(&self.1);
        for i in data_res.len()..data_now.len() {
            data_res.push(data_pre[i] + data_now[i])
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeIndexWeight(pub TradeIndex, pub f32);
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OeiWeight<T, N>(pub T, pub N);

impl<T, N> LiveSig for OeiWeight<T, N>
where
    T: LiveSig<R = OeiRes> + Calc<BoxAny> + Clone,
    N: LiveSig<R = v32> + Calc<BoxAny> + Clone,
{
    type R = (usize, usize, Vec<TradeIndexWeight>);
    fn init(&self, di: &mut Di) {
        di.calc_init(&self.0);
        di.calc_init(&self.1);
    }

    fn get_data(&self, _di: &Di) -> RwLock<Self::R> {
        RwLock::new((0, 0, vec![]))
    }

    fn update(&self, di: &Di, data: &RwLock<Self::R>) {
        let mut res = data.write().unwrap();
        let oei_res = &di.calc_sig(&self.0);
        let weight_res = di.calc_sig(&self.1);
        let keep_len = res.0;
        res.2.truncate(keep_len);
        for i in res.0..oei_res.2.len() {
            let trade_index = &oei_res.2[i];
            res.2.push(TradeIndexWeight(
                trade_index.clone(),
                weight_res[trade_index.inner_index().0],
            ))
        }
        res.0 = oei_res.0;
    }
}

/* #region Get TradeIndex Vec */
trait GetTradeIndexVec {
    fn get(&self) -> &Vec<TradeIndex>;
}
impl GetTradeIndexVec for Vec<TradeIndex> {
    fn get(&self) -> &Vec<TradeIndex> {
        self
    }
}
impl GetTradeIndexVec for (usize, usize, Vec<TradeIndex>) {
    fn get(&self) -> &Vec<TradeIndex> {
        &self.2
    }
}
/* #endregion */

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stp4<T>(pub T);

impl<T> LiveSig for Stp4<T>
where
    T: LiveSig<R = (usize, usize, Vec<TradeIndexWeight>)> + Calc<BoxAny> + Clone,
{
    type R = (Vec<PosiWeight<Hold>>, Vec<PosiWeight<Open>>, Vec<PosiWeight<Exit>>);
    fn init(&self, di: &mut Di) {
        di.calc(&self.0);
    }

    fn get_data(&self, _di: &Di) -> RwLock<Self::R> {
        RwLock::new((vec![PosiWeight(Hold::No, 0.); _di.len()], 
        vec![PosiWeight(Open::No, 0.); _di.len()], vec![PosiWeight(Exit::No, 0.); _di.len()]))
    }

    fn update(&self, di: &Di, data: &RwLock<Self::R>) {
        let mut res = data.write().unwrap();
        let sigr = &di.calc_sig(&self.0).2;
        for trade_index in sigr.iter().skip(res.0.len()) {
            match trade_index.0 {
                TradeIndex::Lo(i, j) => {
                    for i_ in  i .. j - 1 {
                        res.0[i_] = PosiWeight(Hold::Lo(i), trade_index.1);
                    }
                    res.1[i] = PosiWeight(Open::Lo(i), trade_index.1);
                    let mut t_ = HashSet::new();
                    t_.insert(j);
                    res.2[j] = PosiWeight(Exit::Sh(t_), trade_index.1);
                },
                TradeIndex::Sh(i, j ) => {
                    for i_ in i .. j - 1 {
                        res.0[i_] = PosiWeight(Hold::Sh(i), trade_index.1);
                    }
                    res.1[i] = PosiWeight(Open::Sh(i), trade_index.1);
                    let mut t_ = HashSet::new();
                    t_.insert(j);
                    res.2[j] = PosiWeight(Exit::Lo(t_), trade_index.1);
                }
            }
        }
    }
}

/* #region OeiThan */
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OeiThan<T>(pub T);

impl<T> LiveSig for OeiThan<T>
where
    T: LiveSig<R = OeiRes> + Clone + Debug,
{
    type R = OeiRes;
    fn init(&self, di: &mut Di) {
        di.calc_init(&self.0);
    }

    fn get_data(&self, di: &Di) -> RwLock<Self::R> {
        RwLock::new((di.len(), 0, vec![]))
    }

    fn update(&self, di: &Di, data: &RwLock<Self::R>) {
        let mut res = data.write().unwrap();
        let sigr = di.calc_sig(&self.0);
        let keep_len = res.0;
        res.2.truncate(keep_len);
        let mut last_close_index = if res.2.is_empty() {
            0
        } else {
            res.2[res.2.len() - 1].inner_index().1
        };
        let mut temp_index = 0usize;
        for i in res.1..sigr.2.len() {
            let trade_index = &sigr.2[i];
            let (i, j) = trade_index.inner_index();
            if i > last_close_index {
                res.2.push(trade_index.clone());
                last_close_index = j;
                if i >= sigr.0 {
                    temp_index += 1;
                }
            }
        }
        res.0 = res.2.len() - temp_index;
        res.1 = sigr.1;
    }
}

/* #endregion */