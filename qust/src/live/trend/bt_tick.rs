#![allow(clippy::collapsible_if)]
use std::sync::{Arc, RwLock};
use std::thread;
use itertools::{izip, Itertools};
use crate::loge;
use crate::prelude::*;
// use qust_derive::*;
// use super::cond_ops::{ ApiType, CondType1, CondType7, CondType8, RetFnCondType1, RetFnCondType8, StreamApiType, StreamBtMatch, WithAlgoBox, WithMatchBox };
use super::api::*;
use qust_ds::prelude::*;
use super::super::order_types::*;
use super::super::bt::*;


#[derive(Debug, Clone)]
pub struct BtWrapper<T>(pub T);

impl CondType1 for BtWrapper<Ptm> {
    fn cond_type1(&self,di: &Di) -> RetFnCondType1 {
        let b = di.calc(&self.0);
        let ptm_res = b
            .downcast_ref::<RwLock<PtmResState>>()
            .unwrap()
            .read()
            .unwrap()
            .ptm_res
            .0
            .iter()
            .map(|x| OrderTarget::from(x.clone()))
            .collect_vec();
        Box::new(move |stream_api_type1| {
            let i = stream_api_type1.di_kline_state.di_kline.i;
            ptm_res[i].clone()
        })
    }
}

impl CondType1 for BtWrapper<Stral> {
    fn cond_type1(&self, di: &Di) -> RetFnCondType1 {
        let ptm_res = self
            .0
            .0
            .iter()
            .fold(vec![OrderTarget::No; di.size()], |mut accu, x| {
                let b = di.calc(&x.ptm);
                let ptm_res_part = &b
                    .downcast_ref::<RwLock<PtmResState>>()
                    .unwrap()
                    .read()
                    .unwrap()
                    .ptm_res;
                accu
                    .iter_mut()
                    .zip(ptm_res_part.0.iter())
                    .for_each(|(x, y)| {
                        *x = x.add_live_target(&y.clone().into());
                    });
                accu
            });
        Box::new(move |stream_api_type1| {
            let i = stream_api_type1.di_kline_state.di_kline.i;
            ptm_res[i].clone()
        })
    }
}

pub struct DiTick<'a> {
    pub di: &'a Di,
    pub tick: &'a [TickData],
}

#[allow(dead_code)]
#[derive(Debug)]
struct KlineRange {
    time_open: dt,
    time_close: dt,
    i: usize,
}




impl<N: Algo> ApiType for WithInfo<Box<dyn CondType7>, N> {
    fn api_type(&self) -> RetFnApi {
        let mut ops_fn = self.data.cond_type7();
        let mut algo_fn = self.info.algo();
        Box::new(move |stream_api| {
            let order_target = ops_fn(stream_api.tick_data);
            let stream_algo = StreamAlgo {
                stream_api,
                order_target,
            };
            algo_fn(&stream_algo)
        })
    }
}

impl<T> CondType8 for WithMatchBox<T> 
where
    T: ApiType,
{
    fn cond_type8(&self) -> RetFnCondType8 {
        let mut ops_fn = self.data.api_type();
        let mut match_fn = self.info.bt_match();
        let mut hold = Hold::default();
        let mut last_order_action = OrderAction::default();
        Box::new(move |tick_data| {
            if tick_data.ask1 == 0. || tick_data.bid1 == 0. {
                return None;
            }
            let stream_bt_match = StreamBtMatch {
                tick_data,
                hold: &mut hold,
                order_action: &last_order_action.clone(),
            };
            let res = match_fn(stream_bt_match);
            let stream_api = StreamApiType {
                tick_data,
                hold: &hold,
            };
            last_order_action = ops_fn(stream_api);
            res
        })
    }
}

impl<'a, T> BtTick<&'a [TickData]> for T
where
    T: CondType8,
{
    type Output = Vec<TradeInfo>;
    fn bt_tick(&self, input: &'a [TickData]) -> Self::Output {
        let mut res = vec![];
        let mut ops_fn = self.cond_type8();
        for tick_data in input.iter() {
            if let Some(trade_info) = ops_fn(tick_data) {
                res.push(trade_info);
            }
        }
        res
    }
}

impl<T: CondType1> CondTypeA for WithInfo<T, RwLock<Di>> {
    fn cond_type_a(&self) -> RetFnCondType3 {
        let mut di = self.info.write().unwrap();
        let pcon_ident = di.pcon.ident();
        let mut ptm_fn = self.data.cond_type1(&di);
        let mut update_tick_fn = pcon_ident.inter.update_tick_func(pcon_ident.ticker);
        let mut last_update_tick_time = Default::default();//maybe the update come from hold update
        Box::new(move |stream_api| {
            let is_finished = if stream_api.tick_data.t > last_update_tick_time {
                last_update_tick_time = stream_api.tick_data.t;
                update_tick_fn(stream_api.tick_data, &mut di.pcon.price).into()
            } else {
                false
            };
            if is_finished {
                di.clear2();
                loge!(pcon_ident.ticker, "{:?} pcon finished", pcon_ident.inter);
            }
            let stream_cond_type0 = StreamCondType1 {
                stream_api: stream_api.clone(),
                di_kline_state: DiKlineState { 
                    di_kline: DiKline { di: &di, i: di.size() - 1 }, 
                    state: is_finished 
                },
            };
            ptm_fn(&stream_cond_type0)
        })
    }
}


impl CondTypeA for WithTicker<Vec<WithInfo<Stral, RwLock<Di>>>> {
    fn get_ticker(&self) -> Ticker {
        self.info
    }
    fn cond_type_a(&self) -> RetFnCondType3 {
        let mut ops_vec = vec![];
        for ops in self.data.iter() {
            ops_vec.push(ops.cond_type_a());
        }
        Box::new(move |stream_api| {
            let mut live_target = OrderTarget::No;
            for ops in ops_vec.iter_mut() {
                let live_target_stra = ops(stream_api);
                live_target = live_target.add_live_target(&live_target_stra);
            }
            live_target
        })
    }
}

impl<'a, T> CondTypeA for WithInfo<T, &'a Di>
where
    T: CondType1,
{
    fn cond_type_a(&self) -> RetFnCondType3 {
        let di = self.info;
        let mut ops_fn = self.data.cond_type1(di);
        let mut kline_range_vec = izip!(di.pcon.price.ki.iter(), di.pcon.price.t.iter(), 0..)
            .map(|(x, y, z)| {
                KlineRange { 
                    time_open: x.open_time,
                    time_close: *y,
                    i: z,
                }
            });
        let mut kline_range = kline_range_vec.next().unwrap();
        let di_size = di.size();
        let mut last_live_target = OrderTarget::No;
        Box::new(move |stream_api| {
            let tick_data = stream_api.tick_data;
            let hold = stream_api.hold;
            let mut finished = false;
            while tick_data.t >= kline_range.time_close {
                match kline_range_vec.next() {
                    Some(k_next) => {
                        kline_range = k_next;
                        finished = true;
                    }
                    None => {
                        kline_range.i = di_size;
                        break;
                    }
                }
            }
            if kline_range.i < 100 {
                return last_live_target.clone();
            }
            let i = kline_range.i - 1;
            let stream_api = StreamApiType { tick_data, hold };
            let di_kline = DiKline { di, i };
            let di_kline_state = DiKlineState { di_kline, state: finished };
            let stream_cond_type1 = StreamCondType1 { stream_api: stream_api.clone(), di_kline_state };
            last_live_target = ops_fn(&stream_cond_type1);
            last_live_target.clone()
        })
    }
}

pub struct CondType7Wrapper(Box<dyn CondType7>);
impl CondTypeA for CondType7Wrapper {
    fn cond_type_a(&self) -> RetFnCondType3 {
        let mut ops_fn = self.0.cond_type7();
        Box::new(move |stream_api| {
            ops_fn(stream_api.tick_data)
        })
    }
}


impl<T> ApiType for WithAlgoBox<T>
where
    T: CondTypeA + Send + Sync,
{
    fn api_type(&self) -> RetFnApi {
        let mut ops_fn = self.data.cond_type_a();
        let mut algo_fn = self.info.algo();
        Box::new(move |stream_api| {
            let order_target = ops_fn(&stream_api);
            let stream_algo = StreamAlgo { stream_api, order_target };
            algo_fn(&stream_algo)
        })
    }
}

impl<'a> ApiType for WithInfo<Box<dyn CondType4>, &'a Di> {
    fn api_type(&self) -> RetFnApi {
        let mut di = self.info.clone();
        let mut ops_fn = self.data.cond_type4(&di);
        let mut update_tick_fn = di.pcon.inter.update_tick_func(di.pcon.ticker);
        let mut last_update_tick_time = Default::default();
        Box::new(move |stream_api| {
            let is_finished = if stream_api.tick_data.t > last_update_tick_time {
                last_update_tick_time = stream_api.tick_data.t;
                update_tick_fn(stream_api.tick_data, &mut di.pcon.price).into()
            } else {
                false
            };
            if is_finished {
                di.clear2();
            }
            let stream_cond_type1 = StreamCondType1 {
                stream_api: stream_api.clone(),
                di_kline_state: DiKlineState { 
                    di_kline: DiKline { di: &di, i: di.size() - 1 }, 
                    state: is_finished 
                },
            };
            ops_fn(&stream_cond_type1)
        })
    }
}

impl<'a> BtTick<(&'a Di, &'a [TickData])> for WithMatchBox<Box<dyn CondType4>> {
    type Output = Vec<TradeInfo>;
    fn bt_tick(&self, input: (&'a Di, &'a [TickData])) -> Self::Output {
        let with_match_box = WithMatchBox {
            data: WithInfo {
                data: self.data.clone(),
                info: input.0,
            },
            info: self.info.clone(),
        };
        with_match_box.bt_tick(input.1)
    }
}


pub struct BtTickInput<T, N, K> {
    pub ops1: T,
    pub ops2: N,
    pub data: K,
}


impl<'a> BtTick<BtTickInput<AlgoBox, BtMatchBox, &'a hm<Ticker, Vec<TickData>>>> for DiStral<'_> {
    type Output = Vec<InfoPnlRes<Ticker, dt>>;
    fn bt_tick(&self, input: BtTickInput<AlgoBox, BtMatchBox, &'a hm<Ticker, Vec<TickData>>>) -> Self::Output {
        thread::scope(|scope| {
            let mut handles = vec![];
            for (di, index_vec) in self.dil.dil.iter().zip(self.index_vec.iter()) {
                if index_vec.is_empty() { 
                    continue;
                }
                let stra_ops = index_vec.iter()
                    .map(|&i| self.stral.0[i].clone())
                    .collect_vec()
                    .pip(Stral)
                    .pip(BtWrapper)
                    .with_info(di)
                    .with_info(input.ops1.clone())
                    .with_info(input.ops2.clone());
                let ticker = di.pcon.ticker;
                let tick = match input.data.get(&ticker) {
                    Some(tick) => tick,
                    None => {
                        println!("tick data not contains: {:?}", ticker);
                        continue;
                    }
                };
                let handle = scope.spawn(move || {
                    stra_ops
                        .bt_tick(tick)
                        .with_info(ticker)
                        .into_pnl_res()
                        .pip(|x| InfoPnlRes(ticker, x))
                });
                handles.push(handle);
            } 
            let mut res = vec![];
            for handle in handles.into_iter() {
                let res_part = handle.join();
                match res_part {
                    Ok(data) => {
                        res.push(data);
                    }
                    Err(e) => {
                        println!("{:?}", e);
                    }
                }
            }
            res
        })
    }
}

impl<'a> BtTick<BtTickInput<BtMatchBox, AlgoBox, &'a hm<Ticker, Vec<TickData>>>> for DiStral<'_> {
    type Output = Vec<InfoPnlRes<Stra, dt>>;
    fn bt_tick(&self, input: BtTickInput<BtMatchBox, AlgoBox, &'a hm<Ticker, Vec<TickData>>>) -> Self::Output {
        thread::scope(|scope| {
            let mut handles = vec![];
            for (i, stra) in self.stral.0.iter().enumerate() {
                let ticker = stra.ident.ticker;
                let di_opt = self.dil.get_idx(OnlyOne(stra.ident.clone()));
                let Some(di) = di_opt else {
                    println!("not found di: {:?}", stra.ident);
                    continue;
                };
                let stra_ops = BtWrapper(stra.ptm.clone())
                    .with_info(di)
                    .with_info(input.ops2.clone())
                    .with_info(input.ops1.clone());
                let Some(tick_data) = input.data.get(&ticker) else {
                    println!("not found tick data: {:?}", ticker);
                    continue
                };
                let stra = stra.clone();
                let handle = scope.spawn(move || {
                    let res = stra_ops
                        .bt_tick(tick_data)
                        .with_info(ticker)
                        .into_pnl_res();
                    InfoPnlRes(stra.clone(), res)
                });
                handles.push((i, handle));
            }
            let mut res = vec![];
            for handle in handles.into_iter() {
                let res_part = handle.1.join();
                match res_part {
                    Ok(data) => {
                        res.push(data);
                    }
                    Err(e) => {
                        println!("----------- {} {:?}", handle.0, e);
                    }
                }
            }
            res
        })
    }
}

impl<'a> BtTick<BtTickInput<AlgoBox, BtMatchBox, (&'a Di, &'a [TickData])>> for Ptm {
    type Output = Vec<TradeInfo>;
    fn bt_tick(&self, input: BtTickInput<AlgoBox, BtMatchBox, (&'a Di, &'a [TickData])> ) -> Self::Output {
        BtWrapper(self.clone())
            .with_info(input.data.0)
            .with_info(input.ops1)
            .with_info(input.ops2)
            .bt_tick(input.data.1)
    }
} 

impl<'a> BtTick<(BtMatchBox, &'a Di, &'a [TickData])> for Ptm {
    type Output = Vec<TradeInfo>;
    fn bt_tick(&self, input: (BtMatchBox, &'a Di, &'a [TickData])) -> Self::Output {
        self.bt_tick(BtTickInput {
            ops1: AlgoTarget.algo_box(),
            ops2: input.0,
            data: (input.1, input.2)
        })
    }
}

impl<'a> BtTick<(BtMatchBox, &'a Dil, &'a hm<Ticker, Vec<TickData>>)> for Stra {
    type Output = Vec<TradeInfo>;
    fn bt_tick(&self, input: (BtMatchBox, &'a Dil, &'a hm<Ticker, Vec<TickData>>)) -> Self::Output {
        let di = input.1.get_idx(OnlyOne(self.ident.clone())).unwrap();
        let tick_data = &input.2.get(&self.ident.ticker).unwrap()[..];
        self.ptm.bt_tick((input.0, di, tick_data))
    }
}

impl IntoPnlRes for TickerTradeInfo {
    fn into_pnl_res(self) -> PnlRes<dt> {
        let res_size = self.data.len();
        let mut t = Vec::with_capacity(res_size);
        let mut c = Vec::with_capacity(res_size);
        let mut norm_hold = Vec::with_capacity(res_size);
        let mut norm_open = Vec::with_capacity(res_size);
        let mut norm_exit = Vec::with_capacity(res_size);
        let mut state = NormHold::No;
        for order_action in self.data.into_iter() {
            let (open_now, exit_now, price) = match order_action.action {
                OrderAction::LoOpen(i, price) => {
                    let norm_open = NormOpen::Lo(i);
                    state = state.add_norm_hold(&NormHold::Lo(i));
                    (norm_open, NormExit::No, price)
                }
                OrderAction::ShOpen(i, price) => {
                    let norm_open = NormOpen::Sh(i);
                    state = state.add_norm_hold(&NormHold::Sh(i));
                    (norm_open, NormExit::No, price)
                }
                OrderAction::LoClose(i, price) => {
                    let norm_exit = NormExit::Lo(i);
                    state = state.add_norm_hold(&NormHold::Lo(i));
                    (NormOpen::No, norm_exit, price)
                }
                OrderAction::ShClose(i, price) => {
                    let norm_exit = NormExit::Sh(i);
                    state = state.add_norm_hold(&NormHold::Sh(i));
                    (NormOpen::No, norm_exit, price)
                }
                _ => panic!("not implemetnted"),
            };
            t.push(order_action.time);
            c.push(price);
            norm_hold.push(state.clone());
            norm_open.push(open_now);
            norm_exit.push(exit_now);
        }
        let profit = {
            let c_lag = c.lag(1f32);
            let mut res = izip!(c.iter(), c_lag.iter())
                .map(|(x, y)| {
                    x / y - 1.
                })
                .collect_vec();
            res[0] = 0f32;
            res
        };
        let mut pass_num = izip!(t.iter(), t.lag(1f32).iter())
            .map(|(x, y)| ((*x - *y).num_seconds() as f32 + 0.5) / 60.)
            .collect_vec();
        pass_num.remove(0);
        PnlResPreInfo {
            ticker: self.info,
            t,
            c: Arc::new(c),
            profit, 
            comm: cs2,
            pass_num,
            ptm_res: &(norm_hold, norm_open, norm_exit)
        }.into_pnl_res()
    }
}