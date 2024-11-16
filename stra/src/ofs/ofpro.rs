use qust::prelude::*;
// use super::of::{OrderFlowPrice2, self };
use qust_derive::*;
// use crate::econd::*;
use chrono::{Timelike, Duration};

#[derive(Default)]
struct UpdateOrderFlowOnce {
    kline_state: KlineWithState,
    to_pass: bool,
    to_update: bool,
}

impl UpdateDataState<TickData> for UpdateOrderFlowOnce {
    fn update(&mut self, data: &TickData) {
        if self.to_update && !self.to_pass {
            self.kline_state.data.update_begin(data);
            self.kline_state.current = KlineState::Finished;
            self.kline_state.last = KlineState::Finished;
        } else {
            self.kline_state.current = KlineState::Ignor;
            self.kline_state.update(data);
        }
    }
}

#[ta_derive2]
pub struct OrderFlow;

#[typetag::serde(name = "ofpro_orderflow")]
impl Tri for OrderFlow {
    fn update_tick_func(&self, _ticker: Ticker) -> UpdateFuncTick {
        let mut kline      = UpdateOrderFlowOnce::default();
        let mut last_ask   = f32::NAN;
        let mut last_bid   = f32::NAN;
        Box::new(move |tick_data, price_ori| {
            let h               = tick_data.t.hour();
            let is_to_pass      = tick_data.v == 0. || h == 20 || h == 8;
            let of_triggered    = tick_data.c > last_ask || tick_data.c < last_bid;
            kline.to_pass   = is_to_pass;
            kline.to_update = of_triggered;
            kline.kline_state.data.ki.pass_last = 1;
            kline.update(tick_data);
            if let KlineState::Finished = kline.kline_state.last {
                let of_dire = if tick_data.c > last_ask { 1. } else { -1. };
                price_ori.immut_info.push(vec![vec![of_dire]]);
                price_ori.update(&kline.kline_state.data);
                kline.kline_state.data.ki.pass_last = 1;
            }
            last_ask = tick_data.ask1;
            last_bid = tick_data.bid1;
            kline.kline_state.last.clone()
        })
    }
}

#[derive(Default)]
struct UpdateOrderFlow {
    inter_update: KlineStateInter,
    kline_promt: bool,
    next_end: Option<tt>, 
}

impl UpdateDataState<KlineData> for UpdateOrderFlow {
    fn update(&mut self, data: &KlineData) {
        use KlineState::*;
        let is_jumped = match self.next_end {
            None => false,
            Some(i) => data.t.time() > i,
        };
        match (&self.inter_update.kline_state.last, self.kline_promt, is_jumped) {
            (Finished, _, true) => {
                if Interval::get_time_end(&self.inter_update.intervals, &data.t.date(), &data.t.time()).is_some() {
                    let kline = &mut self.inter_update.kline_state;
                    kline.current = Begin;
                    kline.update(data);
                    kline.current = Finished;
                    kline.last = Finished;
                    kline.data.ki.pass_this += data.ki.pass_last + data.ki.pass_this;
                }
            }
            (Begin, true, _) => {
                self.inter_update.kline_state.current = Finished;
                self.inter_update.kline_state.update(data);
                self.inter_update.record.1 = 0;
            }
            (Finished, true, _) => {
                self.inter_update.kline_state.current = Ignor;
                self.inter_update.kline_state.update(data);
            }
            _ => {
                self.inter_update.update(data);
            }
        }
        if let KlineState::Finished = self.inter_update.kline_state.last {
            let time_next = data.t.time() + Duration::microseconds(500);
            self.next_end = self
                .inter_update
                .intervals
                .iter()
                .position(|x| {
                x.is_in(&data.t.date(), &time_next)
            })
                .map(|i| self.inter_update.intervals[i].end_time());
        }
    }
}

// #[typetag::serde(name = "tri_order_flow_Price2")]
// impl Pri for OrderFlowPrice2 {
//     fn update_kline_func(&self, _di: &Di,price: &PriceArc) -> UpdateFuncKline {
//         let mut kline = UpdateOrderFlow {
//             inter_update: KlineStateInter::from_intervals(self.0.intervals()),
//             ..Default::default()
//         };
//         let mut last_info: bm<i32, (f32, f32)> = Default::default();
//         let immut_info = price.immut_info.clone();
//         let f_last_time = of::inter::exit_by_last_time(0, 100).get_func(&self.0);
//         Box::new(move |kline_data, price_ori, i| {
//             let finish_triggered = f_last_time(&kline_data.t);
//             kline.kline_promt = finish_triggered;
//             kline.update(kline_data);
//             if let KlineState::Begin | KlineState::Merging | KlineState::Finished = kline.inter_update.kline_state.last {
//                 let vv = last_info.entry((kline_data.c * 100.) as i32).or_default();
//                 if immut_info[i].first().unwrap().first().unwrap() == &1. {
//                     vv.0 += kline_data.v;
//                 } else {
//                     vv.1 += kline_data.v;
//                 }
//                 if let KlineState::Finished = kline.inter_update.kline_state.last {
//                     let interval_info = last_info
//                         .iter()
//                         .fold(init_a_matrix(last_info.len(), 3), |mut accu, (k, v)| {
//                             accu[0].push(*k as f32 / 100.);
//                             accu[2].push(v.0);
//                             accu[1].push(v.1);
//                             accu
//                         });
//                     price_ori.immut_info.push(interval_info);
//                     price_ori.update(&kline.inter_update.kline_state.data);
//                     last_info.clear();
//                 }
//             }
//             kline.inter_update.kline_state.last.clone()
//         })
//     }
// }

/*
#[ta_derive2]
struct ta;

#[typetag::serde(name = "ofpro_of_ta")]
impl Ta for ta {
    fn calc_di(&self,di: &Di) -> avv32 {
        let data = di.immut_info()
            .iter()
            .fold((Vec::with_capacity(di.len()), Vec::with_capacity(di.len())), |mut accu, x| {
                let bid = x[1].iter().sum();
                let ask = x[2].iter().sum();
                accu.0.push(bid);
                accu.1.push(ask);
                accu
            });
        vec![data.0.into(), data.1.into(), di.l(), di.h()]
    }

    fn calc_da(&self,da:Vec< &[f32]> ,_di: &Di) -> vv32 {
        let mut res = vec![0.; da[0].len()];
        let (sell_att, h) = (da[1], da[3]);
        for i in 2..da[0].len() {
            if sell_att[i] > 14.8 && sell_att[i - 1] > 14.8 && h[i] <= h[i - 1] && h[i - 1] <= h[i - 2] && da[0][i] + da[0][i - 1] < 300. {
                res[i] = 1.;
            }
        }
        vec![res]
    }
}

#[ta_derive2]
pub struct cond;

lazy_static! {
    pub static ref convert: Convert = {
        of::OrderFlowPrice2(of::inter::ofm14.clone()).pri_box().pip(Event)
    };
}

#[typetag::serde(name = "ofpro_cond")]
impl Cond for cond {
    fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
        let pms = ori + convert.clone() + oos + ta;
        let data = di.calc(pms)[0].clone();
        Box::new(move |e, _o| {
            data[e] == 1.
        })
    }
}

#[ta_derive2]
pub struct cond2;

#[typetag::serde(name = "ofpro_cond2")]
impl Cond for cond2 {
    fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
        let data = di.calc(ori + oos + ta);
        Box::new(move |e, _o| {
            e > 2
                && data[1][e] > 14.8 && data[1][e - 1] > 14.8 
                && data[3][e] <= data[3][e - 1] 
                && data[3][e - 1] <= data[3][e - 2] 
                && data[1][e] + data[1][e - 1] < 300.
        })
    }
}
*/