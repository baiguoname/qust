use qust::prelude::*;
use qust_derive::*;

#[ta_derive]
pub struct TriVol2(pub f32);

// #[typetag::serde]
// impl Tri for TriVol2 {
//     fn tri<'a>(&self, di: &'a Di, _price: &PriceArc) -> UpdateFuncKlineIndex<'a> {
//         let mut last_high = 0.;
//         let mut last_low  = 0.;
//         let vol_data = di.calc(&vol_pms.clone())[0].clone();
//         let thre = self.0;
//         let mut kline = KlineWithState::default();
//         Box::new(
//             move |kline_data, price_ori, i| {
//                 match kline.last {
//                     KlineState::Finished => {
//                         last_high = kline.data.h;
//                         last_low = kline.data.l;
//                         if last_high / last_low - 1. <= vol_data[i] * thre {
//                             kline.current = KlineState::Begin;
//                             // price_ori.update(kline_data);
//                         }
//                     },
//                     _ => {
//                         last_high = last_high.max(kline.data.h);
//                         last_low = last_low.min(kline.data.l);
//                         if last_high / last_low - 1. > vol_data[i] * thre {
//                             kline.current = KlineState::Finished;
//                         } else {
//                             kline.current = KlineState::Merging;
//                         }
//                     }
//                 }
//                 kline.update_to(kline_data, price_ori);
//                 kline.last.clone()
//             }
//         )
//     }
// }

#[ta_derive]
pub struct TimeRangeSelect(pub Box<dyn Inter>);

// #[typetag::serde(name = "TimeRangeSelect")]
// impl PriceOriFromKlineData for TimeRangeSelect {
//     fn check_kline_func(&self) -> Box<dyn FnMut(&KlineData) -> KlineState> {
//         let intervals = self.0.intervals();
//         Box::new(move |kline_data| {
//             match Interval::get_time_end(&intervals, &kline_data.t.date(), &kline_data.t.time()) {
//                 Some(_) =>  KlineState::Finished,
//                 None => KlineState::Ignor,
//             }
//         })
//     }
// }

/*
#[ta_derive]
pub struct OrderFlow;

#[typetag::serde]
impl PriceOriFromTickData for OrderFlow {
    fn gen_price_ori(&self, price_tick: &PriceTick) -> PriceOri {
        let num_days = (price_tick.t.last().unwrap().date() - price_tick.t.first().unwrap().date()).num_days() as usize;
        let mut price_ori = PriceOri::with_capacity(num_days * 50);
        price_ori.immut_info = vec![];
        price_ori
    }
    fn check_tick_func(&self) -> Box<dyn FnMut(&TickData) -> KlineState> {
        todo!()
    }
    fn update_tick_func(&self, _ticker: Ticker) -> UpdateFuncTick {
        let mut kline = KlineData::news();
        let mut last_ask = f32::NAN;
        let mut last_bid = f32::NAN;
        let mut last_finished = true;
        Box::new(move |tick_data, price_ori| {
            if last_finished {
                tick_data.update_begin(&mut kline);
                last_finished = false;
            }
            let h = tick_data.t.hour();
            if (tick_data.c > last_ask || tick_data.c < last_bid) 
                && tick_data.v != 0. 
                && h != 20 
                && h != 8 {
                tick_data.update_merging(&mut kline);
                kline.v = tick_data.v;
                let dire = if tick_data.c > last_ask { 1. } else { -1. };
                price_ori.update(&kline);
                price_ori.immut_info.push(vec![vec![dire]]);
                last_finished = true;
            }
            last_ask = tick_data.ask1;
            last_bid = tick_data.bid1;
            last_finished
        })
    }
}

#[ta_derive]
pub struct OrderFlowPrice(pub InterBox);

#[typetag::serde]
impl Tri for OrderFlowPrice {

    fn tri<'a>(&self, _di: &'a Di, price: &'a PriceArc) -> UpdateFuncKlineIndex<'a> {
        let mut f = self.0.check_kline_func();
        let mut kline = KlineData::news();
        let mut last_info: bm<i32, (f32, f32)> = Default::default();
        let immut_info = &price.immut_info;
        Box::new(move |kline_data, price_ori, i| {
            let kline_state = f(kline_data);
            let res = kline.update_with_state(kline_state, kline_data);
            let c_key = (kline_data.c * 100.) as i32;
            last_info.entry(c_key).or_default();
            let dire_info = immut_info[i][0][0];
            if dire_info < 0. {
                last_info.get_mut(&c_key).unwrap().0 += kline_data.v;
            } else {
                last_info.get_mut(&c_key).unwrap().1 += kline_data.v;
            }
            if res {
                price_ori.update(&kline);
                let interval_info = last_info
                    .iter()
                    .fold(vec![Vec::with_capacity(last_info.len()); 3], |mut accu, (k, v)| {
                        accu[0].push((k / 100) as f32);
                        accu[1].push(v.0);
                        accu[2].push(v.1);
                        accu
                    });
                price_ori.immut_info.push(interval_info);
                last_info.clear();
            }
            res
        })   
    }
}

#[typetag::serde(name = "OrderFlow")]
impl Inter for OrderFlow {
    fn intervals(&self) -> Vec<Interval> {
        vec![]
    }

    fn check_kline_state(&self) -> Box<dyn FnMut(&dt) -> KlineState> {
        todo!()
    }

    fn string(&self) -> String {
        self.debug_string()
    }

    fn update_tick(&self, _ticker: Ticker) -> UpdateFuncTick {
        let mut kline = KlineData::news();
        let mut last_ask = f32::NAN;
        let mut last_bid = f32::NAN;
        let mut last_finished = true;
        Box::new(move |tick_data, price_ori| {
            if last_finished {
                tick_data.update_begin(&mut kline);
                last_finished = false;
            }
            let h = tick_data.t.hour();
            if (tick_data.c > last_ask || tick_data.c < last_bid) 
                && tick_data.v != 0. 
                && h != 20 
                && h != 8 {
                tick_data.update_merging(&mut kline);
                kline.v = tick_data.v;
                let dire = if tick_data.c > last_ask { 1. } else { -1. };
                price_ori.update(&kline);
                price_ori.immut_info.push(vec![vec![dire]]);
                last_finished = true;
            }
            last_ask = tick_data.ask1;
            last_bid = tick_data.bid1;
            last_finished
        })
    }
}

#[typetag::serde(name = "OrderFlowPrice")]
impl Inter for OrderFlowPrice {
    fn intervals(&self) -> Vec<Interval> {
        self.0.intervals()
    }

    fn string(&self) -> String {
        self.debug_string()
    }

    fn update_tick(&self, _ticker: Ticker) -> UpdateFuncTick {
        let mut kline = KlineData::news();
        let mut last_info: bm<i32, (f32, f32)> = Default::default();
        let mut last_ask = f32::NAN;
        let mut last_bid = f32::NAN;
        let mut f = self.0.check_tick_func();
        Box::new(move |tick_data, price_ori| {
            let kline_state = f(tick_data);
            let mut res = kline.update_with_state(kline_state.clone(), tick_data);
            match kline_state {
                KlineState::Ignor => {}
                _ => {
                    let h = tick_data.t.hour();
                    if (tick_data.c > last_ask || tick_data.c < last_bid) 
                        && tick_data.v != 0. 
                        && h != 20 
                        && h != 8 {
                        let vv = last_info.entry((tick_data.c * 100.) as i32).or_default();
                            if tick_data.c > last_ask {
                            vv.1 += tick_data.v;
                        } else if tick_data.c < last_bid {
                            vv.0 += tick_data.v;
                        }
                    }
                }
            }
            if res && last_info.is_empty() {
                res = false;
            }
            if res {
                let interval_info = last_info
                    .iter()
                    .fold(vec![Vec::with_capacity(last_info.len()); 3], |mut accu, (k, v)| {
                        accu[0].push((k / 100) as f32);
                        accu[1].push(v.0);
                        accu[2].push(v.1);
                        accu
                    });
                price_ori.update(&kline);
                price_ori.immut_info.push(interval_info);
                last_info.clear();
            }
            last_ask = tick_data.ask1;
            last_bid = tick_data.bid1;
            res
        })
    }
}

#[ta_derive]
pub struct OrderFlowPriceMean;

#[typetag::serde]
impl Tri for OrderFlowPriceMean {
    fn tri<'a>(&self,_di: &'a Di,price: &'a PriceArc) -> UpdateFuncKlineIndex<'a> {
        let mut kline = KlineData::news();
        let immut_info = &price.immut_info;
        Box::new(move |kline_data, price_ori, i| {
            kline.t = kline_data.t;
            kline.o = kline_data.o;
            kline.c = kline_data.c;
            let last_info = &immut_info[i];
            let inten_sell_vol = last_info[1].agg(RollFunc::Sum);
            let inten_buy_vol = last_info[2].agg(RollFunc::Sum);
            kline.l = izip!(last_info[0].iter(), last_info[2].iter())
                .fold(0f32, |mut accu, (p, s)| {
                    accu += p * s;
                    accu
                }) / inten_buy_vol;
            kline.h = izip!(last_info[0].iter(), last_info[1].iter())
                .fold(0f32, |mut accu, (p, s)| {
                    accu += p * s;
                    accu
                }) / inten_sell_vol;
            kline.v = inten_buy_vol - inten_sell_vol;
            price_ori.update(&kline);
            true
        })
    }
}


#[ta_derive]
pub struct OrderFlowAccu(pub InterBox);

#[typetag::serde]
impl Tri for OrderFlowAccu {
    fn tri<'a>(&self,_di: &'a Di,price: &'a PriceArc) -> UpdateFuncKlineIndex<'a> {
        let mut f = self.0.check_kline_func();
        let mut last_info: bm<i32, f32> = Default::default();
        let immut_info = &price.immut_info;
        Box::new(move |kline_data, price_ori, i| {
            let res = f(kline_data);
            price_ori.update(kline_data);
            let info = &immut_info[i];
            izip!(info[0].iter(), info[1].iter(), info[2].iter())
                .for_each(|(p, s, b)| {
                    let p_key = (p * 100.) as i32;
                    last_info.entry(p_key).or_default();
                    *last_info.get_mut(&p_key).unwrap() += b - s;
                });
            price_ori.immut_info.push({
                let (k, v) =  last_info
                    .iter()
                    .fold((vec![], vec![]), |mut accu, (p, v)| {
                        accu.0.push((p / 100) as f32);
                        accu.1.push(*v);
                        accu
                    });
                vec![k, v]
            });
            if let KlineState::Finished = res {
                last_info.clear();
            }
            true
        })
    }
}

#[ta_derive]
pub struct VolumePrice(pub InterBox);

#[typetag::serde]
impl PriceOriFromTickData for VolumePrice {
    fn check_tick_func(&self) -> Box<dyn FnMut(&TickData) -> KlineState> {
        todo!()
    }
    fn update_tick_func(&self, _ticker: Ticker) -> UpdateFuncTick {
        let mut f = self.0.check_tick_func();
        let mut kline = KlineData::news();
        let mut last_info: bm<i32, f32> = Default::default();
        Box::new(move |tick_data, price_ori| {
            let kline_state = f(tick_data);
            let res = kline.update_with_state(kline_state, tick_data);
            let c_key = (tick_data.c * 100.) as i32;
            let k = last_info.entry(c_key).or_default();
            *k += tick_data.v;
            if res {
                price_ori.update(&kline);
                let interval_info = last_info
                    .iter()
                    .fold(vec![Vec::with_capacity(last_info.len()); 2], |mut accu, (k, v)| {
                        accu[0].push((k / 100) as f32);
                        accu[1].push(*v);
                        accu
                    });
                price_ori.immut_info.push(interval_info);
                last_info.clear();
            }
            res
        })
    }
}

pub fn get_dd(data: Vec<vv32>) -> vv32 {
    let mut last_info: bm<i32, (f32, f32)> = Default::default();
    data
        .iter()
        .for_each(|x| {
            x[0].iter()
                .for_each(|y| {
                    let k = (y * 100.) as i32;
                    last_info.entry(k).or_default();
            });
        });
    let k_vec = last_info.keys().cloned().collect_vec();
    let k_vec_f32 = k_vec.iter().map(|x| (x / 100) as f32).collect_vec();
    let mut res = Vec::with_capacity(data.len() + 1);
    res.push(k_vec_f32.clone());
    for data_ in data.iter() {
        let ri = Reindex::new(&data_[0].map(|x| (x * 100.) as i32), &k_vec);
        let sell_side = ri.reindex(&data_[1]).fillna(0.);
        let buy_side = ri.reindex(&data_[2]).fillna(0.);
        let net_side = buy_side.iter().zip(sell_side.iter()).map(|(x, y)| x - y).collect_vec();
        res.push(net_side);
    }
    res
}

pub fn get_dd2(data: Vec<vv32>) -> vv32 {
    let mut last_info: bm<i32, (f32, f32)> = Default::default();
    data
        .iter()
        .for_each(|x| {
            x[0].iter()
                .for_each(|y| {
                    let k = (y * 100.) as i32;
                    last_info.entry(k).or_default();
            });
        });
    let k_vec = last_info.keys().cloned().collect_vec();
    let k_vec_f32 = k_vec.iter().map(|x| (x / 100) as f32).collect_vec();
    let mut res = Vec::with_capacity(data.len() + 1);
    res.push(k_vec_f32.clone());
    for data_ in data.iter() {
        let ri = Reindex::new(&data_[0].map(|x| (x * 100.) as i32), &k_vec);
        let net_side = ri.reindex(&data_[1]).fillna(0.);
        res.push(net_side);
    }
    res
}
*/