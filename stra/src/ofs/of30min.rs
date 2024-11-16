use serde::{ Serialize, Deserialize };
use qust::prelude::*;
use super::econd::*;
use super::of;
use std::collections::BTreeMap as bm;
use chrono::Timelike;

#[ta_derive]
pub struct OrderFlowPrice3(pub InterBox);

#[typetag::serde(name = "OrderFlowPrice3")]
impl Inter for OrderFlowPrice3 {
    fn intervals(&self) -> Vec<Interval> {
        self.0.intervals()
    }

    fn string(&self) -> String {
        self.debug_string()
    }

    fn update_tick(&self, ticker: Ticker) -> UpdateFuncTick {
        let mut kline = KlineData::news();
        let mut last_info: bm<i32, (f32, f32)> = Default::default();
        let mut last_ask = f32::NAN;
        let mut last_bid = f32::NAN;
        let mut f = self.0.check_tick_func();
        let f_jump_time = of::inter::exit_by_jump_time.get_func(ticker);
        let f_last_time = of::inter::exit_by_last_time(0, 100).get_func(&self.0);
        let mut time_kline_state = KlineState::Ignor;
        let mut finished = false;
        Box::new(move |tick_data, price_ori| {
            let h = tick_data.t.hour();
            let triggered_of = (tick_data.c > last_ask  || tick_data.c < last_bid ) && h != 20 && h != 8 && tick_data.v != 0.;
            if triggered_of {
                let vv = last_info.entry((tick_data.c * 100.) as i32).or_default();
                if tick_data.c > last_ask {
                    vv.1 += tick_data.v;
                } else if tick_data.c < last_bid {
                    vv.0 += tick_data.v;
                }
            }
            let triggered_promt = f_jump_time(&tick_data.t.time()) || f_last_time(&tick_data.t);
            let triggered_state = matches!(time_kline_state, KlineState::Finished | KlineState::Ignor);
            let triggered_finish = triggered_of || (triggered_state ^ triggered_promt);
            
            if triggered_finish {
                time_kline_state = f(tick_data);
                finished = kline.update_with_state(time_kline_state.clone(), tick_data);
                match time_kline_state {
                    KlineState::Ignor => last_info.clear(),
                    KlineState::Finished => {
                        let interval_info = last_info
                            .iter()
                            .fold(vec![Vec::with_capacity(last_info.len()); 3], |mut accu, (k, v)| {
                                accu[0].push(*k as f32 / 100.);
                                accu[1].push(v.0);
                                accu[2].push(v.1);
                                accu
                            });
                        price_ori.update(&kline);
                        price_ori.immut_info.push(interval_info);
                        last_info.clear();
                    }
                    _ => {}
                }
            }
            last_ask = tick_data.ask1;
            last_bid = tick_data.bid1;
            finished
        })
    }
}


pub mod of4_30min {
    use super::*;
    
    #[ta_derive2]
    pub struct cond {
        pub dire: Dire,
        pub window: usize,
        pub bid_each_thre: f32,
        pub ask_all_thre: f32,
    }
    
    #[typetag::serde(name = "of30min_of4_cond")]
    impl Cond for cond {
        fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
            let w = self.window;
            let pms = ori + ono + of::of5::flat_ta(ori + ono + of::of5::ta, w);
            let data = di.calc(pms);
            let k: Box<dyn Ta> = Box::new(of::of5::ta); 
            let data2 = di.calc(ori + ono + RollTa(k, RollFunc::Sum, RollOps::N(w)));
            let v = of::of5::ta.estimate_vol(di);
            let bid_thre = self.bid_each_thre * v;
            let ask_thre = self.ask_all_thre * v;
            match self.dire {
                //buy side each time bigger than thre, and sum sell side smaller than thre
                Lo => {
                    Box::new(move |e, _o| {
                        for data_ in data.iter().take(w) {
                            if data_[e] < bid_thre { return false }
                        }
                        if data2[1][e] > ask_thre { return false }
                        true
                    })
                }
                Sh => {
                    Box::new(move |e, _o| {
                        for data_ in data.iter().skip(w) {
                            if data_[e] < bid_thre { return false }
                        }
                        if data2[0][e] > ask_thre { return false }
                        true
                    })
                }
            }
        }
    }

    lazy_static! {
        pub static ref cond_vec: Vec<(Box<dyn Cond>, Box<dyn Cond>)> = {
            vec![
                vec![2f32, 3., 4., 6.],
                vec![0.2f32, 0.4f32, 0.8, 1.5],
                vec![0.4f32, 0.8, 1.5],
            ]
                .inner_product2()
                .unwrap()
                .map(|x| {
                    let cond_lo = cond { 
                        dire: Lo, 
                        window: x[0] as usize, 
                        bid_each_thre: x[1], 
                        ask_all_thre: x[2]
                    };
                    let cond_sh = cond { 
                        dire: Sh, 
                        window: x[0] as usize, 
                        bid_each_thre: x[1], 
                        ask_all_thre: x[2]
                    };
                    (cond_lo.to_box(), cond_sh.to_box())
                })
        };

        pub static ref tickers1: Vec<Ticker> = vec![
            eger, MAer, ler, pper, ruer, eber, SAer, ier, 
            RMer, OIer, sper, sser, aler, per, TAer, jder,
            rber,
        ];
    }
}

pub mod of6_30min {
    use super::*;

    #[ta_derive2]
    pub struct cond {
        pub dire: Dire,
        pub window: usize,
        pub bid_ask_spread: f32,
    }
    
    #[typetag::serde(name = "of30min_of6_cond")]
    impl Cond for cond {
        fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
            let w = self.window;
            let pms = ori + ono + of::of5::flat_ta(ori + ono + of::of6::ta, w);
            let data = di.calc(pms);
            let spread = self.bid_ask_spread * of::of5::ta.estimate_vol(di);
            match self.dire {
                Lo => {
                    let data2 = di.calc(ori + ono + of::of5::flat_ta(KlineType::Low, w));
                    Box::new(move |e, _o| {
                        for data_ in data.iter().take(w) {
                            if data_[e] < spread { return false }
                        }
                        for data2_ in data2.windows(2) {
                            if data2_.first().unwrap()[e] < data2_.last().unwrap()[e] { 
                                return false
                            }
                        }
                        true
                    })
                }
                Sh => {
                    let data2 = di.calc(ori + ono + of::of5::flat_ta(KlineType::High, w));
                    Box::new(move |e, _o| {
                        for data_ in data.iter().skip(w) {
                            if -data_[e] < spread { return false }
                        }
                        for data2_ in data2.windows(2) {
                            if data2_.first().unwrap()[e] > data2_.last().unwrap()[e] { 
                                return false
                            }
                        }
                        true
                    })
                }
            }
        }
    }

    #[ta_derive2]
    pub struct cond_vol_gap(pub Dire, pub f32, pub usize);

    #[typetag::serde(name = "of30min_of6_cond_vol_gap")]
    impl Cond for cond_vol_gap {
        fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
            let w = self.2;
            let spread = self.1 * of::of5::ta.estimate_vol(di);
            let pms = ori + ono + of::of5::flat_ta(ori + ono + of::of5::ta, w);
            let data = di.calc(pms);
            match self.0 {
                // the buy - sell gap keep bigger than the thre
                Lo => {
                    Box::new(move |e, _o| {
                        for data_ in data.iter() {
                            if data_[e] < spread { return false }
                        }
                        true
                    })
                }
                // the sell - buy gap keep bigger than the thre
                Sh => {
                    Box::new(move |e, _o| {
                        for data_ in data.iter() {
                            if data_[e] > -spread { return false }
                        }
                        true
                    })
                }
            }

        }
    }

    #[ta_derive2]
    pub struct cond_price(pub Dire, pub KlineType, pub usize);

    #[typetag::serde(name = "of30min_of4_cond_price")]
    impl Cond for cond_price {
        fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
            let k = self.1.clone();
            let w = self.2;
            match self.0 {
                // the kline is going down repeatedly
                Lo => {
                    let data = di.calc(ori + ono + of::of5::flat_ta(k, w));
                    Box::new(move |e, _o| {
                        for data_ in data.windows(2) {
                            if data_.first().unwrap()[e] < data_.last().unwrap()[e] {
                                return false
                            }
                        }
                        true
                    })
                }
                //the kline is going up repeatedly
                Sh => {
                    let data = di.calc(ori + ono + of::of5::flat_ta(k, w));
                    Box::new(move |e, _o| {
                        for data_ in data.windows(2) {
                            if data_.first().unwrap()[e] > data_.last().unwrap()[e] {
                                return false
                            }
                        }
                        true
                    })
                }
            }   
        }
    }
    
    
    type Box4Cond = (Box<dyn Cond>, Box<dyn Cond>, Box<dyn Cond>, Box<dyn Cond>);

    lazy_static! {
        pub static ref cond_vec: Vec<(Box<dyn Cond>, Box<dyn Cond>)> = {
            vec![
                vec![2f32, 4., 6.],
                vec![0.1f32, 0.3, 1.5, 3.],
            ]
                .inner_product2()
                .unwrap()
                .map(|x| {
                    let cond_lo = cond { 
                        dire: Lo, 
                        window: x[0] as usize, 
                        bid_ask_spread: x[1], 
                    };
                    let cond_sh = cond { 
                        dire: Sh, 
                        window: x[0] as usize, 
                        bid_ask_spread: x[1], 
                    };
                    (cond_lo.to_box(), cond_sh.to_box())
                })
        };

        pub static ref cond_vec_price: Vec<Box4Cond> = {
            vec![2, 3, 5]
                .into_map(|x| {
                    let cond_lo_low  = cond_price(Lo, KlineType::Low, x);   // low keep going down
                    let cond_lo_high = cond_price(Lo, KlineType::High, x);  //high keep going down
                    let cond_sh_high = cond_price(Sh, KlineType::High, x);  //high keep going up
                    let cond_sh_low  = cond_price(Sh, KlineType::Low, x);   //low keep going up
                    (cond_lo_low.to_box(), cond_lo_high.to_box(), cond_sh_high.to_box(), cond_sh_low.to_box())
                })
        };

        pub static ref cond_vec_vol_gap: Vec<(Box<dyn Cond>, Box<dyn Cond>)> = {
            vec![
                vec![0.2f32, 0.6, 1.5],
                vec![2f32, 4., 6.],
            ]
                .inner_product2()
                .unwrap()
                .into_map(|x| {
                    let cond_lo = cond_vol_gap(Lo, x[0], x[1] as usize); //buy - side keep bigger
                    let cond_sh = cond_vol_gap(Sh, x[0], x[1] as usize); //sid - buy keep bigger
                    (cond_lo.to_box(), cond_sh.to_box())
                })
        };
    }
}

pub mod of7_30min {
    use super::*;

    #[ta_derive2]
    pub struct cond2(pub Dire, pub usize);

    #[typetag::serde(name = "of30min_of7_cond2")]
    impl Cond for cond2 {
        fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
            match self.0 {
                Lo => {
                    let h = di.h();
                    let h_max = di.calc(ori + ono + ta_max_n(self.1))[0].clone();
                    Box::new(move |e, o| h[e] > h_max[o])
                }
                Sh => {
                    let l = di.l();
                    let l_min = di.calc(ori + ono + ta_min_n(self.1))[0].clone();
                    Box::new(move |e, o| l[e] < l_min[o])
                }
            }

        }
    }

    #[ta_derive2]
    pub struct cond3(pub Dire, pub f32);

    #[typetag::serde(name = "of30min_of7_cond3")]
    impl Cond for cond3 {
        fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
            match self.0 {
                Lo => {
                    let h = di.h();
                    let mut last_c = di.h().to_vec();
                    let thre = self.1 * di.pcon.ticker.info().0;
                    Box::new(move |e, o| {
                        if e > o && h[e] > h[e - 1] { 
                            last_c[o] = h[e];
                        }
                        h[e] < last_c[o] - thre
                    })
                }
                Sh => {
                    let l = di.l();
                    let mut last_c = di.l().to_vec();
                    let thre = self.1 * di.pcon.ticker.info().0;
                    Box::new(move |e, o| {
                        if e > o && l[e] < l[e - 1] { 
                            last_c[o] = l[e];
                        }
                        l[e] > last_c[o] + thre
                    })
                }
            }
        }
    }

    lazy_static! {
        pub static ref exit_by_tick_size_vec: Vec<(Box<dyn Cond>, Box<dyn Cond>)> = {
            vec![
                vec![10f32, 20f32, 40f32, 100f32],
                vec![10f32, 20f32, 40f32, 100f32],
            ]
                .inner_product2()
                .unwrap()
                .map(|x| {
                    let cond_lo = exit_by_tick_size(Lo, x[0]);
                    let cond_sh = exit_by_tick_size(Sh, x[1]);
                    (cond_lo.to_box(), cond_sh.to_box())
                })
        };

        pub static ref exit_by_sby: Vec<(Box<dyn Cond>, Box<dyn Cond>)> = {
            vec![
                vec![0.1f32, 0.15, 0.25],
                vec![0.1f32, 0.15, 0.25],
            ]
                .inner_product2()
                .unwrap()
                .map(|x| {
                    let cond_lo = price_sby(Lo, x[0]);
                    let cond_sh = price_sby(Sh, x[1]);
                    (cond_lo.to_box(), cond_sh.to_box())
                })
        };

        pub static ref exit_by_normal: Vec<(Box<dyn Cond>, Box<dyn Cond>)> = {
            vec![
                vec![0.2f32, 0.4, 0.8, 1.2],
                vec![0.2f32, 0.4, 0.8, 1.2],
            ]
                .inner_product2()
                .unwrap()
                .map(|x| {
                    let cond_lo = stop_cond(Lo, ThreType::Percent(x[0]));
                    let cond_sh = stop_cond(Sh, ThreType::Percent(x[1]));
                    (cond_lo.to_box(), cond_sh.to_box())
                })
        };

        ///only for cond conditioning on the price is going down
        /// otherwise, this cond make the exiting immediately
        pub static ref cond2_for_lo_vec: Vec<Box<dyn Cond>> = {
            vec![4usize, 8, 20].into_map(|x| cond2(Lo, x).to_box())
        };

        pub static ref cond2_for_sh_vec: Vec<Box<dyn Cond>> = {
            vec![4usize, 8, 20].into_map(|x| cond2(Sh, x).to_box())
        };

        ///only for cond conditioning on the position is long
        /// otherwise, make no sense. say, I have a position short
        /// if now price below the highest price since open, then I exit.
        /// exit on what?
        pub static ref cond3_for_posi_lo: Vec<Box<dyn Cond>> = {
            vec![2f32, 4f32, 20f32].into_map(|x| cond3(Lo, x).to_box())
        };

        pub static ref cond3_for_posi_sh: Vec<Box<dyn Cond>> = {
            vec![2f32, 4f32, 20f32].into_map(|x| cond3(Sh, x).to_box())
        };
    }
}
