use serde::{ Serialize, Deserialize };
use qust::prelude::*;
use crate::econd::*;
use std::collections::BTreeMap as bm;
use chrono::Timelike;
use qust_derive::*;

#[derive(Default)]
pub struct UpdateOrderFlow {
    pub inter_update: KlineStateInter,
    pub only_if: bool,
    pub kline_promt: bool,
}

impl UpdateDataState<TickData> for UpdateOrderFlow {
    fn update(&mut self, data: &TickData) {
        use KlineState::*;
        match (&self.inter_update.kline_state.last, self.only_if, self.kline_promt) {
            (Begin | Merging, false, false) => {
                self.inter_update.kline_state.data.ki.pass_this += 1;
            }
            (Begin | Merging, _, true) => {
                self.inter_update.kline_state.current = Finished;
                self.inter_update.kline_state.update(data);
                self.inter_update.record.1 = 0;
            }
            (_, _, true) => {
                self.inter_update.kline_state.current = Ignor;
                self.inter_update.kline_state.update(data);
            }
            _ => {
                self.inter_update.update(data);
            }
        }
    }
}

#[ta_derive]
pub struct OrderFlowPrice2(pub InterBox);

#[typetag::serde(name = "OrderFlowPrice2")]
impl Tri for OrderFlowPrice2 {
    fn update_tick_func(&self, _ticker: Ticker) -> UpdateFuncTick {
        let mut kline = UpdateOrderFlow::default();
        kline.inter_update.intervals = self.0.intervals();
        let mut last_info: bm<i32, (f32, f32)> = Default::default();
        let mut last_ask = f32::NAN;
        let mut last_bid = f32::NAN;
        // let f_jump_time = inter::exit_by_jump_time.get_func(ticker);
        let f_last_time = inter::exit_by_last_time(0, 100).get_func(&self.0);
        Box::new(move |tick_data, price_ori| {
            let h = tick_data.t.hour();
            let of_triggered = (tick_data.c > last_ask  || tick_data.c < last_bid ) && h != 20 && h != 8 && tick_data.v != 0.;
            if of_triggered {
                let vv = last_info.entry((tick_data.c * 100.) as i32).or_default();
                if tick_data.c > last_ask {
                    vv.1 += tick_data.v;
                } else if tick_data.c < last_bid {
                    vv.0 += tick_data.v;
                }
            }
            // let finish_triggered = f_jump_time(&tick_data.t.time()) || f_last_time(&tick_data.t);
            let finish_triggered = f_last_time(&tick_data.t);
            kline.only_if     = of_triggered;
            kline.kline_promt = finish_triggered;
            kline.update(tick_data);
            match kline.inter_update.kline_state.last {
                KlineState::Ignor => last_info.clear(),
                KlineState::Finished => {
                    let interval_info = last_info
                        .iter()
                        .fold((0..3).map(|_| Vec::with_capacity(last_info.len()))
                        .collect::<Vec<_>>(), |mut accu, (k, v)| {
                            accu[0].push(*k as f32 / 100.);
                            accu[1].push(v.0);
                            accu[2].push(v.1);
                            accu
                        });
                    price_ori.update(&kline.inter_update.kline_state.data);
                    price_ori.immut_info.push(interval_info);
                    last_info.clear();
                }
                _ => {}
            }
            last_ask = tick_data.ask1;
            last_bid = tick_data.bid1;
            kline.inter_update.kline_state.last.clone()
        })
    }
}

#[ta_derive2]
pub struct ta;

#[typetag::serde(name = "of_ta")]
impl Ta for ta {
    fn calc_di(&self, _di: &Di) -> avv32 {
        // let price = di.calc(di.last_dcon());
        todo!();
        // vec![
        //     price.immut_info[0].clone(),
        //     di.v(),
        // ]
    }

    fn calc_da(&self, da: Vec<&[f32]>, _di: &Di) -> vv32 {
        let mut res = vec![vec![0f32; da[0].len()]; 4];
        for i in 0..da[0].len() {
            if da[0][i] < 0f32 {
                res[0][i] = 1f32;
                res[1][i] = da[1][i];
            } else {
                res[2][i] = 1f32;
                res[3][i] = da[1][i];
            }
        }
        let mut res = res
            .iter()
            .map(|x| x.cumsum())
            .collect_vec();
        let gap_price = izip!(res[2].iter(), res[0].iter())
            .map(|(x, y)| x - y)
            .collect_vec();
        let gap_vol = izip!(res[3].iter(), res[1].iter())
            .map(|(x, y)| x - y)
            .collect_vec();
        res.push(gap_price);
        res.push(gap_vol);
        res
    }
}

#[ta_derive2]
pub struct cond(pub Dire, pub f32, pub f32);

#[typetag::serde(name = "of_cond")]
impl Cond for cond {
    fn cond<'a>(&self, di: &'a Di) -> LoopSig<'a> {
        let data = di.calc(ori + oos + ta);
        let gap_price = data[4].clone();
        let gap_vol =  data[5].clone();
        let thre_price = self.1;
        let thre_vol = self.2;
        match self.0 {
            Lo => Box::new(move |e, _o| {
                e > 1 &&
                    gap_price[e] >= thre_price &&
                    gap_vol[e] >= thre_vol &&
                    (
                        gap_price[e - 1] < thre_price ||
                        gap_vol[e - 1] < thre_vol
                    )
            }),
            Sh => Box::new(move |e, _o| {
                e > 1 &&
                    gap_price[e] <= -thre_price &&
                    gap_vol[e] <=  -thre_vol &&
                    (
                        gap_price[e - 1] > -thre_price ||
                        gap_vol[e - 1] > -thre_vol
                    )
            })
        }
    }
}

lazy_static! {
    pub static ref ptm: Ptm = {
        let tsig_l = Tsig::new(Lo, &cond(Lo, 40., 4000.), &msig!(or, stop_cond(Sh, ThreType::Percent(0.5)), exit_by_k_num(2000)));
        let tsig_s = Tsig::new(Sh, &cond(Sh, 40., 4000.), &msig!(or, stop_cond(Lo, ThreType::Percent(0.5)), exit_by_k_num(2000)));
        let stp_l = Stp::Stp(tsig_l);
        let stp_s = Stp::Stp(tsig_s);
        Ptm::Ptm2(Box::new(M1(1.)), stp_l, stp_s)
    };
}
/*
let di_part = dil
    .get_idx(&eber)
    .last()
    .get_part(Between(20220420.to_da().to_dt()..20220425.to_da().to_dt()));
di_part.pcon.price.clone().aa();
di_part.pcon.price.immut_info.clone().to_value().aa_column();
di_part.calc(ori + oos + of_ta).to_value().aa_column();
let data: avv32 = di_part.calc(ori + oos + of_ta);
let gap_price = izip!(data[2].iter(), data[0].iter()).map(|(x, y)| x - y).collect_vec();
let gap_vol = izip!(data[3].iter(), data[1].iter()).map(|(x, y)| x - y).collect_vec();
println!("gap_price std: {} --- gap_vol std: {}", gap_price.std(), gap_vol.std());
// [
//     di_part.c().to_vec(),
//     gap_price,
//     // gap_vol,
// ].aplot(2)
di_part.c().plot()
gap_price.plot()
gap_vol.plot()
*/

#[ta_derive2]
pub struct cond2(pub Dire, pub f32, pub f32);

#[typetag::serde(name = "of_cond2")]
impl Cond for cond2 {
    fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
        let (h, l, v) = (di.h(), di.l(), di.v());
        let thre_price_gap = self.1;
        let thre_vol_gap = self.2;
        match self.0 {
            Dire::Sh => Box::new(move |e, _o| {
                h[e] - l[e] > thre_price_gap && v[e] < -thre_vol_gap
            }),
            Dire::Lo => Box::new(move |e, _o| {
                h[e] - l[e] > thre_price_gap && v[e] > thre_vol_gap
            })
        }
    }
}

#[ta_derive2]
pub struct ta3(pub usize, pub usize);

#[typetag::serde(name = "of_ta3")]
impl Ta for ta3 {
    fn calc_di(&self,di: &Di) -> avv32 {
        vec![di.v()]
    }
    fn calc_da(&self,da:Vec< &[f32]> ,_di: &Di) -> vv32 {
        vec![
            da[0].roll(RollFunc::Mean, RollOps::N(self.0)),
            da[0].roll(RollFunc::Mean, RollOps::N(self.1)),
        ]
    }
}

pub mod of4 {
    use super::*;
    
    #[ta_derive2]
    pub struct cond {
        pub dire: Dire,
        pub window: usize,
        pub bid_each_thre: f32,
        pub ask_all_thre: f32,
    }
    
    #[typetag::serde(name = "of_of4_cond")]
    impl Cond for cond {
        fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
            let w = self.window;
            let pms = ori + oos + of5::flat_ta(ori + ono + of5::ta, w);
            let data = di.calc(pms);
            let k: Box<dyn Ta> = Box::new(of5::ta); 
            let data2 = di.calc(ori + oos + RollTa(k, RollFunc::Sum, RollOps::N(w)));
            let v = of5::ta.estimate_vol(di);
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
                vec![0.1f32, 0.2, 0.8, 1.5, 3.],
                vec![0.2f32, 0.4, 1.5, 3.0],
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

pub mod of5 {
    use super::*;

    #[ta_derive2]
    pub struct ta;

    impl ta {
        pub fn estimate_vol(&self, di: &Di) -> f32 {
            di
                .immut_info()
                .nlast(4000.min(di.len()))
                .iter()
                .map(|x| {
                    x
                        .iter()
                        .skip(1)
                        .fold(0f32, |mut accu, x| {
                            x
                                .iter()
                                .for_each(|x| {
                                    accu = accu.max(*x);
                                });
                            accu
                        })
                })
                .collect_vec()
                .quantile(0.8)
        }
    }
    
    #[typetag::serde(name = "of_of5_ta")]
    impl Ta for ta {
        fn calc_di(&self,di: &Di) -> avv32 {
            let data = di.pcon.price.immut_info
                .iter()
                .fold((Vec::with_capacity(di.len()), Vec::with_capacity(di.len())), |mut accu, x| {
                    let bid = x[1].iter().sum();
                    let ask = x[2].iter().sum();
                    accu.0.push(bid);
                    accu.1.push(ask);
                    accu
                });
            vec![data.0.into(), data.1.into()]
        }
    
        fn calc_da(&self,da:Vec<&[f32]> ,_di: &Di) -> vv32 {
            vec![da[0].to_vec(), da[1].to_vec()]
        }
    }

    #[ta_derive]
    pub struct flat_ta<T>(pub T, pub usize);

    impl<T> flat_ta<T> {
        fn self_calc_ta(&self, data: Vec<&[f32]>) -> vv32 {
            data
                .into_iter()
                .map(|x| {
                    (0..self.1)
                        .rev()
                        .map(|i| x.lag(i as f32))
                        .collect_vec()
                })
                .concat()
        }
    }

    #[typetag::serde(name = "flat_ta_pms")]
    impl Ta for flat_ta<Pms> {
        fn calc_di(&self,di: &Di) -> avv32 {
            di.calc(&self.0)
        }
        fn calc_da(&self,da:Vec< &[f32]> ,_di: &Di) -> vv32 {
            self.self_calc_ta(da)
        }
    }

    #[typetag::serde(name = "flat_ta_ta")]
    impl Ta for flat_ta<TaBox> {
        fn calc_di(&self,di: &Di) -> avv32 {
            di.calc::<&TaBox, TaBox, avv32>(&self.0)
        }
        fn calc_da(&self,da:Vec< &[f32]> ,_di: &Di) -> vv32 {
            self.self_calc_ta(da)
        }
    }

    #[typetag::serde(name = "flat_ta_klinetype")]
    impl Ta for flat_ta<KlineType> {
        fn calc_di(&self,di: &Di) -> avv32 {
            vec![di.get_kline(&self.0)]
        }
        fn calc_da(&self,da:Vec< &[f32]> ,_di: &Di) -> vv32 {
            self.self_calc_ta(da)
        }
    }
}

pub mod of6 {
    use super::*;

    #[ta_derive2]
    pub struct ta;
    
    #[typetag::serde(name = "of_of6_ta")]
    impl Ta for ta {
        fn calc_di(&self,di: &Di) -> avv32 {
            di.calc(of5::ta)
        }
    
        fn calc_da(&self,da:Vec<&[f32]> ,_di: &Di) -> vv32 {
            let res = izip!(da[0].iter(), da[1].iter())
                .map(|(x, y)| x - y)
                .collect_vec();
            vec![res]
        }
    }
    
    #[ta_derive2]
    pub struct cond {
        pub dire: Dire,
        pub window: usize,
        pub bid_ask_spread: f32,
    }
    
    #[typetag::serde(name = "of_of6_cond")]
    impl Cond for cond {
        fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
            let w = self.window;
            let pms = ori + oos + of5::flat_ta(ori + ono + ta, w);
            let data = di.calc(pms);
            let spread = self.bid_ask_spread * of5::ta.estimate_vol(di);
            match self.dire {
                Lo => {
                    let data2 = di.calc(ori + oos + of5::flat_ta(KlineType::Low, w));
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
                    let data2 = di.calc(ori + oos + of5::flat_ta(KlineType::High, w));
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

    #[typetag::serde(name = "of_of6_cond_vol_gap")]
    impl Cond for cond_vol_gap {
        fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
            let w = self.2;
            let spread = self.1 * of5::ta.estimate_vol(di);
            let pms = ori + oos + of5::flat_ta(ori + ono + ta, w);
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

    #[typetag::serde(name = "of_of4_cond_price")]
    impl Cond for cond_price {
        fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
            let k = self.1.clone();
            let w = self.2;
            match self.0 {
                // the kline is going down repeatedly
                Lo => {
                    let data = di.calc(ori + oos + of5::flat_ta(k, w));
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
                    let data = di.calc(ori + oos + of5::flat_ta(k, w));
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
                vec![0.1f32, 0.3, 1.5],
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

pub mod of7 {
    use super::*;

    #[ta_derive2]
    pub struct cond2(pub Dire, pub usize);

    #[typetag::serde(name = "of_of7_cond2")]
    impl Cond for cond2 {
        fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
            match self.0 {
                Lo => {
                    let h = di.h();
                    let h_max = di.calc(ori + oos + ta_max_n(self.1))[0].clone();
                    Box::new(move |e, o| h[e] > h_max[o])
                }
                Sh => {
                    let l = di.l();
                    let l_min = di.calc(ori + oos + ta_min_n(self.1))[0].clone();
                    Box::new(move |e, o| l[e] < l_min[o])
                }
            }

        }
    }

    #[ta_derive2]
    pub struct cond3(pub Dire, pub f32);

    #[typetag::serde(name = "of_of7_cond3")]
    impl Cond for cond3 {
        fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
            match self.0 {
                Lo => {
                    let h = di.h();
                    let last_c = di.h().to_vec();
                    let thre = self.1 * di.pcon.ticker.info().tz;
                    Box::new(move |e, o| {
                        if e > o && h[e] > h[e - 1] { 
                            // last_c[o] = h[e];
                        }
                        h[e] < last_c[o] - thre
                    })
                }
                Sh => {
                    let l = di.l();
                    let last_c = di.l().to_vec();
                    let thre = self.1 * di.pcon.ticker.info().tz;
                    Box::new(move |e, o| {
                        if e > o && l[e] < l[e - 1] { 
                            // last_c[o] = l[e];
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
                vec![2f32, 4f32, 8f32, 20f32, 40.],
                vec![2f32, 4f32, 8f32, 20f32, 40.],
            ]
                .inner_product2()
                .unwrap()
                .map(|x| {
                    let cond_lo = exit_by_tick_size(Lo, x[0]);
                    let cond_sh = exit_by_tick_size(Sh, x[1]);
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

pub trait GroupbyTime {
    fn groupby_time(&self) -> Vec<InfoPnlRes<u32, dt>>;
}

impl GroupbyTime for PnlRes<dt> {
    fn groupby_time(&self) -> Vec<InfoPnlRes<u32, dt>> {
        self
            .0
            .map(|x| x.hour())
            .unique()
            .into_map(|x| {
                let index_vec = self.0.filter_position(|t| t.hour() == x);
                let pnl_res = self.get_part(index_vec);
                InfoPnlRes(x, pnl_res)
            })
    }
}

pub mod inter {
    use chrono::Duration;

    use super::*;

    #[ta_derive2]
    pub struct exit_by_last_time(pub i64, pub i64);

    impl exit_by_last_time {
        pub fn get_func(&self, inter_box: &InterBox) -> impl Fn(&dt) -> bool {
            let binding = inter_box.intervals();
            let t = binding.last().unwrap();
            let last_time = t.end_time();
            let start_time = last_time - Duration::seconds(self.0);
            let end_time   = last_time + Duration::seconds(self.1);
            if end_time.hour() == 0 {
                panic!("something is wrong");
            }
            let range_time = start_time .. end_time;
            move |t| range_time.contains(&t.time())
        }
    }

    #[typetag::serde]
    impl Cond for exit_by_last_time {
        fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
            let t = di.t();
            let f = self.get_func(&ofm14.clone());
            Box::new(move |e, _o| {
                f(&t[e])
            })
        }
    }

    #[ta_derive2]
    pub struct exit_by_jump_time;

    impl exit_by_jump_time {
        pub fn get_func(&self, ticker: Ticker) -> Box<dyn Fn(&tt) -> bool> {
            let trading_period: TradingPeriod = ticker.into();
            let morning_last_time = 112940.to_tt() .. 113100.to_tt();
            let light_last_time   = 145940.to_tt() .. 150100.to_tt();
            let night_last_time = match trading_period {
                TradingPeriod::LightNight => 225940.to_tt() .. 230100.to_tt(),
                _ => 5830.to_tt() .. 10100.to_tt(),
            };
            Box::new(move |t_now: &tt| {
                morning_last_time.contains(t_now)
                    || light_last_time.contains(t_now)
                    || night_last_time.contains(t_now)
            })

        }
    }

    #[typetag::serde]
    impl Cond for exit_by_jump_time {
        fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
            let f = self.get_func(di.pcon.ticker);
            let t = di.t();
            Box::new(move |e, _o| {
                let t_now = t[e].time();
                f(&t_now)
            })
        }
    }


    macro_rules! gen_inter2 {
        ($inter: ident, $vals: expr, $box: ident) => {
            #[derive(PathDebug, Clone, Serialize, Deserialize)]
            pub struct $inter;
    
            #[typetag::serde]
            impl Inter for $inter {
                fn intervals(&self) -> Vec<Interval> { $vals }
            }
    
            lazy_static! {
                pub static ref $box: InterBox = Box::new($inter);
            }
        };
    }

    gen_inter2!(
        m09, 
        even_slice_time_usize(90000, 101430, 60),
        ofm09
    );
    gen_inter2!(
        m10, 
        even_slice_time_usize(103000, 112830, 60),
        ofm10
    );
    gen_inter2!(
        m13, 
        even_slice_time_usize(133000, 135930, 60),
        ofm13
    );
    gen_inter2!(
        m14, 
        even_slice_time_usize(140000, 145830, 60),
        ofm14
    );
    gen_inter2!(
        m21, 
        even_slice_time_usize(210000, 215930, 60),
        ofm21
    );
    gen_inter2!(
        m22, 
        even_slice_time_usize(220000, 225830, 60),
        ofm22
    );
}

pub fn slice_time_train_test(start: da, end: da, percent: f32) -> (ForCompare<dt>, ForCompare<dt>) {
    use rand::prelude::*;
    use std::collections::HashSet;
    fn gen_one_date_range(date: da) -> ForCompare<dt> {
        let start = date.and_hms_opt(21, 0, 0).unwrap();
        let end = date.succ_opt().unwrap().and_hms_opt(15, 0, 0).unwrap();
        start.to(end)
    }
    let num_days = da::signed_duration_since(end, start).num_days() as usize;
    let mut rng = thread_rng();
    let test_date_index = rand::seq::index::sample(&mut rng, num_days, (num_days as f32 * percent) as usize)
        .into_iter()
        .collect::<HashSet<usize>>();
    let (date_vec_tr, date_vec_te) = start
        .iter_days()
        .take(num_days)
        .enumerate()
        .fold((vec![], vec![]), |mut accu, (i, date)| {
            let da_range = gen_one_date_range(date).pip(Box::new);
            if test_date_index.contains(&i) {
                accu.1.push(da_range);
            } else {
                accu.0.push(da_range);
            }
            accu
        });
    (ForCompare::List(date_vec_tr), ForCompare::List(date_vec_te))
}

pub fn from_cond_to_named_ptm(
    cond_open: Vec<Box<dyn Cond>>, 
    cond_exit: Vec<Box<dyn Cond>>,
    frame: &str,
    kind: StraKind,
) -> Vec<NamedPtm> {
    let lo_vec = cond_open
        .into_map(|x| {
            cond_exit
                .map(|y| {
                    Ptm::Ptm3(m1.clone(), Lo, x.clone(), y.clone())
                        .name_for_ptm(
                            StraName
                                ::default()
                                .set_dire(Lo)
                                .set_frame(frame)
                                .set_kind(kind.clone())
                            )
                })
        })
        .concat();
    let sh_vec = lo_vec.map(|x| x.clone().reverse());
    [lo_vec, sh_vec].concat()
}

#[ta_derive2]
pub struct OrderFlowPrice3(pub InterBox);

#[typetag::serde(name = "OrderFlowPrice3")]
impl Tri for OrderFlowPrice3 {
    fn update_tick_func(&self, ticker: Ticker) -> UpdateFuncTick {
        let mut kline = KlineStateInter::from_intervals(self.0.intervals());
        let mut last_info: bm<i32, (f32, f32)> = Default::default();
        let mut last_ask = f32::NAN;
        let mut last_bid = f32::NAN;
        let f_jump_time = inter::exit_by_jump_time.get_func(ticker);
        let f_last_time = inter::exit_by_last_time(0, 100).get_func(&self.0);
        Box::new(move |tick_data, price_ori| {
            let h = tick_data.t.hour();
            let of_triggered = (tick_data.c > last_ask  || tick_data.c < last_bid ) && h != 20 && h != 8 && tick_data.v != 0.;
            if of_triggered {
                let vv = last_info.entry((tick_data.c * 100.) as i32).or_default();
                if tick_data.c > last_ask {
                    vv.1 += tick_data.v;
                } else if tick_data.c < last_bid {
                    vv.0 += tick_data.v;
                }
            }
            let finish_triggered = of_triggered || f_jump_time(&tick_data.t.time()) || f_last_time(&tick_data.t);
            let time_kline_update = matches!(kline.kline_state.last, KlineState::Finished | KlineState::Ignor);
            if finish_triggered || time_kline_update {
                kline.update(tick_data);
                match kline.kline_state.last {
                    KlineState::Ignor => last_info.clear(),
                    KlineState::Finished => {
                        let interval_info = last_info
                            .iter()
                            .fold(init_a_matrix(last_info.len(), 3), |mut accu, (k, v)| {
                                accu[0].push(*k as f32 / 100.);
                                accu[1].push(v.0);
                                accu[2].push(v.1);
                                accu
                            });
                        price_ori.update(&kline.kline_state.data);
                        price_ori.immut_info.push(interval_info);
                        last_info.clear();
                    }
                    _ => {}
                }
            }
            last_ask = tick_data.ask1;
            last_bid = tick_data.bid1;
            kline.kline_state.last.clone()
        })
    }
}