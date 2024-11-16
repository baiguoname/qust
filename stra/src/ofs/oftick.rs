use std::collections::BTreeMap as bm;
use qust::prelude::*;
use crate::prelude::rl1mall;
use qust_derive::*;

use super::of::{OrderFlowPrice2, self, UpdateOrderFlow };
use super::ofpro::OrderFlow;
use chrono::Timelike;

#[ta_derive]
pub struct Rticks;

#[typetag::serde]
impl Tri for Rticks {
    fn gen_price_ori(&self,price_tick: &PriceTick) -> PriceOri {
        let mut res = PriceOri::with_capacity(price_tick.size());
        res.immut_info = Vec::with_capacity(price_tick.size());
        res
    }

    fn update_tick_func(&self,_ticker:Ticker) -> UpdateFuncTick {
        let mut kline = KlineData::default();
        Box::new(move |tick_data, price_ori| {
            kline.update_begin(tick_data);
            kline.ki.pass_last = 1;
            kline.ki.pass_this = 0;
            price_ori.update(&kline);
            price_ori.immut_info.push(vec![vec![tick_data.ask1, tick_data.bid1, tick_data.ask1_v, tick_data.bid1_v]]);
            KlineState::Finished
        })
    }
}

impl UpdateDataState<KlineData> for UpdateOrderFlow {
    fn update(&mut self, data: &KlineData) {
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

#[typetag::serde(name = "PriOrderFlowPrice2")]
impl Pri for OrderFlowPrice2 {
    fn update_kline_func(&self, di: &Di,price: &PriceArc) -> UpdateFuncKline {
        let mut kline = UpdateOrderFlow::default();
        kline.inter_update.intervals = self.0.intervals();
        let mut last_info: bm<i32, (f32, f32)> = Default::default();
        let mut last_ask = f32::NAN;
        let mut last_bid = f32::NAN;
        let tick_data_vec = price.immut_info.clone();
        let f_jump_time = of::inter::exit_by_jump_time.get_func(di.pcon.ticker);
        let f_last_time = of::inter::exit_by_last_time(0, 100).get_func(&self.0);
        Box::new(move |kline_data, price_ori, i| {
            let h = kline_data.t.hour();
            let of_triggered = (kline_data.c > last_ask  || kline_data.c < last_bid ) && h != 20 && h != 8 && kline_data.v != 0.;
            if of_triggered {
                let vv = last_info.entry((kline_data.c * 100.) as i32).or_default();
                if kline_data.c > last_ask {
                    vv.1 += kline_data.v;
                } else if kline_data.c < last_bid {
                    vv.0 += kline_data.v;
                }
            }
            let finish_triggered = f_last_time(&kline_data.t) | f_jump_time(&kline_data.t.time());
            kline.only_if     = of_triggered;
            kline.kline_promt = finish_triggered;
            kline.update(kline_data);
            match kline.inter_update.kline_state.last {
                KlineState::Ignor => last_info.clear(),
                KlineState::Finished => {
                    let interval_info = last_info
                        .iter()
                        .fold((0..3).map(|_| Vec::with_capacity(last_info.len())).collect::<Vec<_>>(), |mut accu, (k, v)| {
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
            last_ask = tick_data_vec[i][0][0];
            last_bid = tick_data_vec[i][0][1];
            kline.inter_update.kline_state.last.clone()
        })
    }
}

#[derive(Default)]
struct UpdateOrderFlowOnce {
    kline_state: KlineWithState,
    to_pass: bool,
    to_update: bool,
}

impl UpdateDataState<KlineData> for UpdateOrderFlowOnce {
    fn update(&mut self, data: &KlineData) {
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

#[typetag::serde(name = "oftick_priorderflow")]
impl Pri for OrderFlow {
    fn update_kline_func(&self,_di: &Di,price: &PriceArc) -> UpdateFuncKline {
        let mut kline      = UpdateOrderFlowOnce::default();
        let mut last_ask   = f32::NAN;
        let mut last_bid   = f32::NAN;
        let tick_data_vec = price.immut_info.clone();
        Box::new(move |kline_data, price_ori, i| {
            let h               = kline_data.t.hour();
            let is_to_pass      = kline_data.v == 0. || h == 20 || h == 8;
            let of_triggered    = kline_data.c > last_ask || kline_data.c < last_bid;
            kline.to_pass   = is_to_pass;
            kline.to_update = of_triggered;
            kline.kline_state.data.ki.pass_last = 1;
            kline.update(kline_data);
            if let KlineState::Finished = kline.kline_state.last {
                let of_dire = if kline_data.c > last_ask { 1. } else { -1. };
                price_ori.immut_info.push(vec![vec![of_dire]]);
                price_ori.update(&kline.kline_state.data);
                kline.kline_state.data.ki.pass_last = 1;
            }
            last_ask = tick_data_vec[i][0][0];
            last_bid = tick_data_vec[i][0][1];
            kline.kline_state.last.clone()
        })
    }
}

#[ta_derive2]
struct ta(usize);

#[typetag::serde(name = "oftick_ta")]
impl Ta for ta {
    fn calc_di(&self,di: &Di) -> avv32 {
        let immut_info = di.immut_info();
        let data = immut_info.iter()
            .fold(init_a_matrix(immut_info.len(), 3), |mut accu, x| {
                let bid: f32 = x[1].iter().sum();
                let ask = x[2].iter().sum();
                accu[0].push(bid);
                accu[1].push(ask);
                accu[2].push(bid - ask);
                accu
            })
            .into_map(|x| x.to_arc());
        vec![
            di.h(),
            di.l(),
            data[0].clone(),
            data[1].clone(),
            data[2].clone(),
        ]
    }

    fn calc_da(&self,da:Vec< &[f32]> ,_di: &Di) -> vv32 {
        vec![
            da[0].to_vec(),//h
            da[1].to_vec(),//l
            da[2].to_vec(),//bid
            da[3].to_vec(),//ask
            da[2].roll(RollFunc::Sum, RollOps::N(self.0)),//bid sum
            da[3].roll(RollFunc::Sum, RollOps::N(self.0)),//ask sum
            da[4].to_vec(),//bid - ask
        ]
    }
}

#[ta_derive2]
pub struct cond {
    pub dire: Dire,
    pub inter: InterBox,
    pub n: usize,
    pub thre_sin: f32,
    pub thre_sum: f32,
}

#[typetag::serde(name = "oftick_cond")]
impl Cond for cond {
    fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
        let convert = self.inter.pip_clone(OrderFlowPrice2).pri_box().pip(Event);
        let pms = ori + convert.clone() + oos + ta(self.n);
        let pms_flat = ori + convert + oos + of::of5::flat_ta(pms, self.n) + ori;
        let data = di.calc(pms_flat);
        let (thre_sin, thre_sum) = (self.thre_sin, self.thre_sum);
        let w = self.n;
        match self.dire {
            Lo => {
                Box::new(move |e, _o| {
                    for da in data.windows(2).take(w - 1) {
                        if da.first().unwrap()[e] < da.last().unwrap()[e] { return false; }
                    }
                    for da in data.iter().take(3 * w).skip(2 * w) {
                        if da[e] < thre_sin { return false; }
                    }
                    data[6 * w - 1][e] < thre_sum 
                })
            }
            Sh => {
                Box::new(move |e, _o| {
                    for da in data.windows(2).skip(w).take(w - 1) {
                        if da.first().unwrap()[e] > da.last().unwrap()[e] { return false; }
                    }
                    for da in data.iter().take(4 * w).skip(3 * w) {
                        if da[e] < thre_sin { return false; }
                    }
                    data[5 * w - 1][e] < thre_sum 
                })
            }
        }
    }
}

#[ta_derive2]
pub struct ta2;

#[typetag::serde(name = "oftick_ta")]
impl Ta for ta2 {
    fn calc_di(&self,di: &Di) -> avv32 {
        let data = di.immut_info()
            .iter()
            .map(|x| *x.first().unwrap().first().unwrap())
            .collect_vec();
        vec![data.to_arc()]
    }

    fn calc_da(&self,da:Vec< &[f32]> ,_di: &Di) -> vv32 {
        vec![da[0].to_vec()]
    }
}

#[ta_derive2]
pub struct cond2(pub Dire);

#[typetag::serde(name = "oftick_cond2")]
impl Cond for cond2 {
    fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
        let convert = OrderFlow.pri_box().pip(Event);
        let pms = ori + convert + ono + ta2 + ori;
        let data = di.calc(pms)[0].clone();
        let exit_dire = if let Lo = self.0 { -1. } else { 1. };
        Box::new(move |e, _o| {
            data[e] == exit_dire
        })
    }
}

#[ta_derive]
pub struct WrapperTaCond(pub Box<dyn Cond>);


#[typetag::serde]
impl Ta for WrapperTaCond {
    fn calc_di(&self,di: &Di) -> avv32 {
        let f = self.0.cond(di);
        let (cond_res, cond_index, _) = (0..di.size())
            .fold((Vec::with_capacity(di.size()), Vec::with_capacity(di.size()), 0.), |mut accu, i| {
                let res = if f(i, i) {
                    accu.2 = i as f32;
                    1.
                } else {
                    0.
                };
                accu.0.push(res);
                accu.1.push(accu.2);
                accu
            });
        vec![cond_res.to_arc(), cond_index.to_arc(), di.c()]
    }
    fn calc_da(&self,da:Vec< &[f32]> ,_di: &Di) -> vv32 {
        da.into_map(|x| x.to_vec())
    }
}

#[ta_derive2]
pub struct cond3 {
    pub dire: Dire,
    pub cond: Box<dyn Cond>,
    pub passed_thre: usize,
    pub hold_thre: f32,
}

#[typetag::serde(name = "oftick_cond3")]
impl Cond for cond3 {
    fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
        let data = di.calc(ori + ono + WrapperTaCond(self.cond.clone()));
        let c_vec = di.c();
        let passed_thre= self.passed_thre;
        let hold_thre = di.pcon.ticker.info().tz * self.hold_thre;
        match self.dire {
            Lo => {
                Box::new(move |e, _o| {
                    let last_i = data[1][e] as usize;
                    e - last_i <= passed_thre
                        &&  c_vec[e] <= c_vec[last_i] - hold_thre
                })
            }
            Sh => {
                Box::new(move |e, _o| {
                    let last_i = data[1][e] as usize;
                    e - last_i <= passed_thre
                        &&  c_vec[e] >= c_vec[last_i] + hold_thre
                })
            }
        }
    }
}

#[ta_derive2]
pub struct cond4(pub Dire, pub f32);

#[typetag::serde(name = "oftick_cond4")]
impl Cond for cond4 {
    fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
        let convert = OrderFlow.pri_box().pip(Event);
        let pms = ori + convert + ono + ta2 + ori;
        let data = di.calc(pms)[0].clone();
        let v = di.v();
        let num_thre = self.1;
        let exit_dire = if let Lo = self.0 { -1. } else { 1. };
        Box::new(move |e, _o| {
            data[e] == exit_dire && v[e] >= num_thre
        })
    }
}

#[ta_derive2]
pub struct cond5 {
    pub dire: Dire,
    pub cond: Box<dyn Cond>,
    pub passed_thre: usize,
    pub passed_thre2: usize,
    pub hold_thre: f32,
}

#[typetag::serde(name = "oftick_cond5")]
impl Cond for cond5 {
    fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
        let data = di.calc(ori + ono + WrapperTaCond(self.cond.clone()));
        let c_vec = di.c();
        let passed_thre= self.passed_thre;
        let passed_thre2= self.passed_thre2;
        let hold_thre = di.pcon.ticker.info().tz * self.hold_thre;
        match self.dire {
            Lo => {
                let cc = di.c().roll(RollFunc::Max, RollOps::N(passed_thre2));
                Box::new(move |e, _o| {
                    let last_i = data[1][e] as usize;
                    e - last_i <= passed_thre
                    && c_vec[e] == cc[e] - hold_thre
                })
            }
            Sh => {
                let cc = di.c().roll(RollFunc::Min, RollOps::N(passed_thre2));
                Box::new(move |e, _o| {
                    let last_i = data[1][e] as usize;
                    e - last_i <= passed_thre
                    && c_vec[e] >= cc[e] + hold_thre
                })
            }
        }
    }
}

#[ta_derive2]
pub struct cond6 {
    pub dire: Dire,
    pub cond: Box<dyn Cond>,
    pub passed_thre: usize,
    pub passed_thre2: usize,
    pub hold_thre: f32,
}

#[typetag::serde(name = "oftick_cond6")]
impl Cond for cond6 {
    fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
        let data = di.calc(ori + ono + WrapperTaCond(self.cond.clone()));
        let c_vec = di.c();
        let passed_thre= self.passed_thre;
        let passed_thre2= self.passed_thre2;
        let hold_thre = di.pcon.ticker.info().tz * self.hold_thre;
        match self.dire {
            Lo => {
                let cc = di.c().roll(RollFunc::Max, RollOps::N(passed_thre2)).lag(1);
                Box::new(move |e, _o| {
                    let last_i = data[1][e] as usize;
                    e - last_i <= passed_thre
                    && c_vec[e] >= cc[e] - hold_thre
                })
            }
            Sh => {
                let cc = di.c().roll(RollFunc::Min, RollOps::N(passed_thre2));
                Box::new(move |e, _o| {
                    let last_i = data[1][e] as usize;
                    e - last_i <= passed_thre
                    && c_vec[e] >= cc[e] + hold_thre
                })
            }
        }
    }
}

pub fn get_econd_box(n: f32) -> CondBox {
    use crate::econd::exit_by_tick_size;
    let cond_e1 = exit_by_tick_size(Lo, n);
    let cond_e2 = exit_by_tick_size(Sh, n);
    let cond_e3 = of::inter::exit_by_last_time(0, 100);
    let cond_e4 = of::inter::exit_by_jump_time;
    cond_e1.cond_box() | cond_e2 | cond_e3 | cond_e4
}

pub fn get_econd_box2(dire: Dire, n1: f32, n2: f32) -> CondBox {
    use crate::econd::exit_by_tick_size;
    let cond_e1 = exit_by_tick_size(dire, n1);
    let cond_e2 = exit_by_tick_size(!dire, n2);
    let cond_e3 = of::inter::exit_by_last_time(0, 100);
    let cond_e4 = of::inter::exit_by_jump_time;
    cond_e1.cond_box() | cond_e2 | cond_e3 | cond_e4
}


#[ta_derive2]
pub struct cond_tick_equal{
    pub n   : usize,
    pub ops : RollFunc,
}

#[typetag::serde(name = "oftick_cond_tick_equal")]
impl Cond for cond_tick_equal {
    fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
        let data = di.c().roll(self.ops, RollOps::N(self.n));
        let c = di.c();
        Box::new(move |e, _o| {
            c[e] == data[e]
        })
    }
}

#[ta_derive2]
pub struct cond_tick_gap {
    pub n: usize,
    pub gap: f32,
}

#[typetag::serde(name = "oftick_cond_tick_gap")]
impl Cond for cond_tick_gap {
    fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
        let data_h = di.c().roll(RollFunc::Max, RollOps::N(self.n));
        let data_l = di.c().roll(RollFunc::Min, RollOps::N(self.n));
        let gap = izip!(data_h, data_l).map(|(x, y)| x - y).collect_vec();
        let thre = di.pcon.ticker.info().tz * self.gap;
        Box::new(move |e, _o| {
            gap[e] <= thre
        })
    }
}

#[ta_derive2]
pub struct cond_lag_gap {
    pub window: usize,
    pub back: usize,
    pub gap: f32,
}

#[typetag::serde(name = "oftick_cond_lag_gap")]
impl Cond for cond_lag_gap {
    fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
        let tz = di.pcon.ticker.info().tz;
        let gap_vec = izip!(
            di.h().roll(RollFunc::Max, RollOps::N(self.window)),
            di.l().roll(RollFunc::Min, RollOps::N(self.window)),
        )
            .map(|(x, y)| x - y)
            .collect_vec()
            .lag(self.back);
        let thre = self.gap * tz;
        Box::new(move |e, _o| {
            gap_vec[e] >= thre
        })
    }
}


#[ta_derive2]
pub struct cond_mean(pub Dire, pub usize, pub f32);

#[typetag::serde(name = "oftick_cond_mean")]
impl Cond for cond_mean {
    fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
        let l_roll = di.l().roll(RollFunc::Min, RollOps::N(self.1));
        let h_roll = di.h().roll(RollFunc::Max, RollOps::N(self.1));
        let gap_vec = izip!(l_roll, h_roll)
            .map(|(x, y)| (y - x) * self.2 + x)
            .collect_vec()
            .lag(1);
        let c = di.c().lag(1);
        match self.0 {
            Lo => {
                Box::new(move |e, _o| {
                    c[e] >= gap_vec[e]
                })
            }
            Sh => {
                Box::new(move |e, _o| {
                    c[e] <= gap_vec[e]
                })
            }
        }
    }
}

#[ta_derive2]
pub struct cond_time_elapse(pub i64);

#[typetag::serde(name = "oftick_cond_time_elapse")]
impl Cond for cond_time_elapse {
    fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
        let t = di.t();
        let thre = self.0;
        Box::new(move |e, o| {
            (t[e] - t[o]).num_seconds() > thre
        })
    }
}

#[ta_derive2]
pub struct non_jump_time;

#[typetag::serde(name = "non_jump_time")]
impl Cond for non_jump_time {
    fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
        let range1 = 101400.to_tt() .. 103100.to_tt();
        let range2 = 112900.to_tt() .. 133100.to_tt();
        let range3 = 145900.to_tt() .. 210500.to_tt();
        let range4 = 225900.to_tt() .. 230000.to_tt();
        let range5 = 0.to_tt() .. 90500.to_tt();
        let t_vec = di.t();
        Box::new(move |e, _o| {
            let t_now = t_vec[e].time();
            range1.contains(&t_now)
            || range2.contains(&t_now)
            || range3.contains(&t_now)
            || range4.contains(&t_now)
            || range5.contains(&t_now)
        })
    }
}

#[ta_derive2]
pub struct CumChange(pub usize);

#[typetag::serde]
impl Ta for CumChange {
    fn calc_da(&self,da:Vec< &[f32]>, di: &Di) -> vv32 {
        let tz = di.pcon.ticker.info().tz;
        let mut res = da[0]
            .windows(2)
            .map(|x| (*x.last().unwrap() - *x.first().unwrap()).abs() / tz)
            .collect_vec()
            .roll(RollFunc::Sum, RollOps::N(self.0));
        res.insert(0, 0.);
        vec![res]
    }
}

#[ta_derive2]
pub struct SimpleChange(pub Dire);

#[typetag::serde]
impl Cond for SimpleChange {
    fn cond<'a>(&self,di: &'a Di) -> LoopSig<'a> {
        let c = di.c();
        Box::new(move |e, _o| {
            e > 0 && c[e] > c[e - 1]
        })   
    }
}



#[derive(Clone)]
pub struct RecurOpt {
    pub ticker: Ticker,
    pub dire: Dire,
    pub filters: Vec<CondBox>,
    pub cond_exit: CondBox,
}

impl RecurOpt {
    pub fn gen_ptm(&self) -> Ptm {
        let mut cond_base = cond2(self.dire).cond_box();
        self.filters
            .iter()
            .for_each(|x| {
                cond_base = cond_base.clone() & x.clone();
            });
        Ptm::Ptm3(
            m1.clone(),
            self.dire,
            cond_base,
            self.cond_exit.clone(),
        )
    }
}

lazy_static! {
    pub static ref recur_opt_SA_Lo: RecurOpt = {
        RecurOpt {
            ticker: SAer,
            dire: Lo,
            filters: vec![
                cond_tick_equal { n: 2, ops: RollFunc::Max }.cond_box(),
                Iocond { 
                    pms: ori + Event(rl1mall.pri_box()) + ono + Atr(30) + rank_day + vori,
                    range: 0. .. 80.,
                }.cond_box(),
                Iocond { 
                    pms: ori + Event(rl1mall.pri_box()) + ono + Rsi(30) + rank_day + vori,
                    range: 0. .. 80.,
                }.cond_box(),
            ],
            cond_exit: get_econd_box2(Lo, 2., 2.)
        }
    };
    pub static ref recur_opt_SA_Sh: RecurOpt = {
        RecurOpt {
            ticker: SAer,
            dire: Sh,
            filters: vec![
                cond_tick_equal { n: 2, ops: RollFunc::Min }.cond_box(),
                Iocond { 
                    pms: ori + Event(rl1mall.pri_box()) + ono + Rsi(30) + rank_day + vori,
                    range: 70. .. 81.,
                }.cond_box(),
            ],
            cond_exit: get_econd_box2(Sh, 2., 2.)
        }
    };
    pub static ref recur_opt_ss_Lo: RecurOpt = {
        RecurOpt {
            ticker: sser,
            dire: Lo,
            filters: vec![
                cond_tick_equal { n: 2, ops: RollFunc::Max }.cond_box(),
                cond_tick_gap { n: 15, gap: 1. }.cond_box(),
                Iocond { 
                    pms: ori + Event(rl1mall.pri_box()) + ono + Atr(30) + rank_day + vori,
                    range: 10. .. 40.,
                }.cond_box(),
            ],
            cond_exit: get_econd_box2(Lo, 2., 2.)
        }
    };
    pub static ref recur_opt_al_Lo: RecurOpt = {
        RecurOpt {
            ticker: aler,
            dire: Lo,
            filters: vec![
                cond_tick_equal { n: 2, ops: RollFunc::Max }.cond_box(),
                cond_tick_gap { n: 15, gap: 1. }.cond_box(),
                Iocond { 
                    pms: ori + Event(rl1mall.pri_box()) + ono + Atr(30) + rank_day + vori,
                    range: 10. .. 40.,
                }.cond_box(),
            ],
            cond_exit: get_econd_box2(Lo, 2., 2.)
        }
    };
    pub static ref recur_opt_TA_Sh: RecurOpt = {
        RecurOpt {
            ticker: TAer,
            dire: Sh,
            filters: vec![
                cond_tick_equal { n: 2, ops: RollFunc::Min }.cond_box(),
                cond_tick_gap { n: 10, gap: 2. }.cond_box(),
                Iocond { 
                    pms: ori + Event(rl1mall.pri_box()) + ono + Rsi(30) + rank_day + vori,
                    range: 40. .. 60.,
                }.cond_box(),
            ],
            cond_exit: get_econd_box2(Sh, 2., 2.)
        }
    };
    pub static ref recur_opt_ss_Sh: RecurOpt = {
        RecurOpt {
            ticker: sser,
            dire: Sh,
            filters: vec![
                cond_tick_equal { n: 2, ops: RollFunc::Min }.cond_box(),
                cond_tick_gap { n: 10, gap: 2. }.cond_box(),
                !Iocond { 
                    pms: ori + Event(rl1mall.pri_box()) + ono + Atr(30) + rank_day + vori,
                    range:20. .. 50.,
                }.cond_box(),
                Iocond { 
                    pms: ori + Event(rl1mall.pri_box()) + ono +  RollTa(KlineType::Close, RollFunc::Std, RollOps::N(10)) + rank_day + vori,
                    range: 70. .. 101.,
                }.cond_box(),
                !Iocond { 
                    pms: ori + Event(rl1mall.pri_box()) + ono +  Macd(10, 20, 10) + rank_day + vori,
                    range: 0. .. 5.,
                }.cond_box(),
            ],
            cond_exit: get_econd_box2(Sh, 2., 2.)
        }
    };
}