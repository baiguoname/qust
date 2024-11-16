use std::array;
use std::collections::HashSet as hs;
use std::hash::{Hash, Hasher};

use chrono::Timelike;
use itertools::{izip, Itertools};
use qust::live::bt::TradeInfo;
use qust::prelude::*;
use qust_io::prelude::{Stats, StatsRes};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct cond {
    pub ticker: Ticker,
    tz: f32,
    pub in_bounds: f32,//主ticker没有突破？
    pub n: f32,//过去的data是否处于平稳状态？
    pub m: f32,//次ticker是否突破？
    pub l: usize,//判断平稳的期数？
}
impl Eq for cond {}
impl Hash for cond {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.ticker.hash(state);
        self.tz.to_bits().hash(state);
        self.in_bounds.to_bits().hash(state);
        self.n.to_bits().hash(state);
        self.m.to_bits().hash(state);
        self.l.hash(state);
    }
}

impl cond {

    pub fn new(ticker: Ticker, in_bounds: f32, n: f32, m: f32, l: usize) -> Self {
        Self {ticker, tz: ticker.info().tz, in_bounds, n, m, l }
    }
    fn find_peace(&self, data: &[f32]) -> Option<f32> {
        let data_len = data.len();
        if data_len < 200 {
            return None;
        }
        let data_k = &data[data_len - self.l..];
        let value_max = data_k.max();
        let value_min = data_k.min();
        if (value_max - value_min) >= self.tz * self.n {
            return None;
        }
        Some(data_k.mean())
    }

    fn p_in_bounds(&self, dire: Dire, tick_data: &TickData, last_mean: f32) -> bool {
        match dire {
            Dire::Lo => tick_data.ask1 <= last_mean + self.in_bounds * self.tz,
            Dire::Sh => tick_data.bid1 >= last_mean - self.in_bounds * self.tz,
        }
    }

    fn s_trigger(&self, dire: Dire, tick_data: &TickData, last_mean: f32) -> bool {
        match dire {
            Dire::Lo => tick_data.ask1 >= last_mean + self.m * self.tz,
            Dire::Sh => tick_data.bid1 <= last_mean - self.m * self.tz,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct cond_vec {
    pub data: Vec<cond>,
    pub elapsed_secs: i64,//过去多少秒，平稳消失？
    pub trigger_thre: usize,//多个个次ticker突破算突破？
}

impl PartialEq for cond_vec {
    fn eq(&self, other: &Self) -> bool {
        self.elapsed_secs == other.elapsed_secs && 
           self.trigger_thre == other.trigger_thre && {
                self.data[0].ticker == other.data[0].ticker && 
                self.data[0].in_bounds == other.data[0].in_bounds &&
                self.data[0].n == other.data[0].n && 
                self.data[0].l == other.data[0].l 
           } &&
           self.get_hs() == other.get_hs()
    }
}
impl Eq for cond_vec {}
impl Hash for cond_vec {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut d = self.get_hs().into_iter().collect_vec();
        d.sort_by(|a, b| a.ticker.partial_cmp(&b.ticker).unwrap());
        d.hash(state);
        {
            self.data[0].ticker.hash(state);
            self.data[0].in_bounds.to_bits().hash(state);
            self.data[0].n.to_bits().hash(state);
            self.data[0].l.hash(state);
        }
        self.elapsed_secs.hash(state);
        self.trigger_thre.hash(state);
    }
}

impl cond_vec {
    fn get_hs(&self) -> hs<cond> {
        let mut res = hs::new();
        for k in self.data.iter().skip(1) {
            let c = cond {
                ticker: k.ticker,
                tz: 0.,
                in_bounds: 0.,
                n: k.n,
                m: k.m,
                l: k.l,
            };
            res.insert(c);
        }
        res
    }
    fn s_trigger_counts(&self, dire: Dire, ticks: &[&TickData], mean: &[f32]) -> usize {
        let mut res = 0usize;
        for ((c, &tick_data), mean_value) in self.data[1..].iter().zip(ticks.iter()).zip(mean.iter()) {
            if c.s_trigger(dire, tick_data, *mean_value) {
                res += 1;
            }
        }
        res
    }

    fn is_t_in_bounds_met(&self, time_now: &[&TickData], time_last: &[dt]) -> bool {
        for (x, y) in time_now.iter().zip(time_last.iter()) {
            if (x.t - *y).num_seconds() > self.elapsed_secs {
                return false;
            }
        }
        true
    }

    fn is_p_in_bounds_met(&self, dire: Dire, tick: &TickData, mean: f32) -> bool {
        self.data[0].p_in_bounds(dire, tick, mean)
    }

    fn is_trigger_met(&self, dire: Dire, ticks: &[&TickData], mean: &[f32]) -> bool {
        self.s_trigger_counts(dire, ticks, mean) >= self.trigger_thre
    }
    
    pub fn replicated(&self) -> bool {
        let n = self.data.len();
        let m = self.data
            .iter()
            .map(|x| x.ticker)
            .collect::<std::collections::HashSet<_>>()
            .len();
        n == m
    }
}

impl GetTickerVec for cond_vec {
    fn get_ticker_vec(&self) -> Vec<Ticker> {
        self.data.iter().map(|x| x.ticker).collect_vec()
    }
}

impl CondCrossTarget for cond_vec {
    fn cond_cross_target(&self) -> RetFnCrossTarget {
        let data_len = self.data.len();
        let mut last_tick_time = vec![dt::default(); data_len];
        let mut last_open_time = dt::default();
        let mut last_target = vec![OrderTarget::No; data_len];
        let mut last_mean = vec![0.; data_len];
        let range1 = "14:56:00".to_tt() .. "15:00:00".to_tt();
        let range2 = "22:56:00".to_tt() .. "23:00:00".to_tt();
        let mut tick_record  = repeat_to_vec(std::vec::Vec::new, data_len);
        let mut last_mean_time = vec![dt::default(); data_len];
        // let tickers = self.get_ticker_vec();
        Box::new(move |stream| {
            // last_tick_time.iter_mut().zip(stream.iter()).zip(tick_record.iter_mut())
            //     .for_each(|((x, &y), z)| {
            //         if &y.t > x {
            //             *x = y.t;
            //             z.push(y.c);
            //         }
            //     });
            stream.iter()
                .zip(self.data.iter())
                .zip(tick_record.iter_mut())
                .zip(last_tick_time.iter_mut())
                .zip(last_mean.iter_mut())
                .zip(last_mean_time.iter_mut())
                // .zip(tickers.iter())
                .for_each(|(((((a, b), c), d), e), f)| {
                    if &a.t > d {
                        *d = a.t;
                        c.push(a.c);
                        if let Some(g) = b.find_peace(c) {
                            *e = g;
                            *f = a.t;
                            // loge!(*z, "{:?} -- {}", a.t, g);
                        }
                    }

                });
            {
                let ts = stream[0].t.time();
                if range1.contains(&ts) || range2.contains(&ts) {
                    last_target[0] = OrderTarget::No;
                    return last_target.to_vec()
                }
            }
            match &last_target[0] {
                OrderTarget::No => {
                    if self.is_t_in_bounds_met(&stream, &last_mean_time) {
                        if self.is_p_in_bounds_met(Dire::Lo, stream[0], last_mean[0]) &&
                            self.is_trigger_met(Dire::Lo, &stream[1..], &last_mean[1..])
                            {
                                // loge!("ctp", "aaa");
                                last_target[0] = OrderTarget::Lo(1.);
                                last_open_time = stream[0].t;
                        } else if self.is_p_in_bounds_met(Dire::Sh, stream[0], last_mean[0]) &&
                                self.is_trigger_met(Dire::Sh, &stream[1..], &last_mean[1..])
                            {
                                last_target[0] = OrderTarget::Sh(1.);
                                last_open_time = stream[0].t;
                                
                            }
                    }
                }
                OrderTarget::Lo(_) => {
                    if (stream[0].t - last_open_time).num_seconds() >= 100 || 
                        self.data[0].s_trigger(Dire::Lo, stream[0], last_mean[0]) {
                        last_target[0] = OrderTarget::No;
                    }
                }
                OrderTarget::Sh(_) => {
                    if (stream[0].t - last_open_time).num_seconds() >= 100 || 
                        self.data[0].s_trigger(Dire::Sh, stream[0], last_mean[0]) {
                            last_target[0] = OrderTarget::No;
                    }
                }
            }
            last_target.to_vec()
        })

    }
}

pub fn backtestwrapper(cond: cond_vec, price_tick: &hm<Ticker, Vec<TickData>>) -> Option<StatsRes> {
    use qust::live::cross::prelude::GetTickerVec;
    let ticker0 = cond.get_ticker_vec()[0];
    let res = backtest(cond, price_tick);
    if res.len() < 200 {
        return None;
    }
    res.with_info(ticker0).into_pnl_res().stats().pip(Some)
}
pub fn backtest(cond: cond_vec, price_tick: &qust::prelude::hm<Ticker, Vec<TickData>>) -> Vec<TradeInfo> {
    let l = cond.pool_size();
    let mut res = WithInfo {
        data: cond,
        info: qust::prelude::AlgoTarget
    }
        .with_info(AllEmergedQue(l))
        .with_info(MatchSimnow.btmatch_box())
        .bt_tick(price_tick);
    res.remove(0)
}

pub fn backtest2(cond: cond_vec, price_tick: &qust::prelude::hm<Ticker, Vec<TickData>>) -> Vec<TradeInfo> {
    let l = cond.pool_size();
    let mut res = WithInfo {
        data: cond,
        info: qust::prelude::AlgoTarget
    }
        .with_info(AllEmergedQue(l))
        .with_info(MatchMean.btmatch_box())
        .bt_tick(price_tick);
    res.remove(0)
}

pub fn get_cond_vec() -> Vec<cond> {
    itertools::iproduct!(
        tickers3.clone(),
        [1f32, 2.],
        [2f32, 4.],
        [5f32,  8.],
        [30usize, 50]
    )
        .map(|(ticker, in_bounds, n, m, l)| {
            cond::new(ticker, in_bounds, n, m, l)
        })
        .collect::<Vec<_>>()
}

pub fn get_cond_vec2() -> Vec<cond_vec> {
    let conds_p = get_cond_vec();
    let conds_s = conds_p.clone();
    itertools::iproduct!(conds_p, conds_s)
        .map(|(p, s)| {
            cond_vec {
                data: vec![p, s],
                elapsed_secs: 5,
                trigger_thre: 1,
            }
        })
        .filter(|x| x.replicated())
        .collect::<hs<_>>()
        .into_iter()
        .collect::<Vec<_>>()
}

pub fn get_cond_vec3() -> Vec<cond_vec> {
    let conds_vec2 = get_cond_vec2();
    let conds_s = get_cond_vec();
    let res: hs<_> = itertools::iproduct!(conds_vec2.into_iter(), conds_s.into_iter())
        .map(|(p, s)| {
            cond_vec {
                data: vec![p.data[0].clone(), p.data[1].clone(), s],
                elapsed_secs: 20,
                trigger_thre: 2,
            }
        })
        .filter(|x| x.replicated())
        .take(50000)
        .collect();
    res.into_iter().collect()
}