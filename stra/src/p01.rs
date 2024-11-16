use chrono::Timelike;
use qust::prelude::*;

#[derive(Clone, Debug)]
pub struct cond {
    pub ticker0: Ticker,
    pub ticker1: Ticker,
    pub n0: f32,
    pub n1: f32,
    pub m: f32,
    pub l0: usize,
    pub l1: usize,
}

impl GetTickerVec for cond {
    fn get_ticker_vec(&self) -> Vec<Ticker> {
        vec![self.ticker0, self.ticker1]
    }
}

pub fn find_peace(data: &[f32], tz: f32, n: f32, l: usize) -> Option<f32> {
    let data_len = data.len();
    if data_len < 100 {
        return None;
    }
    let data_k = &data[data_len - l..];
    let value_max = data_k.max();
    let value_min = data_k.min();
    if (value_max - value_min) >= tz * n {
        return None;
    }
    Some(data_k.mean())
}

impl CondCrossTarget for cond {
    fn cond_cross_target(&self) -> RetFnCrossTarget {
        let tz0 = self.ticker0.info().tz;
        let tz1 = self.ticker1.info().tz;
        let mut last_time0 = dt::default();
        let mut last_time1 = dt::default();
        let mut tick_record0 = vec![];
        let mut tick_record1 = vec![];
        let mut last_target = vec![OrderTarget::No, OrderTarget::No];
        let mut last_mean0 = 0.;
        let mut last_mean1 = 0.;
        let mut last_record_time = dt::default();
        let mut elapsed = 0usize;
        let range1 = "14:56:00".to_tt() .. "15:00:00".to_tt();
        let range2 = "22:56:00".to_tt() .. "23:00:00".to_tt();
        Box::new(move |stream| {
            let tick0 = stream[0];
            let tick1 = stream[1];
            if tick0.t > last_time0 {
                last_time0 = tick0.t;
                tick_record0.push(tick0.c);
            }
            if tick1.t > last_time1 {
                last_time1 = tick1.t;
                tick_record1.push(tick1.c);
            }
            if range1.contains(&tick0.t.time()) || range2.contains(&tick0.t.time()) {
                last_target[0] = OrderTarget::No;
                return last_target.clone();
            }
            match (find_peace(&tick_record0, tz0, self.n0, self.l0), find_peace(&tick_record1, tz1, self.n1, self.l1)) {
                (Some(m0), Some(m_1)) => {
                    last_mean0 = m0;
                    last_mean1 = m_1;
                    elapsed = 0;
                    last_record_time = tick1.t;
                }
                _ => {
                    elapsed += 1;
                }
            }
            match &last_target[0] {
                OrderTarget::No => {
                    if elapsed < 4 && (tick1.t - last_record_time).num_minutes() <= 2  { 
                        if tick0.c <= last_mean0 + 1. * tz0 && tick1.c > last_mean1 + 4. * tz1 {
                            last_target[0] = OrderTarget::Lo(1.);
                        } else if tick1.c < last_mean1 - 4. * tz1 {
                            last_target[0] = OrderTarget::Sh(1.);
                        }
                    }
                }
                OrderTarget::Lo(_) => {
                    if tick1.c <= last_mean1 + 1. * tz1 || tick0.c >= last_mean0 + self.m * tz0 {
                        last_target[0] = OrderTarget::No;
                    }
                }
                OrderTarget::Sh(_) => {
                    if tick1.c >= last_mean1 - 1. * tz1 || tick0.c <= last_mean0 - self.m * tz0 {
                        last_target[0] = OrderTarget::No;
                    }
                }
            }
            last_target.clone()
        })
    }
}



