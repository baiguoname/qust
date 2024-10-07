use crate::idct::ta::Ta;
use crate::prelude::TradingPeriod;
use crate::prelude::{Calc, CalcSave, CalcSaveWrapper, Pre};
use crate::trade::di::*;
use chrono::Timelike;
use qust_derive::*;
use qust_ds::prelude::*;

pub struct FData<'f, T, N> {
    pub data: &'f T,
    pub f: N,
}

fn _find_day_index(time_vec: &[dt]) -> vuz {
    let mut res = vec![0usize];
    for i in 1..time_vec.len() {
        if time_vec[i].date() != time_vec[i - 1].date() {
            res.push(i);
        }
    }
    res.push(time_vec.len());
    res
}
pub fn find_day_index_night_flat(time_vec: avdt) -> vuz {
    let mut res = vec![0usize; time_vec.len()];
    let hour_vec = time_vec.iter().map(|x| x.hour()).collect_vec();
    let end_range = 13..16;
    for i in 1..res.len() {
        if end_range.contains(&hour_vec[i - 1]) && !end_range.contains(&hour_vec[i]) {
            res[i] = res[i - 1] + 1;
        } else {
            res[i] = res[i - 1];
        }
    }
    res
}
pub fn find_day_index_night(time_vec: avdt) -> vuz {
    let mut res = vec![0usize];
    let hour_vec = time_vec.iter().map(|x| x.hour()).collect_vec();
    let end_range = 13..16;
    for i in 1..time_vec.len() {
        if end_range.contains(&hour_vec[i - 1]) && !end_range.contains(&hour_vec[i]) {
            res.push(i);
        }
    }
    res.push(time_vec.len());
    res
}

#[ta_derive]
struct FindDayIndex(Pre);

impl CalcSave for FindDayIndex {
    type Output = vuz;
    fn calc_save(&self, di: &Di) -> Self::Output {
        self.0 .1.cut_index(di)
    }
}

pub(crate) fn find_day_index_night_pre(time_vec: &[dt]) -> vuz {
    let mut res = vec![0usize];
    let hour_vec = time_vec.map(|x| x.hour());
    let tt_vec = time_vec.map(|x| x.time());
    let end_range_light = 8..20;
    let end_range_night = 20..23;
    izip!(hour_vec.windows(2), tt_vec.windows(2))
        .enumerate()
        .for_each(|(i, (h, t))| {
            let (hour_pre, hour_now) = (h.first().unwrap(), h.last().unwrap());
            let is_in_light_pre = end_range_light.contains(hour_pre);
            let is_in_night_now = end_range_night.contains(hour_now);
            let is_in_cut = if is_in_light_pre && is_in_night_now {
                true
            } else {
                let time_pre = t.first().unwrap();
                let time_now = t.last().unwrap();
                let is_time_growing = time_pre > time_now;
                if is_in_light_pre && is_time_growing {
                    true
                } else {
                    let is_in_night_pre = end_range_night.contains(hour_pre);
                    is_in_night_pre && is_in_night_now && is_time_growing
                }
            };
            if is_in_cut {
                res.push(i + 1);
            }
        });
    res.push(time_vec.len());
    res
}

pub fn find_day_index_night_pro(time_vec: &[dt], di: &Di) -> vuz {
    let trading_period: TradingPeriod = di.pcon.ticker.into();
    match trading_period {
        TradingPeriod::LightNightMorn => find_day_index_night_pre(time_vec),
        _ => find_day_index_night_pre(time_vec),
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum Part {
    oos,
    ono,
}

impl Part {
    pub fn calc_part(&self, di: &Di, ta: Box<dyn Ta>) -> vv32 {
        let index_part = Pre(di.last_dcon(), self.clone())
            .pip(FindDayIndex)
            .pip(CalcSaveWrapper)
            .calc(di);
        let index_part = index_part.downcast_ref().unwrap();
        let data = ta.calc_di(di);
        let mut data_iter = self.cut_part(index_part, &data);
        let mut res_part = ta.calc_da(data_iter.next().unwrap(), di);
        let mut accu = init_a_matrix(data[0].len(), res_part.len());
        accu.vcat_other(&mut res_part);
        for data_part in data_iter {
            let mut res_part = ta.calc_da(data_part, di);
            accu.vcat_other(&mut res_part);
        }
        accu
    }
    fn cut_index(&self, di: &Di) -> vuz {
        match *self {
            Part::oos => {
                let time_vec = di.t();
                find_day_index_night_pro(&time_vec, di)
            }
            Part::ono => {
                vec![0, di.t().len()]
            }
        }
    }
    fn cut_part<'a>(
        &self,
        index_part: &'a vuz,
        data: &'a avv32,
    ) -> impl Iterator<Item = Vec<&'a [f32]>> {
        index_part.windows(2).map(|x| {
            data.iter()
                .map(|d| &d[*x.first().unwrap()..*x.last().unwrap()])
                .collect_vec()
        })
    }
}
