use std::collections::HashSet;
use crate::prelude::{Hold, TickData};
use super::super::bt::*;
use chrono::Timelike;
use qust_ds::prelude::*;


pub enum UpdatedData {
    TickData(TickData),
    Hold(Hold),
}
pub struct UpdatedDataIndex {
    pub index: usize,
    pub data: UpdatedData,
}

#[derive(Debug)]
pub struct UpdatedTickDataIndex {
    pub index: usize,
    pub data: TickData,
}

pub trait UpdatedPool {
    fn updated_pool(&self) -> FnMutBox<UpdatedTickDataIndex, Option<Vec<TickData>>>;
}

#[derive(Debug, Clone)]
pub struct AllEmergedQue(pub usize);

impl UpdatedPool for AllEmergedQue {
    fn updated_pool(&self) -> FnMutBox<UpdatedTickDataIndex, Option<Vec<TickData>>> {
        let mut tick_cache = repeat_to_vec(|| Option::<TickData>::None, self.0);
        let mut cached_set: HashSet<usize> = HashSet::with_capacity(self.0); 
        Box::new(move |updated_tick_data_index| {
            let index = updated_tick_data_index.index;
            let tick_data = updated_tick_data_index.data;
            tick_cache[index] = Some(tick_data);
            cached_set.insert(index);
            if cached_set.len() != self.0 {
                return None;
            }
            cached_set.clear();
            tick_cache
                .iter_mut()
                .map(|x| x.take().unwrap())
                .collect_vec()
                .pip(Some)
        })
    }
}


#[derive(Debug)]
pub struct MillisAlignment(pub usize);

impl UpdatedPool for MillisAlignment {
    fn updated_pool(&self) -> FnMutBox<UpdatedTickDataIndex, Option<Vec<TickData>>> {
        let mut tick_time_last = dt::default().modify();
        let mut tick_cache = repeat_to_vec(|| Option::<TickData>::None, self.0);
        let mut cached_set: HashSet<usize> = HashSet::with_capacity(self.0); 
        let mut tick_vec_last = repeat_to_vec(TickData::default, self.0);
        Box::new(move |updated_tick_data_index| {
            // loge!("ctp", "{:?}", updated_tick_data_index);
            let index = updated_tick_data_index.index;
            let tick_data = updated_tick_data_index.data;
            let tick_time_now = tick_data.t.modify();
            let res_ffill = if tick_time_now > tick_time_last && !tick_cache.check_all(|x| x.is_none()) {
                cached_set.clear();
                tick_cache
                    .iter_mut()
                    .zip(tick_vec_last.iter())
                    .map(|(x, y)| x.take().unwrap_or_else(|| y.clone()))
                    .collect_vec()
                    .pip(Some)
            } else {
                None
            };
            tick_vec_last[index] = tick_data.clone();
            tick_cache[index] = Some(tick_data);
            cached_set.insert(index);
            tick_time_last = tick_time_now;
            match res_ffill {
                Some(data) => Some(data),
                None if cached_set.len() == self.0 => {
                    cached_set.clear();
                    tick_cache
                        .iter_mut()
                        .map(|x| x.take().unwrap())
                        .collect_vec()
                        .pip(Some)
                }
                _ =>  None,
            }
        })
    }
}


pub trait ModifyMillis {
    type Output;
    fn modify(&self) -> Self::Output;
}
impl ModifyMillis for [dt] {
    type Output = Vec<dt>;
    fn modify(&self) -> Self::Output {
        self.iter()
            .map(|x| {
                x.modify()
            })
            .collect_vec()
    }
}

impl ModifyMillis for dt {
    type Output = dt;
    fn modify(&self) -> Self::Output {
        let millis = self.and_utc().timestamp_subsec_millis();
        let millis_new = if let 0..500 = millis {
            0
        } else {
            500
        };
        self.with_nanosecond(millis_new * 1_000_000).unwrap()
    }
}
