use std::collections::VecDeque;
use std::collections::HashSet;
use itertools::Itertools;
use qust_ds::prelude::*;
use super::super::bt::*;
use super::super::algo::*;
use crate::live::prelude::Hold;
use crate::loge;
use crate::prelude::OrderAction;
use crate::prelude::OrderTarget;
use crate::prelude::Ticker;
use crate::prelude::{ HasLen, TickData};
use super::update_sync::*;


#[derive(Debug, Default)]
pub struct StreamTickHold {
    pub tick_data: TickData,
    pub hold: Hold,
}

#[derive(Debug)]
pub struct StreamTickHoldPool {
    pub data: Vec<StreamTickHold>,
}

impl HasLen for StreamTickHoldPool {
    fn size(&self) -> usize {
        self.data.len()
    }
}

impl StreamTickHoldPool
{
    pub fn from_len(i: usize) -> Self {
        Self {
            data: repeat_to_vec(Default::default, i)
        }
    }
}

pub type RetFnCrossUpdatedData<'a> = FnMutBox<'a, UpdatedData, Option<Vec<OrderAction>>>;
pub type RetFnCrossUpdatedDataIndex<'a> = FnMutBox<'a, UpdatedDataIndex, Option<Vec<OrderAction>>>;
pub type RetFnCrossAction<'a> = Box<dyn FnMut(&StreamTickHoldPool) -> Vec<OrderAction> + 'a>;
pub type RetFnCrossTarget<'a> = Box<dyn FnMut(Vec<&TickData>) -> Vec<OrderTarget> + 'a>;

pub trait GetTickerVec {
    fn get_ticker_vec(&self) -> Vec<Ticker>;
    fn pool_size(&self) -> usize {
        self.get_ticker_vec().len()
    }
}

pub trait CrossUpdateData {
    fn cross_updated_data(&self) -> RetFnCrossUpdatedData;
}

pub trait CondCrossAction {
    fn cond_cross_action(&self) -> RetFnCrossAction;
}

pub trait CondCrossTarget {
    fn cond_cross_target(&self) -> RetFnCrossTarget;
}

impl<T, N> CondCrossAction for WithInfo<T, N>
where
    T: CondCrossTarget + GetTickerVec,
    N: Algo,
{
    fn cond_cross_action(&self) -> RetFnCrossAction {
        let pool_size = self.data.pool_size();
        let mut cond_ops = self.data.cond_cross_target();
        let mut algo_ops_vec = repeat_to_vec(|| self.info.algo(), pool_size);
        Box::new(move |stream| {
            let tick_data_vec = stream.data.iter().map(|x| &x.tick_data).collect_vec();
            let order_target_vec = cond_ops(tick_data_vec);
            algo_ops_vec
                .iter_mut()
                .zip(order_target_vec)
                .zip(stream.data.iter())
                .map(|((algo_ops, order_target), stream_data)| {
                    let stream_algo = StreamAlgo {
                        stream_api: StreamApiType { tick_data: &stream_data.tick_data, hold: &stream_data.hold },
                        order_target,
                    };
                    algo_ops(&stream_algo)
                })
                .collect_vec()
        })
    }
}

pub trait CondCrossUpdatedDataIndex {
    fn cond_cross_updated_data_index(&self) -> RetFnCrossUpdatedDataIndex;
}

impl<T, N> CondCrossUpdatedDataIndex for WithInfo<T, N>
where
    T: CondCrossAction + GetTickerVec,
    N: UpdatedPool,
{
    fn cond_cross_updated_data_index(&self) -> RetFnCrossUpdatedDataIndex {
        let pool_size = self.data.get_ticker_vec().len();
        let mut stra_ops = self.data.cond_cross_action();
        let mut tick_cache_ops = self.info.updated_pool();
        let mut stream_th = StreamTickHoldPool::from_len(pool_size);
        Box::new(move |updated_data_index| {
            let index = updated_data_index.index;
            let updated_data = updated_data_index.data;
            match updated_data {
                UpdatedData::TickData(tick_data) => {
                    let updated_tick_data_index = UpdatedTickDataIndex {
                        index, 
                        data: tick_data,
                    };
                    let tick_data_updated = tick_cache_ops(updated_tick_data_index)?;
                    stream_th.data
                        .iter_mut()
                        .zip(tick_data_updated)
                        .for_each(|(stream_data, tick_data)| {
                            stream_data.tick_data = tick_data;
                        });
                }
                UpdatedData::Hold(hold) => {
                    // if stream_th.data[index].hold == hold {
                    //     return None;
                    // }
                    stream_th.data[index].hold = hold;
                }
            }
            let res = stra_ops(&stream_th);
            Some(res)
        })

    }
}

impl<T, N> GetTickerVec for WithInfo<T, N>
where
    T: GetTickerVec,
{
    fn get_ticker_vec(&self) -> Vec<Ticker> {
        self.data.get_ticker_vec()
    }
}
