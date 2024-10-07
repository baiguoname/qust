#![allow(unused_imports)]
use crate::loge;
use crate::prelude::{TickData, Ticker};
use qust_ds::prelude::*;
use qust_derive::*;
use dyn_clone::{clone_trait_object, DynClone};
use serde::{Deserialize, Serialize};

use super::prelude::{HoldLocal, LiveTarget, OrderAction, RetFnAlgo };
use crate::sig::prelude::ToNum;

#[clone_trait]
pub trait Algo {
    fn algo(&self, ticker: Ticker) -> RetFnAlgo;
}

#[ta_derive]
pub struct TargetSimple;

#[typetag::serde]
impl Algo for TargetSimple {
    fn algo(&self, _ticker: Ticker) -> RetFnAlgo {
        Box::new(move |stream_algo| {
            use OrderAction::*;
            let target = stream_algo.live_target.to_num() as i32;
            let hold_local = stream_algo.stream_api.hold;
            let tick_data = stream_algo.stream_api.tick_data;
            let gap = target - hold_local.sum();
            match (gap, target, hold_local.yd_sh, hold_local.yd_lo, hold_local.td_sh, hold_local.td_lo) {
                (0, ..) => No,
                (_, 0.., 1.., ..) => LoCloseYd(hold_local.yd_sh, tick_data.bid1),
                (_, 0.., 0, _, 1.., _) => LoClose(hold_local.td_sh, tick_data.bid1),
                (0.., 0.., 0, _, 0, _) => LoOpen(gap, tick_data.bid1),
                (..=-1, 0.., 0, 1.., 0, 0..) => {
                    if hold_local.yd_lo >= -gap {
                        ShCloseYd(-gap, tick_data.ask1)
                    } else {
                        ShCloseYd(hold_local.yd_lo, tick_data.ask1)
                    }
                }
                (..=-1, 0.., 0, 0, 0, 1..) => ShClose(-gap, tick_data.ask1),
                (_, ..=-1, _, 1.., ..) => ShCloseYd(hold_local.yd_lo, tick_data.ask1),
                (_, ..=-1, _, 0, _, 1..) => ShClose(hold_local.td_lo, tick_data.ask1),
                (..=-1, ..=-1, _, 0, _, 0) => ShOpen(-gap, tick_data.ask1),
                (0.., ..=-1, 1.., 0, 0.., 0) => {
                    if hold_local.yd_sh >= gap {
                        LoCloseYd(gap, tick_data.bid1)
                    } else {
                        LoCloseYd(hold_local.yd_sh, tick_data.bid1)
                    }
                }
                (0.., ..=-1, 0, 0, 1.., 0) => LoClose(gap, tick_data.bid1),
                _ => panic!("something action wrong"),
            }
        })
    }
}

#[ta_derive]
#[derive(Default)]
pub struct TargetPriceDum {
    original_price: f32,
    exit_counts: usize,
    last_action: OrderAction,
    n_thre: usize,
    open_counts: usize,
    n_thre_open: usize,
}

impl TargetPriceDum {
    pub fn from_n_thre(n: usize, n_open: usize) -> Self {
        Self {
            n_thre: n,
            n_thre_open: n_open,
            ..Default::default()
        }
    }
}


#[ta_derive]
pub struct AlgoTargetAndPrice;


#[typetag::serde]
impl Algo for AlgoTargetAndPrice {
    fn algo(&self, ticker:Ticker) -> RetFnAlgo {
        Box::new(move |stream_algo| {
            let LiveTarget::OrderAction(target) = &stream_algo.live_target else { 
                panic!("wrong match algo: {} {:?}", ticker, stream_algo.live_target);
            };
            target.clone()
        })
    }
}

