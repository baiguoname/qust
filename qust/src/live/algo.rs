use crate::prelude::{Dire, TickData, ToNum };
use super::order_types::*;
use super::bt::*;
use qust_ds::prelude::*;
use qust_derive::*;
use dyn_clone::{clone_trait_object, DynClone};

pub type OrderTargetAndPrice = WithDire<(f32, f32)>;
pub type RetFnAlgo = Box<dyn FnMut(&StreamAlgo) -> OrderAction +  'static>;
pub type WithAlgoBox<T> = WithInfo<T, AlgoBox>;


impl ToNum for OrderTargetAndPrice {
    fn to_num(&self) -> f32 {
        match self.info {
            Dire::Lo => self.data.0,
            Dire::Sh => -self.data.0
        }
    }
}

pub struct StreamAlgo<'a> {
    pub stream_api: StreamApiType<'a>,
    pub order_target: OrderTarget,
}

#[derive(Debug, Clone)]
pub struct StreamOrderTarget {
    pub tick_data: TickData,
    pub hold: Hold,
    pub num_price: OrderTarget,
}


#[clone_trait]
pub trait Algo {
    fn algo(&self) -> RetFnAlgo;
}

#[ta_derive2]
pub struct AlgoTarget;

#[typetag::serde]
impl Algo for AlgoTarget {
    fn algo(&self) -> RetFnAlgo {
        Box::new(move |stream| {
            let target = stream.order_target.to_num();
            let hold = stream.stream_api.hold.sum();
            let gap = target - hold;
            let mut res = OrderAction::No;
            let tick_data = stream.stream_api.tick_data;
            if gap == 0. {
                return res;
            }
            if gap > 0. {
                if hold < 0. {
                    res = OrderAction::LoClose(-hold, tick_data.bid1);
                } else if hold >= 0. {
                    res = OrderAction::LoOpen(gap, tick_data.bid1);
                }
            } else if gap < 0. {
                if hold > 0. {
                    res = OrderAction::ShClose(hold, tick_data.ask1);
                } else if hold <= 0. {
                    res = OrderAction::ShOpen(-gap, tick_data.ask1);
                }
            }
            res
        })
    }
}


#[ta_derive2]
pub struct AlgoTargetQuik;

#[typetag::serde]
impl Algo for AlgoTargetQuik {
    fn algo(&self) -> RetFnAlgo {
        Box::new(move |stream| {
            let target = stream.order_target.to_num();
            let hold = stream.stream_api.hold.sum();
            let gap = target - hold;
            let mut res = OrderAction::No;
            let tick_data = stream.stream_api.tick_data;
            if gap == 0. {
                return res;
            }
            if gap > 0. {
                if hold < 0. {
                    res = OrderAction::LoClose(-hold, tick_data.ask1);
                } else if hold >= 0. {
                    res = OrderAction::LoOpen(gap, tick_data.ask1);
                }
            } else if gap < 0. {
                if hold > 0. {
                    res = OrderAction::ShClose(hold, tick_data.bid1);
                } else if hold <= 0. {
                    res = OrderAction::ShOpen(-gap, tick_data.bid1);
                }
            }
            res
        })
    }
}

// impl Algo for AlgoTarget {
//     type Output = OrderActionTarget;
//     fn algo(&self) -> FnMutBox<'static, StreamOrderTarget, Self::Output> {
//         Box::new(move |stream| {
//             let target = stream.num_price.to_num();
//             let hold = stream.hold.sum();
//             let gap = target - hold;
//             let mut res = OrderActionTarget::No;
//             if gap == 0. {
//                 return res;
//             }
//             if gap > 0. {
//                 if hold < 0. {
//                     res = OrderActionTarget::LoClose(-hold);
//                 } else if hold >= 0. {
//                     res = OrderActionTarget::LoOpen(gap);
//                 }
//             } else if gap < 0. {
//                 if hold > 0. {
//                     res = OrderActionTarget::ShClose(hold);
//                 } else if hold <= 0. {
//                     res = OrderActionTarget::ShOpen(-gap);
//                 }
//             }
//             res
//         })
        
//     }
// }

// pub struct AlgoTargetAndPrice;

// impl Algo for AlgoTargetAndPrice {
//     fn algo(&self) -> FnMutBox<'static, StreamOrderTarget, OrderAction> {
//         let mut algo_target = AlgoTarget.algo();
//         Box::new(move |stream| {
//             let tick_data = stream.tick_data.clone();
//             let order_action_target = algo_target(stream);
//             match order_action_target {
//                 OrderActionTarget::No => {
//                     OrderAction::No
//                 }
//                 OrderActionTarget::LoOpen(i) => {
//                     OrderAction::LoOpen(i, tick_data.bid1)
//                 }
//                 OrderActionTarget::LoClose(i) => {
//                     OrderAction::LoClose(i, tick_data.bid1)
//                 }
//                 OrderActionTarget::ShOpen(i) => {
//                     OrderAction::ShOpen(i, tick_data.ask1)
//                 }
//                 OrderActionTarget::ShClose(i) => {
//                     OrderAction::ShClose(i, tick_data.ask1)
//                 }
//             }
//         })
//     }
// }


// pub struct AlgoTargetAndPrice2;

// impl Algo for AlgoTargetAndPrice2 {
//     type Output = OrderAction;
//     fn algo(&self) -> FnMutBox<'static, StreamOrderTarget, Self::Output> {
//         let mut algo_target = AlgoTarget.algo();
//         Box::new(move |stream| {
//             let tick_data = stream.tick_data.clone();
//             let order_action_target = algo_target(stream);
//             match order_action_target {
//                 OrderActionTarget::No => {
//                     OrderAction::No
//                 }
//                 OrderActionTarget::LoOpen(i) => {
//                     OrderAction::LoOpen(i, tick_data.ask1)
//                 }
//                 OrderActionTarget::LoClose(i) => {
//                     OrderAction::LoClose(i, tick_data.ask1)
//                 }
//                 OrderActionTarget::ShOpen(i) => {
//                     OrderAction::ShOpen(i, tick_data.bid1)
//                 }
//                 OrderActionTarget::ShClose(i) => {
//                     OrderAction::ShClose(i, tick_data.bid1)
//                 }
//             }
//         })
//     }
// }