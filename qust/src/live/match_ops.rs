use qust_derive::*;
use crate::trade::prelude::*;
use serde::{ Serialize, Deserialize };
use super::order_types::*;
use super::bt::*;
use dyn_clone::{clone_trait_object, DynClone};
use qust_ds::prelude::*;



pub type WithMatchBox<T> = WithInfo<T, BtMatchBox>;


#[derive(Debug)]
pub struct StreamBtMatch<'a> {
    pub tick_data: &'a TickData,
    pub hold: &'a mut Hold,
    pub order_action: &'a OrderAction,
}

pub type RetFnBtMatch<'a> = Box<dyn FnMut(StreamBtMatch) -> Option<TradeInfo> + 'a>;

#[clone_trait]
pub trait BtMatch {
    fn bt_match(&self) -> RetFnBtMatch;
}

#[ta_derive2]
pub struct MatchSimple;

#[typetag::serde]
impl BtMatch for MatchSimple {
    fn bt_match(&self) -> RetFnBtMatch {
        Box::new(move |stream_bt_match| {
        use OrderAction::*;
        let mut res = None;
        let tick_data = stream_bt_match.tick_data;
        let hold = stream_bt_match.hold;
        match stream_bt_match.order_action.clone() {
            LoOpen(i, price) => {
                if tick_data.c <= price {
                    res = Some(TradeInfo { time: tick_data.t, action: LoOpen(i, price) });
                    hold.lo += i;
                }
            }
            LoClose(i, price) => {
                if tick_data.c <= price {
                    // res = Some(TradeInfo { time: tick_data.t, action: LoClose(i, tick_data.c) });
                    res = Some(TradeInfo { time: tick_data.t, action: LoClose(i, price) });
                    hold.sh -= i;
                }
            }
            ShOpen(i, price) => {
                if tick_data.c >= price {
                    // res = Some(TradeInfo { time: tick_data.t, action: ShOpen(i, tick_data.c) });
                    res = Some(TradeInfo { time: tick_data.t, action: ShOpen(i, price) });
                    hold.sh += i;
                }
            }
            ShClose(i, price) => {
                if tick_data.c >= price {
                    // res = Some(TradeInfo { time: tick_data.t, action: ShClose(i, tick_data.c) });
                    res = Some(TradeInfo { time: tick_data.t, action: ShClose(i, price) });
                    hold.lo -= i;
                }
            }
            _ => { }
        }
        res
        })
    }
}

#[ta_derive2]
pub struct MatchSimnow;

fn middle_value(a: f32, b: f32, c: f32) -> f32 {
    if (a >= b) != (a >= c) {
        a
    } else if (b >= a) != (b >= c) {
        b
    } else {
        c
    }
}

#[typetag::serde]
impl BtMatch for MatchSimnow {
    fn bt_match(&self) -> RetFnBtMatch {
        Box::new(move |stream_bt_match| {
            use OrderAction::*;
            let tick_data = stream_bt_match.tick_data;
            let hold = stream_bt_match.hold;
            let mut res = None;
            match stream_bt_match.order_action.clone() {
                LoOpen(i, price) => {
                    if tick_data.ask1 <= price {
                        let match_price = middle_value(price, tick_data.c, tick_data.ask1);
                        res = Some(TradeInfo { time: tick_data.t, action: LoOpen(i, match_price)});
                        hold.lo += i;
                    }
                }
                LoClose(i, price) => {
                    if tick_data.ask1 <= price {
                        let match_price = middle_value(price, tick_data.c, tick_data.ask1);
                        res = Some(TradeInfo { time: tick_data.t, action: LoClose(i, match_price)});
                        hold.sh -= i;
                    }
                }
                ShOpen(i, price) => {
                    if tick_data.bid1 >= price {
                        let match_price = middle_value(price, tick_data.c, tick_data.bid1);
                        res = Some(TradeInfo { time: tick_data.t, action: ShOpen(i, match_price)});
                        hold.sh += i;
                    }
                }
                ShClose(i, price) => {
                    if tick_data.bid1 >= price {
                        let match_price = middle_value(price, tick_data.c, tick_data.bid1);
                        res = Some(TradeInfo { time: tick_data.t, action: ShClose(i, match_price)});
                        hold.lo -= i;
                    }
                }
                _ => { }
            }
            res
        })
    }
}

#[ta_derive2]
pub struct MatchOldBt;

#[typetag::serde]
impl BtMatch for MatchOldBt {
    fn bt_match(&self) -> RetFnBtMatch {
        let mut c = 0.;
        Box::new(move |stream_bt_match| {
            use OrderAction::*;
            let tick_data = stream_bt_match.tick_data;
            let hold = stream_bt_match.hold;
            if c == 0. {
                c = tick_data.c;
            }
            let res = match stream_bt_match.order_action.clone() {
                LoOpen(i, _) => {
                    hold.lo += i;
                    Some(TradeInfo { time: tick_data.t, action: LoOpen(i, c) })
                }
                LoClose(i, _) => {
                    hold.sh -= i;
                    Some(TradeInfo { time: tick_data.t, action: LoClose(i, c) })
                }
                ShOpen(i, _) => {
                    hold.sh += i;
                    Some(TradeInfo { time: tick_data.t, action: ShOpen(i, c) })
                }
                ShClose(i, _) => {
                    hold.lo -= i;
                    Some(TradeInfo { time: tick_data.t, action: ShClose(i, c) })
                }
                _ => { None }
            };
            c = stream_bt_match.tick_data.c;
            res
        })
    }
}


#[ta_derive2]
pub struct MatchMean;

#[typetag::serde]
impl BtMatch for MatchMean {
    fn bt_match(&self) -> RetFnBtMatch {
        let mut c = 0.;
        Box::new(move |stream_bt_match| {
            use OrderAction::*;
            let tick_data = stream_bt_match.tick_data;
            let hold = stream_bt_match.hold;
            if c == 0. {
                c = tick_data.c;
            }
            let p = (tick_data.c + c) / 2.;
            let res = match stream_bt_match.order_action.clone() {
                LoOpen(i, _) => {
                    hold.lo += i;
                    Some(TradeInfo { time: tick_data.t, action: LoOpen(i, p) })
                }
                LoClose(i, _) => {
                    hold.sh -= i;
                    Some(TradeInfo { time: tick_data.t, action: LoClose(i, p) })
                }
                ShOpen(i, _) => {
                    hold.sh += i;
                    Some(TradeInfo { time: tick_data.t, action: ShOpen(i, p) })
                }
                ShClose(i, _) => {
                    hold.lo -= i;
                    Some(TradeInfo { time: tick_data.t, action: ShClose(i, p) })
                }
                _ => { None }
            };
            c = tick_data.c;
            res
        })
    }
}

#[ta_derive2]
pub struct MatchSimnow2;


#[typetag::serde]
impl BtMatch for MatchSimnow2 {
    fn bt_match(&self) -> RetFnBtMatch {
        Box::new(move |stream_bt_match| {
            use OrderAction::*;
            let tick_data = stream_bt_match.tick_data;
            let hold = stream_bt_match.hold;
            let mut res = None;
            match stream_bt_match.order_action.clone() {
                LoOpen(i, price) => {
                    if tick_data.ask1 <= price {
                        res = Some(TradeInfo { time: tick_data.t, action: LoOpen(i, tick_data.c)});
                        hold.lo += i;
                    }
                }
                LoClose(i, price) => {
                    if tick_data.ask1 <= price {
                        res = Some(TradeInfo { time: tick_data.t, action: LoClose(i, tick_data.c)});
                        hold.sh -= i;
                    }
                }
                ShOpen(i, price) => {
                    if tick_data.bid1 >= price {
                        res = Some(TradeInfo { time: tick_data.t, action: ShOpen(i, tick_data.c)});
                        hold.sh += i;
                    }
                }
                ShClose(i, price) => {
                    if tick_data.bid1 >= price {
                        res = Some(TradeInfo { time: tick_data.t, action: ShClose(i, tick_data.c)});
                        hold.lo -= i;
                    }
                }
                _ => { }
            }
            res
        })
    }
}

#[ta_derive2]
pub struct MatchQueue;

#[typetag::serde]
impl BtMatch for MatchQueue {
    fn bt_match(&self) -> RetFnBtMatch {
        let mut last_order_action = OrderAction::No;
        // let mut counts = 0i32;
        let mut remain = 0f32;
        let mut last_tick_data = TickData::default();
        Box::new(move |stream| {
            use OrderAction::*;
            let tick_data = stream.tick_data;
            let order_action = stream.order_action.clone();
            let mut res = None;
            let hold = stream.hold;
            if last_order_action != order_action {
                match &order_action {
                    LoOpen(_, ref p) | LoClose(_, ref p) => {
                        if *p == last_tick_data.bid1 {
                            remain = last_tick_data.bid1_v;
                        }
                    }
                    ShOpen(_, ref p) | ShClose(_, ref p) => {
                        if *p == last_tick_data.ask1 {
                            remain = last_tick_data.ask1_v;
                        }
                    }
                    _ => {}
                }
            }
            match order_action.clone() {
                LoOpen(i, p)  => {
                    if tick_data.c == p {
                        if tick_data.v >= remain {
                            res = Some(TradeInfo { time: tick_data.t, action: order_action.clone() });
                            hold.lo += i;
                        } else {
                            remain -= tick_data.v;
                        }
                    } else if tick_data.c < p {
                        res = Some(TradeInfo { time: tick_data.t, action: order_action.clone() });
                        hold.lo +=  i;
                    }
                }
                LoClose(i, p) => {
                    if tick_data.c == p {
                        if tick_data.v >= remain {
                            res = Some(TradeInfo { time: tick_data.t, action: order_action.clone() });
                            hold.lo -= i;
                        } else {
                            remain -= tick_data.v;
                        }
                    } else if tick_data.c < p {
                        res = Some(TradeInfo { time: tick_data.t, action: order_action.clone() });
                        hold.lo -= i;
                    }
                }
                ShOpen(i, p) => {
                    if tick_data.c == p {
                        if tick_data.v >= remain {
                            res = Some(TradeInfo { time: tick_data.t, action: order_action.clone() });
                            hold.sh += i;
                        } else {
                            remain -= tick_data.v;
                        }
                    } else if tick_data.c > p {
                        res = Some(TradeInfo { time: tick_data.t, action: order_action.clone() });
                        hold.sh += i;
                    }
                }
                ShClose(i, p) => {
                    if tick_data.c == p {
                        if tick_data.v >= remain {
                            res = Some(TradeInfo { time: tick_data.t, action: order_action.clone() });
                            hold.sh -= i;
                        } else {
                            remain -= tick_data.v;
                        }
                    } else if tick_data.c > p {
                        res = Some(TradeInfo { time: tick_data.t, action: order_action.clone() });
                        hold.sh -= i;
                    }
                }
                _ => {
                    remain = 0.;
                }
            }
            last_order_action = order_action;
            last_tick_data = tick_data.clone();
            res
        })
    }
}
