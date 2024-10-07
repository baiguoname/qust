use qust_derive::*;
use serde::{ Serialize, Deserialize };
use super::cond_ops::*;
use super::order_types::*;

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
                    hold.td_lo += i;
                }
            }
            LoClose(i, price) => {
                if tick_data.c <= price {
                    // res = Some(TradeInfo { time: tick_data.t, action: LoClose(i, tick_data.c) });
                    res = Some(TradeInfo { time: tick_data.t, action: LoClose(i, price) });
                    hold.td_sh -= i;
                }
            }
            ShOpen(i, price) => {
                if tick_data.c >= price {
                    // res = Some(TradeInfo { time: tick_data.t, action: ShOpen(i, tick_data.c) });
                    res = Some(TradeInfo { time: tick_data.t, action: ShOpen(i, price) });
                    hold.td_sh += i;
                }
            }
            ShClose(i, price) => {
                if tick_data.c >= price {
                    // res = Some(TradeInfo { time: tick_data.t, action: ShClose(i, tick_data.c) });
                    res = Some(TradeInfo { time: tick_data.t, action: ShClose(i, price) });
                    hold.td_lo -= i;
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
                        hold.td_lo += i;
                    }
                }
                LoClose(i, price) => {
                    if tick_data.ask1 <= price {
                        let match_price = middle_value(price, tick_data.c, tick_data.ask1);
                        res = Some(TradeInfo { time: tick_data.t, action: LoClose(i, match_price)});
                        hold.td_sh -= i;
                    }
                }
                ShOpen(i, price) => {
                    if tick_data.bid1 >= price {
                        let match_price = middle_value(price, tick_data.c, tick_data.bid1);
                        res = Some(TradeInfo { time: tick_data.t, action: ShOpen(i, match_price)});
                        hold.td_sh += i;
                    }
                }
                ShClose(i, price) => {
                    if tick_data.bid1 >= price {
                        let match_price = middle_value(price, tick_data.c, tick_data.bid1);
                        res = Some(TradeInfo { time: tick_data.t, action: ShClose(i, match_price)});
                        hold.td_lo -= i;
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
                    hold.td_lo += i;
                    Some(TradeInfo { time: tick_data.t, action: LoOpen(i, c) })
                }
                LoClose(i, _) => {
                    hold.td_sh -= i;
                    Some(TradeInfo { time: tick_data.t, action: LoClose(i, c) })
                }
                ShOpen(i, _) => {
                    hold.td_sh += i;
                    Some(TradeInfo { time: tick_data.t, action: ShOpen(i, c) })
                }
                ShClose(i, _) => {
                    hold.td_lo -= i;
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
            let p = if c == 0. {
                tick_data.c
            } else {
                (tick_data.c + c) / 2.
            };
            let res = match stream_bt_match.order_action.clone() {
                LoOpen(i, _) => {
                    hold.td_lo += i;
                    Some(TradeInfo { time: tick_data.t, action: LoOpen(i, p) })
                }
                LoClose(i, _) => {
                    hold.td_sh -= i;
                    Some(TradeInfo { time: tick_data.t, action: LoClose(i, p) })
                }
                ShOpen(i, _) => {
                    hold.td_sh += i;
                    Some(TradeInfo { time: tick_data.t, action: ShOpen(i, p) })
                }
                ShClose(i, _) => {
                    hold.td_lo -= i;
                    Some(TradeInfo { time: tick_data.t, action: ShClose(i, p) })
                }
                _ => { None }
            };
            c = tick_data.c;
            res
        })
    }
}