#![allow(unused_imports)]
use serde::{ Serialize, Deserialize };
use qust_ds::prelude::*;
use qust_derive::*;
use crate::loge;
use crate::prelude::{PconIdent, Ticker};
use crate::sig::prelude::{NormHold, ToNum};
use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU64, Ordering};

fn generate_order_ref() -> String {
    uuid::Uuid::new_v4().to_string().chars().take(16).collect()
}


#[derive(Clone, Debug, thiserror::Error)]
pub enum OrderError {
    #[error("order error: {0}")]
    Message(String),
    #[error("order not found by order_ref: {0}")]
    OrderNotFound(String),
    #[error("di not found {0:?}")]
    DiNotFound(PconIdent),
    #[error("order logic error: {0}")]
    Logic(String)
}

pub type OrderResult<T> = Result<T, OrderError>;

#[ta_derive]
#[derive(Default, PartialEq)]
pub enum OrderAction {
    LoOpen(i32, f32),
    LoClose(i32, f32),
    LoCloseYd(i32, f32),
    ShOpen(i32, f32),
    ShClose(i32, f32),
    ShCloseYd(i32, f32),
    #[default]
    No,
}


impl From<NormHold> for LiveTarget {
    fn from(value: NormHold) -> Self {
        match value {
            NormHold::No => Self::No,
            NormHold::Lo(i) => Self::Lo(i),
            NormHold::Sh(i) => Self::Sh(i),
        }
    }
}

#[derive(Default, Debug, Clone)]
pub enum LiveTarget {
    #[default]
    No,
    Lo(f32),
    Sh(f32),
    OrderAction(OrderAction),
}

impl LiveTarget {
    pub fn add_live_target(&self, other: &Self) -> Self {
        use LiveTarget::*;
        match (self, other) {
            (No, other) => other.clone(),
            (other, No) => other.clone(),
            (Lo(n1), Lo(n2)) => Lo(n1 + n2),
            (Sh(n1), Sh(n2)) => Sh(n1 + n2),
            (Lo(n1), Sh(n2)) => {
                if n1 > n2 {
                    Lo(n1 - n2)
                } else if n1 < n2 {
                    Sh(n2 - n1)
                } else {
                    No
                }
            }
            (Sh(n1), Lo(n2)) => {
                if n1 > n2 {
                    Sh(n1 - n2)
                } else if n1 < n2 {
                    Lo(n1 - n2)
                } else {
                    No
                }
            }
            _ => panic!("cannot add: {:?} {:?}", self, other),
        }
    }
}

impl ToNum for LiveTarget {
    fn to_num(&self) -> f32 {
        use LiveTarget::*;
        match self {
            Lo(i) => *i,
            Sh(i) => -i,
            No => 0.,
            _ => panic!("cannot convert to num for: {:?}", self),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct HoldLocal {
    pub yd_sh: i32,
    pub yd_lo: i32,
    pub td_sh: i32,
    pub td_lo: i32,
    pub exit_sh: i32,
    pub exit_lo: i32,
}

impl HoldLocal {
    pub fn sum(&self) -> i32 {
        self.yd_lo + self.td_lo - self.yd_sh - self.td_sh
    }

    pub fn sum_pending(&self) -> i32 {
        self.sum() + self.exit_lo - self.exit_sh
    }
}



#[derive(Clone, Debug, Default)]
pub enum OrderStatus {
    #[default]
    SubmittingToApi,
    AllTraded,
    PartTradedQueueing(i32),
    Canceled(i32),
    NotTouched,
    Unknown(char),
    Inserted,
    InsertError(i32),
}

#[derive(Clone, Debug, Default)]
pub struct OrderSend {
    pub id: String,
    pub order_action: OrderAction,
    pub order_status: OrderStatus,
    pub is_to_cancel: bool,
    pub create_time: dt,
    pub update_time: dt,
    pub order_ref: Option<[i8; 13]>,
    pub front_id: Option<i32>,
    pub session_id: Option<i32>,
    pub exchange_id: Option<[i8; 9]>,
}

#[derive(Clone, Debug, Default)]
pub struct OrderReceive {
    pub id: String,
    pub order_status: OrderStatus,
    pub update_time: dt,
    pub order_ref: Option<[i8; 13]>,
    pub front_id: Option<i32>,
    pub session_id: Option<i32>,
    pub exchange_id: Option<[i8; 9]>,
}


#[derive(Debug)]
pub struct OrderPool {
    pub ticker: Ticker,
    pub hold: HoldLocal,
    pub pool: hm<String, OrderSend>,
}

impl OrderPool {
    pub fn create_order(&mut self, order_action: OrderAction) -> OrderSend {
        // let order_ref: String = uuid::Uuid::new_v4().to_string().chars().take(12).collect();
        let order_id = generate_order_ref();
        let new_order = OrderSend {
            id: order_id.clone(),
            order_action,
            order_status: OrderStatus::SubmittingToApi,
            create_time: chrono::Local::now().naive_local(),
            update_time: chrono::Local::now().naive_local(),
            is_to_cancel: false,
            order_ref: None,
            front_id: None,
            session_id: None,
            exchange_id: None,
        };
        loge!(self.ticker, "order pool create a order: {:?}", new_order);
        self.pool.insert(order_id, new_order.clone());
        new_order
    }

    pub fn cancel_order(&mut self, order_ref: &str) -> OrderResult<Option<OrderSend>> {
        let order = self.pool
            .get_mut(order_ref)
            .ok_or(OrderError::OrderNotFound(order_ref.to_string()))?;
        match order.is_to_cancel {
            true => {
                loge!(self.ticker, "cancel: canceling");
                Ok(None)
            }
            false => {
                loge!(self.ticker, "cancel: not canceling");
                order.is_to_cancel = true;
                Ok(Some(order.clone()))
            }
        }
    }

    fn delete_order(&mut self, order_ref: &str) -> OrderResult<OrderSend> {
        self.pool
            .remove(order_ref)
            .ok_or(OrderError::OrderNotFound(order_ref.to_string()))
    }

    fn finished_order_update(&mut self, order_ref: &str, c: Option<i32>) -> OrderResult<bool> {
        let order_action = self
            .pool
            .get(order_ref)
            .ok_or(OrderError::OrderNotFound(order_ref.to_string()))?;
        match &order_action.order_action {
            OrderAction::LoOpen(i, _) => {
                self.hold.td_lo += c.unwrap_or(*i);
            }
            OrderAction::ShOpen(i, _) => {
                self.hold.td_sh += c.unwrap_or(*i);
            }
            OrderAction::LoClose(i, _) => {
                self.hold.td_sh -= c.unwrap_or(*i);
            }
            OrderAction::ShClose(i, _) => {
                self.hold.td_lo -= c.unwrap_or(*i);
            }
            other => {
                return Err(OrderError::Logic(format!("order action on what? {:?} {:?}", other, line!())));
            }
        }
        self.delete_order(order_ref)?;
        Ok(true)
    }

    pub fn update_order(&mut self, order: OrderReceive) -> OrderResult<bool> {
        loge!(self.ticker, "order pool get a order rtn from ctp: {:?}", order);
        loge!(self.ticker, "order pool: {:?}", self.pool.iter().map(|x| x.0.to_string()).collect_vec());
        let order_local = self
            .pool
            .get_mut(&order.id)
            .ok_or(OrderError::OrderNotFound(order.id.clone()))?;
        order_local.order_ref = order.order_ref;
        order_local.front_id = order.front_id;
        order_local.session_id = order.session_id;
        order_local.exchange_id = order.exchange_id;
        order_local.update_time = order.update_time;
        order_local.order_status = order.order_status.clone();
        let is_changed = match order.order_status {
            OrderStatus::AllTraded => {
                loge!(self.ticker, "order pool order update finished");
                self.finished_order_update(&order.id,  None)?
            }
            OrderStatus::Canceled(i) => {
                loge!(self.ticker, "order pool order update canceled");
                self.finished_order_update(&order.id, Some(i))?
            }
            OrderStatus::InsertError(_i) => {
                loge!(self.ticker, "order pool order update insert error");
                self.delete_order(&order.id)?;
                false
            }
            _ => { false }
        };
        Ok(is_changed)
    }

    fn is_need_to_wait(&self) -> bool {
        let mut res = false;
        for order_input in self.pool.values() {
            if let OrderStatus::NotTouched | OrderStatus::Unknown(_) | OrderStatus::SubmittingToApi = order_input.order_status {
                res = true;
                break;
            }
        }
        res
    }

    fn get_to_cancel_order(&self, order_action: &OrderAction) -> CancelRes {
        use OrderStatus::*;
        if let OrderAction::No = order_action {
            if !self.pool.is_empty() {
                return CancelRes::CancelAll;
            } else {
                return CancelRes::DoNothing;
            }
        }
        for order_input in self.pool.values() {
            if let  PartTradedQueueing(_) = order_input.order_status {
                if &order_input.order_action != order_action {
                    return CancelRes::HaveDiffOrder(order_input.id.clone());
                } else {
                    return CancelRes::HaveTheSameOrder;
                }
            }
        }
        CancelRes::NotHave
    }

    pub fn process_order_action(&mut self, order_action: OrderAction) -> OrderResult<Option<OrderSend>> {
        if self.is_need_to_wait() {
            std::thread::sleep(std::time::Duration::from_millis(10));
            loge!(self.ticker, "order pool said: need to wait");
            return Ok(None);
        }
        match self.get_to_cancel_order(&order_action) {
            CancelRes::HaveTheSameOrder => {
                loge!(self.ticker, "order pool have the same order");
                Ok(None)
            }
            CancelRes::HaveDiffOrder(order_ref) => {
                let order_res = self.cancel_order(&order_ref)?;
                loge!(self.ticker, "order pool need to cacel this order: {:?}", order_res);
                Ok(order_res)
            }
            CancelRes::NotHave => {
                let order_res = self.create_order(order_action);
                loge!(self.ticker, "order pool need to create this order: {:?}", order_res);
                Ok(Some(order_res))
            }
            CancelRes::CancelAll => {
                loge!(self.ticker, "order pool cancel all orders: {:?}", order_action);
                match self.pool.keys().take(1).next().cloned() {
                    Some(order_id) => self.cancel_order(&order_id),
                    None => Ok(None),
                }
            }
            CancelRes::DoNothing => {
                loge!(self.ticker, "order pool do nothing: {:?}", order_action);
                Ok(None)
            }
        }
    }
}


enum CancelRes {
    HaveTheSameOrder,
    HaveDiffOrder(String),
    NotHave,
    CancelAll,
    DoNothing,
}