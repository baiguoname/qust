use super::super::bt::*;
use qust_ds::prelude::*;
use crate::loge;
use crate::prelude::StreamBtMatch;
use crate::prelude::WithMatchBox;
use crate::trade::inter::*;
use super::super::order_types::*;
use super::super::live_ops::*;
use super::super::live_run::*;
use std::sync::{ Arc, Mutex };
use crate::trade::ticker::*;
use std::collections::VecDeque;
use super::super::trend::prelude::*;
use super::cond::*;
use super::update_sync::*;



pub struct TradeCross<T> {
    pub stra: T,
    data_recv: NotifyDataRecv,
    pub trade_api: Vec<TradeApi>,
    pub trade_manager: Vec<TradeManager>,
}

impl<T: GetTickerVec + std::fmt::Debug> TradeCross<T> {
    pub fn new(stra: T, tickers: &hm<Ticker, sstr>) -> Self {
        let data_recv = NotifyDataRecv::default();
        let mut trade_api = vec![];
        let mut trade_manager = vec![];
        stra
            .get_ticker_vec()
            .iter()
            .for_each(|&ticker| {
                let contract = tickers[&ticker];
                let data_recv_id = DataRecvId {
                    tick_data_id: contract.to_string(),
                    order_return_id: (&stra, ticker).gen_unique_id(ORDER_RET_ID_LEN),
                };
                let trade_api_part = TradeApi {
                    contract,
                    ticker,
                    data_recv_id,
                    data_send: NotifyDataSend::default(),
                    data_recv: data_recv.clone(),
                };
                let order_pool = OrderPool {
                    contract,
                    hold: Default::default(),
                    pool: Default::default(),
                    pool_id: trade_api_part.data_recv_id.order_return_id.clone(),
                };
                let order_pool = Mutex::new(order_pool);
                let trade_manager_part = TradeManager {
                    contract,
                    order_pool,
                    hold: Mutex::new(Default::default()),
                };
                trade_api.push(trade_api_part);
                trade_manager.push(trade_manager_part);
            });
        Self {
            stra,
            data_recv,
            trade_api,
            trade_manager,
        }

    }
}


impl<T: CondCrossUpdatedDataIndex + Send + Sync> ApiBridge for TradeCross<T> {
    fn gen_trade_api(&self) -> Vec<TradeApi> {
        self.trade_api.clone()
    }

    fn data_recv_get(&self) -> Arc<NotifyDataQue<DataRecv>> {
        self.data_recv.clone()
    }

    fn handle_notify<'a>(&'a self) -> Box<dyn FnMut(VecDeque<DataRecv>) + 'a> {
        let pool_len = self.trade_api.len();
        let mut stra_ops = self.stra.cond_cross_updated_data_index();
        let mut order_pool_vec = self.trade_manager
            .iter()
            .map(|x| x.order_pool.lock().unwrap())
            .collect_vec();
        let contract_vec = self.trade_manager
            .iter()
            .map(|x| x.contract)
            .collect_vec();
        Box::new(move |data_recv_que| {
            let mut data_recv_que = data_recv_que;
            while let Some(data_recv) = data_recv_que.pop_front() {
                let mut i;
                let updated_data_index = match data_recv {
                    DataRecv::TickData(contract, tick_data) => {
                        if tick_data.ask1 == 0. || tick_data.bid1 == 0. {
                            continue;
                        }
                        i = contract_vec.position(&contract);
                        UpdatedDataIndex { index: i, data: UpdatedData::TickData(tick_data) }
                    }
                    DataRecv::OrderRecv(order_recv) => {
                        i = contract_vec.position(&order_recv.contract.as_str());
                        order_pool_vec[i].update_order(order_recv);
                        UpdatedDataIndex { index: i, data: UpdatedData::Hold(order_pool_vec[i].hold.clone())}
                    }
                    DataRecv::OrderRecvHis(order_recv_vec) => {
                        for order_pool in order_pool_vec.iter_mut() {
                            order_pool.update_order_his(order_recv_vec.clone());
                        }
                        continue;
                    }
                };
                let Some(order_action_vec) = stra_ops(updated_data_index) else {
                    return;
                };
                order_action_vec.into_iter().zip(order_pool_vec.iter_mut()).zip(self.trade_api.iter())
                    .for_each(|((order_action, order_pool), trade_api_part)| {
                        let data_send = &trade_api_part.data_send;
                        match order_pool.process_order_action(order_action) {
                            Ok(Some(order_send)) => {
                                data_send.push(order_send);
                                data_send.keep_last();
                                data_send.notify_all();;
                            }
                            Ok(None) => {
                                // println!("update order None");
                            }
                            Err(e) => { 
                                // println!("update order error: {e:?}"); 
                            }
                        }
                    })
            }
        })
    }
}

impl<'a, T> BtTick<Vec<&'a [TickData]>> for WithMatchBox<T> 
where
    T: CondCrossUpdatedDataIndex + GetTickerVec,
{
    type Output = Vec<Vec<TradeInfo>>;
    fn bt_tick(&self, input: Vec<&'a [TickData]>) -> Self::Output {
        let pool_size = self.data.get_ticker_vec().len();
        let mut res = repeat_to_vec(Vec::new, pool_size);
        let mut stra_ops = self.data.cond_cross_updated_data_index();
        let mut tick_data_merged = input.into_iter().enumerate()
            .map(|(i, x)| {
                x.iter().map(|y| (i, y)).collect_vec()
            })
            .collect_vec()
            .concat();
        let mut hold_vec = repeat_to_vec(Hold::default, pool_size);
        let mut order_action_pre = repeat_to_vec(|| OrderAction::No, pool_size);
        let match_pool = repeat_to_vec(|| self.info.clone(), pool_size);
        let mut match_ops_vec = match_pool.iter().map(|x| x.bt_match()).collect_vec();
        tick_data_merged.sort_by(|x, y| x.1.t.cmp(&y.1.t));
        for (i, tick_data) in tick_data_merged.into_iter() {
            if tick_data.ask1 == 0. || tick_data.bid1 == 0. {
                continue;
            }
            let stream_bt_match = StreamBtMatch {
                tick_data,
                hold: &mut hold_vec[i],
                order_action: &order_action_pre[i],
            };
            if let Some(trade_info) = match_ops_vec[i](stream_bt_match) {
                res[i].push(trade_info);
                let updated_data_index = UpdatedDataIndex {
                    index: i,
                    data: UpdatedData::Hold(hold_vec[i].clone()),
                };
                if let Some(order_action_vec) = stra_ops(updated_data_index) {
                    if let OrderAction::No = order_action_vec[0] {} else {
                        loge!("ctp", "{:?}", order_action_vec);
                    }
                    order_action_pre = order_action_vec;
                }
            }
            let updated_data_index = UpdatedDataIndex {
                index:i,
                data: UpdatedData::TickData(tick_data.clone()),
            };
            if let Some(order_action_vec) = stra_ops(updated_data_index) {
                order_action_pre = order_action_vec;
            }
        }
        res
    }
}

impl<'a, 'b, T> BtTick<&'a hm<Ticker, Vec<TickData>>> for T
where
    T: BtTick<Vec<&'b [TickData]>, Output = Vec<Vec<TradeInfo>>>,
    T: GetTickerVec,
    'a: 'b,
{
    type Output = Vec<Vec<TradeInfo>>;
    fn bt_tick(&self, input: &'a hm<Ticker, Vec<TickData>>) -> Self::Output {
        let input_new = self.get_ticker_vec()
            .into_iter()
            .map(|x| input[&x].as_ref())
            .collect_vec();
        self.bt_tick(input_new)
    }
}