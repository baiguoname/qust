use super::super::bt::*;
use qust_ds::prelude::*;
use crate::loge;
use crate::trade::inter::*;
use super::super::order_types::*;
use super::super::live_ops::*;
use super::super::live_run::*;
use std::sync::Mutex;
use crate::trade::ticker::*;
use std::collections::VecDeque;




pub struct TradeOne<T> {
    pub stra: T,
    pub trade_api: TradeApi,
    pub trade_manager: TradeManager,
}

impl<T: std::fmt::Debug> TradeOne<T> {
    pub fn new(stra: T, ticker: Ticker, contract_hm: &hm<Ticker, sstr>) -> Self {
        let contract = contract_hm[&ticker];
        let data_recv_id = DataRecvId {
            tick_data_id: contract.to_string(),
            order_return_id: (&stra, ticker).gen_unique_id(ORDER_RET_ID_LEN),
        };
        let trade_api = TradeApi {
            contract,
            ticker,
            data_recv_id,
            data_send: Default::default(),
            data_recv: Default::default(),
        };
        let order_pool = Mutex::new(OrderPool { 
            contract, 
            hold: Default::default(), 
            pool: Default::default(),
            pool_id: trade_api.data_recv_id.order_return_id.clone(),
        });
        let trade_manager = TradeManager {
            contract,
            order_pool,
            hold: Default::default(),
        };
        Self { stra, trade_api, trade_manager }
    }
}

impl<T: ApiType> ApiBridge for TradeOne<T> {

    fn gen_trade_api(&self) -> Vec<TradeApi> {
        vec![self.trade_api.clone()]
        
    }

    fn data_recv_get(&self) -> NotifyDataRecv {
        self.trade_api.data_recv.clone()
    }

    fn handle_notify<'a>(&'a self) -> Box<dyn FnMut(VecDeque<DataRecv>) + 'a> {
        let mut order_pool = self.trade_manager.order_pool.lock().unwrap();
        let mut live_api_ops = self.stra.api_type();
        let mut last_tick_data = TickData::default();
        let ticker = self.trade_api.ticker;
        Box::new(move |data_recv_que| {
            let mut data_recv_que = data_recv_que;
            while let Some(data_receive) = data_recv_que.pop_front() {
                match data_receive {
                    DataRecv::TickData(_, tick_data) => {
                        loge!(ticker, "data recive ---------- tick data --------------");
                        last_tick_data = tick_data;
                        let stream_api = StreamApiType { tick_data: &last_tick_data, hold: &order_pool.hold };
                        live_api_ops(stream_api);
                        loge!(ticker, "data recive ++++++++++ tick data ++++++++++++++");
                    }
                    DataRecv::OrderRecv(data_receive) => {
                        loge!(ticker, "data recive ---------- data receive --------------");
                        if let Err(e) = order_pool.update_order(data_receive) {
                            loge!(ticker, "update err {:?}", e);
                        }
                        loge!(ticker, "data recive ++++++++++ data receive ++++++++++++++");
                    } 
                    DataRecv::OrderRecvHis(order_recv_vec) => {
                        order_pool.update_order_his(order_recv_vec);
                        continue;
                    }
                }
                if data_recv_que.is_empty() {
                    loge!(ticker, "data receive ----------: {:?}", &order_pool.hold);
                    let stream_api = StreamApiType { tick_data: &last_tick_data, hold: &order_pool.hold };
                    let order_action = live_api_ops(stream_api);
                    loge!(ticker, "stra calced a order_action: {:?}", order_action);
                    match order_pool.process_order_action(order_action) {
                        Ok(Some(order_input)) => {
                            loge!(ticker, "data receive +++++++ stra send a order to ctp: {:?}", order_input);
                            let mut order_input_que = VecDeque::new();
                            order_input_que.push_back(order_input);
                            self.trade_api.data_send.set(order_input_que);
                            self.trade_api.data_send.notify_all();
                        }
                        Ok(None) => {
                            loge!(ticker, "data receive +++++++ stra order pool calc a none order send");
                        }
                        Err(e) => {
                            loge!(ticker, "data receive +++++++ order output error: {:?}", e);
                        }
                    }
                }
            }
        })
    }
}