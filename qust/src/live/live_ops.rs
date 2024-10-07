use crate::prelude::StreamApiType;
use crate::{ loge, std_prelude::*, trade::prelude::* };
use qust_ds::prelude::*;
use serde::de::DeserializeOwned;
use std::sync::MutexGuard;
use super::prelude::LiveStraPool;
use super::order_types::*;
use std::collections::VecDeque;

#[derive(Default, Debug)]
pub struct NotifyData<T> {
    pub data: Mutex<T>,
    pub started: Mutex<bool>,
    cv: Condvar,
}

impl<T> NotifyData<T> {
    pub fn notify_all(&self) {
        self.cv.notify_all();
    }

    pub fn wait_or_exit(&self, info: &str) -> (MutexGuard<T>, bool) {
        let guard = self.cv.wait(self.data.lock().unwrap()).unwrap();
        let is_started = *self.started.lock().unwrap();
        if !is_started {
            loge!("spy", "{info}");
        }
        (guard, is_started)
    }

    pub fn start(&self) {
        *self.started.lock().unwrap() = true;
    }

    pub fn stop(&self) {
        *self.started.lock().unwrap() = false;
        self.notify_all();
    }
    
    pub fn set(&self, data: T) {
        *self.data.lock().unwrap() = data;
    }
}

impl<T> NotifyData<VecDeque<T>> {
    fn wait_or_exit_vec(&self, info: &str) -> (MutexGuard<VecDeque<T>>, bool) {
        let mut guard = self.data.lock().unwrap();
        while guard.is_empty() {
            guard = self.cv.wait(guard).unwrap();
        }
        let is_started = *self.started.lock().unwrap();
        if !is_started {
            loge!("spy", "{info}");
        }
        (guard, is_started)
    }
    
    pub fn push(&self, data: T) {
        self.data.lock().unwrap().push_back(data);
    }
}


#[derive(Clone, Debug)]
pub enum DataReceive {
    TickData(TickData),
    OrderReceive(OrderReceive),
}

impl From<TickData> for DataReceive {
    fn from(value: TickData) -> Self {
        Self::TickData(value)
    }
}
impl From<OrderReceive> for DataReceive {
    fn from(value: OrderReceive) -> Self {
        Self::OrderReceive(value)
    }
}

pub type DataSendOn = Arc<NotifyData<OrderSend>>;
pub type DataReceiveOn = Arc<NotifyData<VecDeque<DataReceive>>>;

#[derive(Debug)]
pub struct TradeApi {
    pub contract: &'static str,
    pub ticker: Ticker,
    pub data_send: DataSendOn,
    pub data_receive: DataReceiveOn,
}

#[derive(Default)]
pub struct UpdateDi {
    pub live_api: LiveStraPool,
    ticker_contract_map: hm<Ticker, &'static str>,
    ticker_order_pool_map: hm<Ticker, Arc<Mutex<OrderPool>>>,
    pub ticker_record: hm<Ticker, Mutex<Vec<TickData>>>,
}

impl UpdateDi {
    pub fn new(live_api: LiveStraPool, ticker_contract_map: hm<Ticker, &'static str>) -> Self {
        let mut res = Self {
            live_api,
            ..Default::default()
        };
        res.merge_ticker_contract_map(ticker_contract_map);
        res
    }

    pub fn get_ticker_string_vec(&self) -> Vec<String> {
        self
            .ticker_contract_map
            .keys()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
    }
    
    pub fn merge_ticker_contract_map(&mut self, ticker_contract_map: hm<Ticker, &'static str>) {
        self.ticker_contract_map.clear();
        let (ticker_contract_map, ticker_order_pool_map, ticker_record) = self
            .live_api
            .data
            .iter()
            .fold((hm::new(), hm::new(), hm::new()), |mut accu, live_api_ticker| {
                let ticker = live_api_ticker.ticker;
                match ticker_contract_map.get(&ticker) {
                    Some(&contract) => {
                        let order_pool = OrderPool {
                            ticker,
                            hold: Default::default(),
                            pool: Default::default(),
                        };
                        let order_pool = Arc::new(Mutex::new(order_pool));
                        // accu.0.insert(contract, ticker);
                        accu.0.insert(ticker, contract);
                        accu.1.insert(ticker, order_pool);
                        accu.2.insert(ticker, Default::default());
                    }
                    None => {
                        loge!("stra", "ticker cannot find mapping contract");
                    }
                }
                accu
            });
        self.ticker_contract_map = ticker_contract_map;
        self.ticker_order_pool_map = ticker_order_pool_map;
        self.ticker_record = ticker_record;
    }

    fn start_spy_on_data_receive(&self, trade_api: Arc<TradeApi>) -> Option<()> {
        trade_api.data_receive.start();
        let order_pool_arc = self.ticker_order_pool_map.get(&trade_api.ticker)?.clone();
        let mut order_pool = order_pool_arc.lock().unwrap();
        let live_api_ticker = self.live_api.data.iter().find(|x| x.ticker == trade_api.ticker)?;
        let mut live_api_ops = live_api_ticker.data.api_type();
        let mut last_tick_data = TickData::default();
        loge!("spy", "stra start to send data: {:?}", trade_api.ticker);
        loop {
            let (mut guard, is_started) = trade_api
                .data_receive
                .wait_or_exit_vec(&format!("stra stop to receive data: {:?}", trade_api.ticker));
            if !is_started {
                break;
            }
            let mut data_receive_vec = VecDeque::default();
            data_receive_vec.append(&mut guard);
            drop(guard);
            loge!(trade_api.ticker, "data receive: cumlative len: {}", data_receive_vec.len());
            while let Some(data_receive) = data_receive_vec.pop_front() {
                match data_receive {
                    DataReceive::TickData(tick_data) => {
                        loge!(trade_api.ticker, "data recive ---------- tick data --------------");
                        self.ticker_record[&trade_api.ticker].lock().unwrap().push(tick_data.clone());
                        last_tick_data = tick_data;
                        let stream_api = StreamApiType { tick_data: &last_tick_data, hold: &order_pool.hold };
                        live_api_ops(stream_api);
                        loge!(trade_api.ticker, "data recive ++++++++++ tick data ++++++++++++++");
                    }
                    DataReceive::OrderReceive(data_receive) => {
                        loge!(trade_api.ticker, "data recive ---------- data receive --------------");
                        if let Err(e) = order_pool.update_order(data_receive) {
                            loge!(trade_api.ticker, "update err {:?}", e);
                        }
                        loge!(trade_api.ticker, "data recive ++++++++++ data receive ++++++++++++++");
                    } 
                }
                if data_receive_vec.is_empty() {
                    loge!(trade_api.ticker, "data receive ----------: {:?}", &order_pool.hold);
                    let stream_api = StreamApiType { tick_data: &last_tick_data, hold: &order_pool.hold };
                    let order_action = live_api_ops(stream_api);
                    loge!(trade_api.ticker, "stra calced a order_action: {:?}", order_action);
                    match order_pool.process_order_action(order_action) {
                        Ok(Some(order_input)) => {
                            loge!(trade_api.ticker, "data receive +++++++ stra send a order to ctp: {:?}", order_input);
                            trade_api.data_send.set(order_input);
                            trade_api.data_send.notify_all();
                        }
                        Ok(None) => {
                            loge!(trade_api.ticker, "data receive +++++++ stra order pool calc a none order send");
                        }
                        Err(e) => {
                            loge!(trade_api.ticker, "data receive +++++++ order output error: {:?}", e);
                        }
                    }
                }
            }
        }
        Some(())
    }
}

pub struct StraApi {
    pub update_di: Arc<UpdateDi>,
}

impl StraApi {
    pub fn new(live_api: LiveStraPool, ticker_contract_map: hm<Ticker, &'static str>) -> Self {
        let update_di = UpdateDi::new(live_api, ticker_contract_map).pip(Arc::new);
        StraApi { update_di }
    }

    pub fn load_from_update_di_path<T>(p: impl AsRef<Path>) -> Self
    where
        T: DeserializeOwned + Into<LiveStraPool>,
    {
        let p_path = p.as_ref();
        let file_name = p_path.file_name().unwrap();
        let dir_name = p_path.parent().unwrap();
        let stra_api = T::rof(file_name.to_str().unwrap(), dir_name.as_os_str().to_str().unwrap());
        let update_di = UpdateDi::new(stra_api.into(), Default::default());
        Self { update_di: Arc::new(update_di) }
    }

    pub fn get_trade_api_vec1(&self) -> Vec<Arc<TradeApi>> {
        self.update_di
            .ticker_contract_map
            .iter()
            .map(|(ticker, contract)| {
                TradeApi {
                    contract,
                    ticker: *ticker,
                    data_send: Default::default(),
                    data_receive: Default::default(),
                }
                .pip(Arc::new)
            })
            .collect_vec()
    }

    pub fn start_spy_on_data_send(&self, trade_api: Arc<TradeApi>) {
        trade_api.data_send.start();
    }

    pub fn start_spy_on_data_receive(&self, trade_api: Arc<TradeApi>) {
        let update_di = Arc::clone(&self.update_di);
        thread::spawn(move || {
            update_di.start_spy_on_data_receive(trade_api);
        });
    }
}
