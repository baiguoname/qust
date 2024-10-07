#![allow(unused_variables)]
// use qust::loge;
use qust:: { prelude::*, std_prelude::* };
use ctp_futures::md_api::MdApi;
use ctp_futures::trader_api::TraderApi;
use ctp_futures::{ md_api, trader_api as td_api};
use futures::{StreamExt, executor::block_on};
use super::config::CtpAccountConfig;
use super::utiles::*;
use super::api::{ApiConvert, CtpOrderAction, CtpQueryRes, OrderSendWithAcco};
use super::type_bridge::*;
use std::ffi::CStr;
use std::path::PathBuf;
use std::{ sync::{ Arc, Mutex }, ffi::CString };
use anyhow::Result;

#[derive(Debug)]
pub enum CtpError {
    LoginError(i32),
}

pub trait CError {
    type Output;
    fn c_error(self) -> Self::Output;
}

impl CError for i32 {
    type Output = Result<(), CtpError>;
    fn c_error(self) -> Self::Output {
        match self {
            0 => Ok(()),
            other => Err(CtpError::LoginError(other)),
        }
    }
}

pub struct Ctp {
    pub ca: CtpAccountConfig,
    md_rid: Mutex<RequestId>,
    td_rid: Mutex<RequestId>,
    pub md: Mutex<MdApi>,
    td: Mutex<TraderApi>,
    query_res: CtpQueryRes,
    td_req_interval: Mutex<Instant>,
    need_reconnect_md: Mutex<bool>,
    need_reconnect_td: Mutex<bool>,
}

impl Ctp {
    pub fn new(flow_path: &str, config: &CtpAccountConfig, query_res: CtpQueryRes) -> Self {
        let flow_path_dir = PathBuf::from(flow_path);
        if !flow_path_dir.exists() {
            flow_path_dir.build_an_empty_dir();
        }
        let flow_path_save_data = flow_path_dir.join("save_data");
        flow_path_save_data.build_an_empty_dir();
        flow_path_dir.join("sig_log").build_an_empty_dir();
        let flow_path_str = flow_path_save_data.as_os_str().to_str().unwrap();
        Ctp {
            ca: config.clone(),
            md_rid: Mutex::new(RequestId(0)),
            td_rid: Mutex::new(RequestId(0)),
            md: Mutex::new(md_api::create_api(flow_path_str, false, false)),
            td: Mutex::new(td_api::create_api(flow_path_str, false)),
            query_res,
            td_req_interval: Mutex::new(Instant::now()),
            need_reconnect_md: Mutex::new(false),
            need_reconnect_td: Mutex::new(false),
        }
    }

    pub fn get_api_version(&self) -> Result<String> {
        let res = ctp_futures::trader_api::get_api_version();
        let c_str = unsafe { CStr::from_ptr(res) };
        let res = c_str.to_str()?;
        Ok(res.into())
    }

    fn md_accu(&self) -> i32 {
        let a = &mut self.md_rid.lock().unwrap().0;
        *a += 1;
        *a
    }

    fn td_accu(&self) -> i32 {
        let a = &mut self.td_rid.lock().unwrap().0;
        *a += 1;
        *a
    }

    fn req_order(&self, req: &mut CtpOrderAction) -> i32 {
        match req {
            CtpOrderAction::InsertOrder(input_order_field) => {
                self.td.lock().unwrap().req_order_insert(input_order_field, self.td_accu())
            }
            CtpOrderAction::CancelOrder(action_order_field) => {
                self.td.lock().unwrap().req_order_action(action_order_field, self.td_accu())
            }
        }
    }

    fn req_update_trading_account(&self) -> i32 {
        let mut req = QryTradingAccountField::default();
        set_cstr_from_str_truncate_i8(&mut req.BrokerID, self.ca.broker_id.as_str());
        set_cstr_from_str_truncate_i8(&mut req.InvestorID, self.ca.account.as_str());
        self.td.lock().unwrap().req_qry_trading_account(&mut req, self.td_accu())
    }

    fn update_trading_account(&self, data: TradingAccountField) {
        *self.query_res.trading_account.write().unwrap() = data;
    }

    fn _req_update_qry_instrument(&self, instrumentid: IstmId) -> i32 {
        let mut req = QryInstrumentField {
            InstrumentID: instrumentid,
            ..QryInstrumentField::default()
        };
        self.td.lock().unwrap().req_qry_instrument(&mut req, self.td_accu())
    }

    fn update_qry_instrument(&self, data: InstrumentField) {
        self.query_res.instrument_info.write().unwrap().insert(data.InstrumentID, data);
    }
    

    fn req_update_positions(&self) -> i32 {
        let mut req = QryInvestorPositionField::default();
        set_cstr_from_str_truncate_i8(&mut req.BrokerID, self.ca.broker_id.as_str());
        set_cstr_from_str_truncate_i8(&mut req.InvestorID, self.ca.account.as_str());
        // let mut res = 0;
        match self.td_req_interval.try_lock() {
            Ok(ref mut t_last) => {
                let t_elapsed = t_last.elapsed().as_millis() as u64;
                if t_elapsed < 1000 {
                    sleep2millis(1000 - t_elapsed);
                    **t_last = Instant::now();
                }
                self.td.lock().unwrap().req_qry_investor_position(&mut req, self.td_accu())
            }
            Err(_) => {
                0
            }
        }
        // res
    }

    fn _req_update_positions_istmid(&self, instrumentid: IstmId) -> i32 {
        let mut req = QryInvestorPositionField::default();
        set_cstr_from_str_truncate_i8(&mut req.BrokerID, self.ca.broker_id.as_str());
        set_cstr_from_str_truncate_i8(&mut req.InvestorID, self.ca.account.as_str());
        req.InstrumentID = instrumentid;
        self.td.lock().unwrap().req_qry_investor_position(&mut req, self.td_accu())
    }

    pub fn release(&self) {
        self.md.lock().unwrap().release();
        self.td.lock().unwrap().release();
    }

    pub fn login_md(&self) -> i32 {
        *self.need_reconnect_md.lock().unwrap() = true;
        let mut req = ReqUserLoginField::default();
        set_cstr_from_str_truncate_i8(&mut req.BrokerID, self.ca.broker_id.as_str());
        set_cstr_from_str_truncate_i8(&mut req.UserID, self.ca.account.as_str());
        set_cstr_from_str_truncate_i8(&mut req.Password, self.ca.password.as_str());
        let res = self.md.lock().unwrap().req_user_login(&mut req, self.md_accu());
        res
    }

    pub fn login_td(&self) -> i32 {
        *self.need_reconnect_td.lock().unwrap() = true;
        let mut req = ReqUserLoginField::default();
        set_cstr_from_str_truncate_i8(&mut req.BrokerID, self.ca.broker_id.as_str());
        set_cstr_from_str_truncate_i8(&mut req.UserID, self.ca.account.as_str());
        set_cstr_from_str_truncate_i8(&mut req.Password, self.ca.password.as_str());
        self.td.lock().unwrap().req_user_login(&mut req, self.td_accu())
    }

    fn settlement_info_confirm(&self) -> i32 {
        let mut req = SettlementInfoConfirmField::default();
        set_cstr_from_str_truncate_i8(&mut req.BrokerID, &self.ca.broker_id);
        set_cstr_from_str_truncate_i8(&mut req.InvestorID, &self.ca.account);
        self.td
            .lock()
            .unwrap()
            .req_settlement_info_confirm(&mut req, self.td_accu())
    }

    pub fn logout_md(&self) -> i32 {
        *self.need_reconnect_md.lock().unwrap() = false;
        let mut req = UserLogoutField::default();
        set_cstr_from_str_truncate_i8(&mut req.BrokerID, self.ca.broker_id.as_str());
        set_cstr_from_str_truncate_i8(&mut req.UserID, self.ca.account.as_str());
        self.md.lock().unwrap().req_user_logout(&mut req, self.md_accu())
    }

    pub fn logout_td(&self) -> i32 {
        *self.need_reconnect_td.lock().unwrap() = false;
        let mut req = UserLogoutField::default();
        set_cstr_from_str_truncate_i8(&mut req.BrokerID, self.ca.broker_id.as_str());
        set_cstr_from_str_truncate_i8(&mut req.UserID, self.ca.account.as_str());
        self.td.lock().unwrap().req_user_logout(&mut req, self.td_accu())
    }

    pub fn authenticate(&self) -> i32 {
        let mut req = ReqAuthenticateField::default();
        set_cstr_from_str_truncate_i8(&mut req.BrokerID, self.ca.broker_id.as_str());
        set_cstr_from_str_truncate_i8(&mut req.UserID, self.ca.account.as_str());
        set_cstr_from_str_truncate_i8(&mut req.AuthCode, self.ca.auth_code.as_str());
        set_cstr_from_str_truncate_i8(&mut req.UserProductInfo, self.ca.user_product_info.as_str());
        set_cstr_from_str_truncate_i8(&mut req.AppID, self.ca.app_id.as_str());
        self.td.lock().unwrap().req_authenticate(&mut req, self.td_accu())
    }

    pub fn subscribe_market_data(&self, contracts: Vec<String>) -> i32 {
        let contracts = contracts.into_iter().map(|x| CString::new(x).unwrap()).collect_vec();
        let n = contracts.len() as i32;
        self.md.lock().unwrap().subscribe_market_data(contracts, n)
    }

    pub fn un_subscribe_market_data(&self, contracts: Vec<String>) -> i32 {
        let contracts = contracts.into_iter().map(|x| CString::new(x).unwrap()).collect_vec();
        let n = contracts.len() as i32;
        self.md.lock().unwrap().un_subscribe_market_data(contracts, n)
    }


    pub fn subsecribe_market_data_all(&self) -> i32 {
        let contracts = self
            .query_res
            .contract_ticker_map
            .keys()
            .map(|x| {
                x.to_str_0()
            })
            .collect::<Vec<_>>();
        self.subscribe_market_data(contracts)
    }
    
    pub fn un_subseribe_market_data_all(&self) -> i32 {
         let contracts = self
            .query_res
            .contract_ticker_map
            .keys()
            .map(|x| {
                x.to_str_0()
            })
            .collect::<Vec<_>>();
        self.un_subscribe_market_data(contracts)      
    }

    pub async fn start_md(&self) {
        let mut stream = {
            let (stream, pp) = md_api::create_spi();
            self.md.lock().unwrap().register_spi(pp);
            stream
        };
        self.md.lock().unwrap().register_front(CString::new(self.ca.md_front.as_str()).unwrap());
        sleep2(1);
        self.md.lock().unwrap().init();
        sleep2(1);
        while let Some(spi_msg) = stream.next().await {
            use ctp_futures::md_api::CThostFtdcMdSpiOutput::*;
            match spi_msg {
                OnFrontConnected(_p) => {
                    loge!("ctp", "md connected");
                }
                OnFrontDisconnected(p) => {
                    loge!("ctp", "md disconnected");
                    if *self.need_reconnect_md.lock().unwrap() {
                        loge!("ctp", "try to reconnect md");
                        while self.login_md() != 0 {
                            loge!("stra", "try to reconnect md...");
                            sleep2(1);
                            continue;
                        }
                        self.subsecribe_market_data_all();
                    } else {
                        loge!("ctp", "md disconnect intentiolly");
                        self.md.lock().unwrap().release();
                        break;
                    }
                }
                OnRspUserLogin(ref p) => {
                    let error_id = p.p_rsp_info.as_ref().unwrap().ErrorID;
                    let error_msg = p.p_rsp_info.as_ref().unwrap().ErrorMsg;
                    loge!("ctp", "md login msg: {}", error_msg.to_str_0());
                    if error_id != 0 {
                        loge!(level: Error, "ctp", "md login wrong");
                        println!("ctp md login wrong: {}", error_msg.to_str_0());
                    } else {
                        loge!("ctp", "md login success");
                        println!("ctp: md login success");
                    }
                }
                OnRspUserLogout(ref p) => {
                    loge!("ctp", "md logout");
                }
                OnRspSubMarketData(ref p) => {
                    let error_id = p.p_rsp_info.as_ref().unwrap().ErrorID;
                    let error_msg = p.p_rsp_info.as_ref().unwrap().ErrorMsg;
                    loge!(
                        "ctp", "subscribe market data res, {}: {}", 
                        p.p_specific_instrument.unwrap().InstrumentID.to_str_0(),error_msg.to_str_0());
                }
                OnRtnDepthMarketData(ref md) => {
                    let market_data: DepthMarketDataField = md.p_depth_market_data.unwrap();
                    self.query_res.send_data_receive(market_data);
                }
                OnRspUnSubMarketData(ref p) => {
                    loge!("ctp", "unsubmarketdata res: {}", p.p_rsp_info.unwrap().ErrorMsg.to_str_0());
                }
                _ => {
                    loge!("ctp", "get an unkown md spi_msg: {:?}", spi_msg);
                }
            }
        }
    }

    pub async fn start_td(&self) {
        let broker_id = self.ca.broker_id.as_str();
        let account = self.ca.account.as_str();
        let trade_front = self.ca.trade_front.as_str();
        let auth_code = self.ca.auth_code.as_str();
        let user_product_info = self.ca.user_product_info.as_str();
        let app_id = self.ca.app_id.as_str();
        let password = self.ca.password.as_str();
        
        let mut stream = {
            let (stream, pp) = td_api::create_spi();
            self.td.lock().unwrap().register_spi(pp);
            stream
        };
        {
            let mut td_api = self.td.lock().unwrap();
            td_api.register_front(CString::new(trade_front).unwrap());
            td_api.subscribe_public_topic(ctp_futures::THOST_TE_RESUME_TYPE_THOST_TERT_QUICK);
            td_api.subscribe_private_topic(ctp_futures::THOST_TE_RESUME_TYPE_THOST_TERT_QUICK);
            td_api.init();
        }
        while let Some(spi_msg) = stream.next().await {
            use ctp_futures::trader_api::CThostFtdcTraderSpiOutput::*;
            match spi_msg {
                OnFrontConnected(_p) => {
                    loge!("ctp", "td connected");
                }
                OnFrontDisconnected(p) => {
                    if *self.need_reconnect_td.lock().unwrap() {
                        loge!("ctp", "td disconnected, try again..");
                        while self.login_td() != 0 {
                            loge!("ctp", "try to reconnect td...");
                            sleep2(1);
                            continue;
                        }
                    } else {
                        loge!("ctp", "td disconnect intentiolly");
                        self.td.lock().unwrap().release();
                        break;
                    }
               }
                OnRspAuthenticate(ref p) => {
                    let error_msg = p.p_rsp_info.as_ref().unwrap().ErrorMsg;
                    let error_id = p.p_rsp_info.as_ref().unwrap().ErrorID;
                    if error_id == 0 {
                        loge!("ctp", "authenticate success");
                    } else {
                        loge!(level: Error, "ctp", "authenticate error id: {:?} error msg: {}, program exit.",
                             error_id, error_msg.to_str_0());
                        std::process::exit(-1);
                    }
                }
                OnRspUserLogin(ref p) => {
                    let error_id = p.p_rsp_info.as_ref().unwrap().ErrorID;
                    let error_msg = p.p_rsp_info.as_ref().unwrap().ErrorMsg;
                    if error_id == 0 {
                        loge!("ctp", "td login success");
                        println!("ctp: td login success");
                        self.settlement_info_confirm();
                    } else {
                        loge!(level: Error, "ctp", "td login failed: {error_id}");
                        println!("ctp td login wrong: {}", error_msg.to_str_0());
                    }
                }
                OnRspUserLogout(ref p) => {
                    loge!("ctp", "td logout");
                }
                OnRspSettlementInfoConfirm(ref _p) => {
                    loge!("ctp", "settlement info confirm");
                    let result = self.req_update_trading_account();
                    sleep2(1);
                    let result = self.req_update_positions();
                }
                OnRspQryTradingAccount(ref p) => {
                    if let Some(taf) = p.p_trading_account {
                        self.update_trading_account(taf);
                    }
                }
                OnRspQryInvestorPositionDetail(ref detail) => {
                    if detail.b_is_last {
                        sleep2(1);
                        let mut req = QryInvestorPositionField::default();
                        set_cstr_from_str_truncate_i8(&mut req.BrokerID, broker_id);
                        set_cstr_from_str_truncate_i8(&mut req.InvestorID, account);
                        let result = self.td
                            .lock()
                            .unwrap()
                            .req_qry_investor_position(&mut req, self.td_accu());
                    }
                }
                OnRspQryInvestorPosition(ref p) => {
                    if let Some(p) = p.p_investor_position {
                        // self.query_res.update_position(p);
                    }
                    if p.b_is_last {
                        sleep2(1);
                    }
                }
                OnRspQryOrder(ref p) => {
                    if p.b_is_last {
                        let mut req = QryTradeField::default();
                        set_cstr_from_str_truncate_i8(&mut req.BrokerID, broker_id);
                        set_cstr_from_str_truncate_i8(&mut req.InvestorID, account);
                        sleep2(1);
                        let result = { self.td.lock().unwrap().req_qry_trade(&mut req, self.td_accu()) };
                        if result != 0 {
                        }
                    }
                }
                OnRspOrderInsert(ref p) => {
                    self.query_res.send_data_receive(p.clone());
                    let g: RspInfoField = p.p_rsp_info.unwrap();
                    if g.ErrorID != 0 {
                        println!(
                            "insert error {:?} {:?} {}", 
                            g.ErrorMsg.to_str_0(),
                            p.p_input_order.unwrap(),
                            p.p_input_order.unwrap().see_string(),
                        );
                        sleep2(1);
                    }
                }
                OnRtnOrder(ref p) => {
                    let p_order: OrderField = p.p_order.unwrap();
                    self.query_res.send_data_receive(p_order);
                }
                OnRspQryInstrument(ref p) => {
                    if p.b_is_last {
                        let res = p.p_instrument.unwrap();
                        self.update_qry_instrument(res);
                    }
                }
                OnRtnTrade(ref _p) => {}
                OnRtnInstrumentStatus(ref p) => {
                }
                OnRspOrderAction(ref _p) => {}
                OnHeartBeatWarning(ref p) => {
                    loge!("ctp", "hear beat warning: {:?}", p);
                }
                _ => {
                    loge!("ctp", "get an unkown td spi_msg: {:?}", spi_msg);
                }
            }
        }
    }

    pub fn start_spy_on_data_send(&self, trade_api: Arc<TradeApi>) -> Option<()> {
        let contract = trade_api.contract;
        let instrumentid = contract.into_istm_id();
        let ticker = self.query_res.contract_ticker_map.get(&instrumentid)?;
        loge!("spy", "ctp start holder nitification: {}", contract);
        loop {
            let (guard, is_started) = trade_api
                .data_send
                .wait_or_exit(&format!("ctp stop holder notification: {}", contract));
            if !is_started { 
                break; 
            }
            let order_send = guard.clone();
            if order_send.id.is_empty() {
                loge!(ticker, "windows wrong: somehow be notified when start tradeapi");
                continue;
            }
            loge!(ticker, "ctp get a order_action_price notify: {:?}", order_send);
            let mut order = OrderSendWithAcco {
                contract: &instrumentid,
                invester_id: &self.ca.account,
                order_input: order_send.clone(),
                broker_id: self.ca.broker_id.as_str(),
                account: self.ca.account.as_str(),
            }.api_convert();
            let req_order_res = self.req_order(&mut order);
            loge!(ticker, "ctp req a order, res: {req_order_res} -- {:?}", order);
        }
        Some(())
    }

}

pub struct CtpApi {
    pub ctp: Arc<Ctp>,
}

impl CtpApi {

    pub fn new(
        account: CtpAccountConfig, 
        trade_api_vec: Vec<Arc<TradeApi>>,
    ) -> Self {
        let (contract_data_receive_map, contract_ticker_map) = trade_api_vec
            .into_iter()
            .fold((hm::new(), hm::new()), |mut accu, trade_api| {
                let istm_id = trade_api.contract.into_istm_id();
                accu.0.insert(istm_id, trade_api.data_receive.clone());
                accu.1.insert(istm_id, trade_api.ticker.into());
                accu
            });
        let query_res = CtpQueryRes {
            contract_data_receive_map,
            contract_ticker_map,
            ..Default::default()
        };
        let ctp = Ctp::new("./data", &account, query_res).pip(Arc::new);
        CtpApi { ctp }
    }

    pub fn init_service(&self) {
        *self.ctp.need_reconnect_md.lock().unwrap() = true;
        *self.ctp.need_reconnect_td.lock().unwrap() = true;
        let api_ref = self.ctp.clone();
        thread::spawn(move || block_on(api_ref.start_md()));
        sleep2(1);
        let api_ref = self.ctp.clone();
        thread::spawn(move || block_on(api_ref.start_td()));
        sleep2(1);
    }

    
    fn login(&self) -> Result<(), CtpError> {
        self.ctp.login_md().c_error()?;
        sleep2(1);
        self.ctp.authenticate().c_error()?;
        sleep2(5);
        self.ctp.login_td().c_error()?;
        sleep2(1);
        Ok(())
    }

    pub fn logout(&self) {
        self.ctp.logout_md();
        sleep2(1);
        self.ctp.logout_td();
        sleep2(1);
    }

    pub fn start_spy_on_data_send(&self, trade_api: Vec<Arc<TradeApi>>) {
        trade_api
            .into_iter()
            .for_each(|x| {
                let ctp = Arc::clone(&self.ctp);
                thread::spawn(move || {
                    ctp.start_spy_on_data_send(x);
                });
            });
     }

    pub fn start_spy_on_data_receive(&self) {
        self.ctp.subsecribe_market_data_all();
    }
}

impl ServiceApi for CtpApi {
    fn start(&self, trade_api: Vec<Arc<TradeApi>>) -> Result<()> {
        loge!("ctp", "api version {}", self.ctp.get_api_version().unwrap());
        self.init_service();
        self.login().map_err(|err| anyhow::anyhow!(format!("{err:?}")))?;
        self.start_spy_on_data_send(trade_api);
        sleep2(1);
        self.start_spy_on_data_receive();
        Ok(())
    }

    fn stop(&self, _trade_api: Vec<Arc<TradeApi>>) -> Result<()> {
        self.ctp.un_subseribe_market_data_all();
        self.logout();
        Ok(())
    }
}


pub async fn run_ctp<T: ServiceApi>(running_api: RunningApi<T, CtpApi>) {
    use super::time_manager::*;

    let mut running_api = running_api;
    let mut time_manager = TimeManager::default();
    let sleep_n = 100;
    for _ in 0..10000 {
        match time_manager.get_state() {
            RunningAction::StartToRun(target_state) => {
                loge!("ctp", "Start running");
                if let Ok(()) = running_api.start() {
                    time_manager.last_running_state = target_state;
                }
                sleep2(sleep_n)
            }
            RunningAction::StopToRun(target_state) => {
                loge!("ctp", "Stop running");
                running_api.stop().unwrap();
                running_api.service_api = CtpApi::new(
                    running_api.service_api.ctp.ca.clone(), 
                    running_api.trade_api.clone()
                );
                time_manager.last_running_state = target_state;
                sleep2(sleep_n)
            }
            RunningAction::Sleep(time_sleep, msg) => {
                loge!("ctp", "{}", msg);
                sleep2(time_sleep);
                // sleep2(sleep_n);
            }
            RunningAction::Impossible => {
                loge!("ctp", "an impossible time service action");
                sleep2(sleep_n);
            }
        }
        // self.save_dil();
    }
}

pub fn running_api_ctp(stra_api: StraApi, account: CtpAccountConfig) -> RunningApi<StraApi, CtpApi> {
    let trade_api_vec = stra_api.get_trade_api_vec1();
    let ctp_api = CtpApi::new(account, trade_api_vec.clone());
    RunningApi {
        stra_api,
        service_api: ctp_api,
        log_path: Some("./logs".into()),
        trade_api: trade_api_vec,
    }
}