use super::type_bridge::*;
use qust_ds::prelude::*;
use qust::prelude::*;
use qust::std_prelude::*;
use ctp_futures::*;
// use super::config::CtpAccountConfig;
use super::utiles::*;
// use CombineOffset::*;
// use Direction::*;
// use CombOffsetFlag::*;
// use PosiDirection::*;


pub trait ApiConvert<T> {
    fn api_convert(self) -> T;
}

pub trait GetInstrumentID {
    fn get_instrument_id(&self) -> [i8; 81];
}

impl ApiConvert<DataReceive> for DepthMarketDataField {
    fn api_convert(self) -> DataReceive {
        TickData {
            t: {
                let c = format!("{} {}.{}", self.TradingDay.to_str_0(), self.UpdateTime.to_str_0(), self.UpdateMillisec);
                dt::parse_from_str(&c, "%Y%m%d %H:%M:%S%.f").expect(&c)
            },
            c     : self.LastPrice as f32,
            v     : self.Volume as f32,
            bid1  : self.BidPrice1 as f32,
            ask1  : self.AskPrice1 as f32,
            bid1_v: self.BidVolume1 as f32,
            ask1_v: self.AskVolume1 as f32,
            ct    : 0,
        }.into()
    }
}

impl GetInstrumentID for DepthMarketDataField {
    fn get_instrument_id(&self) -> [i8; 81] {
        self.InstrumentID
    }
}

pub struct OrderSendWithAcco<'a> {
    pub contract: &'a IstmId,
    pub order_input: OrderSend,
    pub broker_id: &'a str,
    pub invester_id: &'a str,
    pub account: &'a str,
}

#[derive(Debug)]
pub enum CtpOrderAction {
    InsertOrder(InputOrderField),
    CancelOrder(InputOrderActionField),
}

impl ApiConvert<CtpOrderAction> for OrderSendWithAcco<'_> {
    fn api_convert(self) -> CtpOrderAction {
        use OrderAction::*;
        match self.order_input.is_to_cancel {
            false => {
                let mut req = InputOrderField::default();
                set_cstr_from_str_truncate_i8(&mut req.BrokerID, self.broker_id);
                set_cstr_from_str_truncate_i8(&mut req.InvestorID, self.invester_id);
                // set_cstr_from_str_truncate_i8(&mut req.OrderRef, &self.order_input.id);
                set_cstr_from_str_truncate_i8(&mut req.InvestUnitID, &self.order_input.id);
                req.InstrumentID = *self.contract;
                let (dire, action, num, price) = match self.order_input.order_action {
                    No           => (THOST_FTDC_D_Buy as i8, THOST_FTDC_OF_Open as i8, 0, 0.),
                    LoOpen(i, p)    => (THOST_FTDC_D_Buy as i8, THOST_FTDC_OF_Open as i8, i, p as f64),
                    ShOpen(i, p)    => (THOST_FTDC_D_Sell as i8, THOST_FTDC_OF_Open as i8, i, p as f64),
                    LoClose(i, p)   => (THOST_FTDC_D_Buy as i8, THOST_FTDC_OF_CloseToday as i8, i, p as f64),
                    ShClose(i, p)   => (THOST_FTDC_D_Sell as i8, THOST_FTDC_OF_CloseToday as i8, i, p as f64),
                    LoCloseYd(i, p) => (THOST_FTDC_D_Buy as i8, THOST_FTDC_OF_CloseYesterday as i8, i, p as f64),
                    ShCloseYd(i, p) => (THOST_FTDC_D_Sell as i8, THOST_FTDC_OF_CloseYesterday as i8, i, p as f64),
                };
                req.Direction           = dire;
                req.CombOffsetFlag[0]   = action;
                req.VolumeTotalOriginal = num;
                req.OrderPriceType      = THOST_FTDC_OPT_LimitPrice as i8;
                req.LimitPrice          = price;
                req.ContingentCondition = THOST_FTDC_CC_Immediately as i8;
                req.CombHedgeFlag[0]    = THOST_FTDC_HF_Speculation as i8;
                req.TimeCondition       = THOST_FTDC_TC_GFD as i8;
                req.VolumeCondition     = THOST_FTDC_VC_AV as i8;
                req.ForceCloseReason    = THOST_FTDC_FCC_NotForceClose as i8;
                CtpOrderAction::InsertOrder(req)
            }
            true => {
                let mut req = InputOrderActionField::default();
                set_cstr_from_str_truncate_i8(&mut req.BrokerID, self.broker_id);
                set_cstr_from_str_truncate_i8(&mut req.InvestorID, self.invester_id);
                // set_cstr_from_str_truncate_i8(&mut req.OrderRef, &self.order_input.id);
                set_cstr_from_str_truncate_i8(&mut req.InvestUnitID, &self.order_input.id);
                req.InstrumentID = *self.contract;
                req.OrderRef = self.order_input.order_ref.unwrap();
                req.FrontID = self.order_input.front_id.unwrap();
                req.SessionID = self.order_input.session_id.unwrap();
                req.ActionFlag = THOST_FTDC_AF_Delete as i8;
                req.ExchangeID = self.order_input.exchange_id.unwrap();
                CtpOrderAction::CancelOrder(req)
            }
        }
    }
}

impl ApiConvert<DataReceive> for OrderField {
    fn api_convert(self) -> DataReceive {
        let order_status = match self.OrderStatus as u8 as char {
            '0' => OrderStatus::AllTraded,
            '1' | '3' => OrderStatus::PartTradedQueueing(self.VolumeTraded),
            '5' => OrderStatus::Canceled(self.VolumeTraded),
            'a' => OrderStatus::NotTouched,
            other => OrderStatus::Unknown(other),
        };
        OrderReceive {
            // order_ref: gb18030_cstr_to_str_i8(&self.OrderRef).to_string(),
            id: gb18030_cstr_to_str_i8(&self.InvestUnitID).to_string(),
            // order_ref: i8_array_to_string(&self.OrderRef),
            order_status,
            update_time: {
                // let c = format!("{} {}", self.TradingDay.to_str_0(), gb18030_cstr_to_str_i8(&self.UpdateTime));
                // dt::parse_from_str(&c, "%Y%m%d %H:%M:%S").expect(&c)
                Default::default()
            },
            order_ref: Some(self.OrderRef),
            front_id: Some(self.FrontID),
            session_id: Some(self.SessionID),
            exchange_id: Some(self.ExchangeID),
        }.into()
    }
}

impl GetInstrumentID for OrderField {
    fn get_instrument_id(&self) -> [i8; 81] {
        self.InstrumentID
    }
}

impl ApiConvert<DataReceive> for OnRspOrderInsertPacket {
    fn api_convert(self) -> DataReceive {
        let order_input_field = self.p_input_order.unwrap();
        let id = gb18030_cstr_to_str_i8(&order_input_field.OrderRef).to_string();
        let order_status = match self.p_rsp_info.unwrap().ErrorID {
            0 => OrderStatus::Inserted,
            other => OrderStatus::InsertError(other),
        };
        OrderReceive {
            id,
            order_status,
            update_time: Default::default(),
            order_ref: Some(order_input_field.OrderRef),
            front_id: None,
            session_id: None,
            exchange_id: Some(order_input_field.ExchangeID),
        }.into()
    }
}

impl GetInstrumentID for OnRspOrderInsertPacket {
    fn get_instrument_id(&self) -> [i8; 81] {
        self.p_input_order.unwrap().InstrumentID
    }
}

#[derive(Default)]
pub struct CtpQueryRes {
    pub trading_account: RwLock<TradingAccountField>,
    pub instrument_info: RwLock<hm<IstmId, InstrumentField>>,
    pub contract_data_receive_map: hm<IstmId, DataReceiveOn>, 
    pub contract_ticker_map: hm<IstmId, &'static str>,
}

impl CtpQueryRes {
    pub fn send_data_receive<T>(&self, data: T)
    where
        T: GetInstrumentID + ApiConvert<DataReceive>,
    {
        let istm = data.get_instrument_id();
        let ticker = match self.contract_ticker_map.get(&istm) {
            Some(ticker) => ticker,
            None => {
                loge!("ctp", "{:?} not found in ticker_contract_map", istm.to_str_v());
                println!("ctp: {:?} not found in ticker_contract_map", istm.to_str_v());
                return;
            }
        };
        let data_receive = data.api_convert();
        loge!(ticker, "ctp have a  data receive: {:?}", data_receive);
        if let Some(data_receive_on) = self.contract_data_receive_map.get(&istm) {
            data_receive_on.push(data_receive);
            data_receive_on.notify_all();
        }
    }
}