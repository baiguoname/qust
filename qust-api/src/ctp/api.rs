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
    fn api_convert(self) -> Option<T>;
}

impl ApiConvert<DataRecv> for (sstr, DepthMarketDataField) {
    fn api_convert(self) -> Option<DataRecv> {
        let contract = self.0;
        let depth_data = self.1;
        let tick_data = TickData {
            t: {
                let c = format!("{} {}.{}", depth_data.TradingDay.to_str_0(), depth_data.UpdateTime.to_str_0(), depth_data.UpdateMillisec);
                dt::parse_from_str(&c, "%Y%m%d %H:%M:%S%.f").expect(&c)
            },
            c     : depth_data.LastPrice as f32,
            v     : depth_data.Volume as f32,
            bid1  : depth_data.BidPrice1 as f32,
            ask1  : depth_data.AskPrice1 as f32,
            bid1_v: depth_data.BidVolume1 as f32,
            ask1_v: depth_data.AskVolume1 as f32,
            ct    : 0,
        };
        Some(DataRecv::TickData(contract, tick_data))
    }
}

impl ApiConvert<DataRecv> for (&CtpQueryRes, DepthMarketDataField) {
    fn api_convert(self) -> Option<DataRecv> {
        let istm = self.1.InstrumentID;
        let contract =  *self.0.contract_ticker_map.get(&istm)?;
        (contract, self.1).api_convert()
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
    fn api_convert(self) -> Option<CtpOrderAction> {
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
                    LoOpen(i, p)    => (THOST_FTDC_D_Buy as i8, THOST_FTDC_OF_Open as i8, i as i32, p as f64),
                    ShOpen(i, p)    => (THOST_FTDC_D_Sell as i8, THOST_FTDC_OF_Open as i8, i as i32, p as f64),
                    LoClose(i, p)   => (THOST_FTDC_D_Buy as i8, THOST_FTDC_OF_CloseToday as i8, i as i32, p as f64),
                    ShClose(i, p)   => (THOST_FTDC_D_Sell as i8, THOST_FTDC_OF_CloseToday as i8, i as i32, p as f64),
                    // LoCloseYd(i, p) => (THOST_FTDC_D_Buy as i8, THOST_FTDC_OF_CloseYesterday as i8, i as i32, p as f64),
                    // ShCloseYd(i, p) => (THOST_FTDC_D_Sell as i8, THOST_FTDC_OF_CloseYesterday as i8, i as i32, p as f64),
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
        }.pip(Some)
    }
}

impl ApiConvert<DataRecv> for OrderField {
    fn api_convert(self) -> Option<DataRecv> {
        let order_status = match self.OrderStatus as u8 as char {
            '0' => {
                let volume_traded = match self.Direction as u8 as char {
                    '0' => self.VolumeTraded as f32,
                    '1' => -(self.VolumeTraded as f32),
                    _ => panic!("dire not implement"),
                };
                OrderStatus::AllTraded(volume_traded)
            }
            '1' | '3' => OrderStatus::PartTradedQueueing(self.VolumeTraded as f32),
            '5' => OrderStatus::Canceled(self.VolumeTraded as f32),
            'a' => OrderStatus::NotTouched,
            other => OrderStatus::Unknown(other),
        };
        OrderRecv {
            // order_ref: gb18030_cstr_to_str_i8(&self.OrderRef).to_string(),
            id: gb18030_cstr_to_str_i8(&self.InvestUnitID).to_string(),
            contract: gb18030_cstr_to_str_i8(&self.InstrumentID).to_string(),
            // order_ref: i8_array_to_string(&self.OrderRef),
            order_status,
            update_time: {
                let c = format!("{} {}", self.TradingDay.to_str_0(), gb18030_cstr_to_str_i8(&self.InsertTime));
                dt::parse_from_str(&c, "%Y%m%d %H:%M:%S").expect(&c)
                // Default::default()
            },
            order_ref: Some(self.OrderRef),
            front_id: Some(self.FrontID),
            session_id: Some(self.SessionID),
            exchange_id: Some(self.ExchangeID),
        }.pip(DataRecv::OrderRecv).pip(Some)
    }
}

impl ApiConvert<DataRecv> for Vec<OrderField> {
    fn api_convert(self) -> Option<DataRecv> {
        let mut res = vec![];
        for k in self.into_iter() {
            let data_recv = k.api_convert()?;
            if let DataRecv::OrderRecv(order_recv) = data_recv {
                res.push(order_recv);
            };
        }
        res.sort_by(|a, b| a.update_time.partial_cmp(&b.update_time).unwrap());
        Some(DataRecv::OrderRecvHis(res))
    }
}

impl ApiConvert<DataRecv> for OnRspOrderInsertPacket {
    fn api_convert(self) -> Option<DataRecv> {
        let order_input_field = self.p_input_order.unwrap();
        let id = gb18030_cstr_to_str_i8(&order_input_field.OrderRef).to_string();
        let order_status = match self.p_rsp_info.unwrap().ErrorID {
            0 => OrderStatus::Inserted,
            other => OrderStatus::InsertError(other),
        };
        OrderRecv {
            id,
            order_status,
            contract: gb18030_cstr_to_str_i8(&order_input_field.InstrumentID).to_string(),
            update_time: Default::default(),
            order_ref: Some(order_input_field.OrderRef),
            front_id: None,
            session_id: None,
            exchange_id: Some(order_input_field.ExchangeID),
        }.pip(DataRecv::OrderRecv).pip(Some)
    }
}

impl<T: ApiConvert<DataRecv>> ApiConvert<DataRecv> for (&CtpQueryRes, T) {
    fn api_convert(self) -> Option<DataRecv> {
        self.1.api_convert()
    }

}

#[derive(Default)]
pub struct CtpQueryRes {
    pub trading_account: RwLock<TradingAccountField>,
    pub instrument_info: RwLock<hm<IstmId, InstrumentField>>,
    pub contract_data_receive_map: hm<DataRecvId, NotifyDataRecv>, 
    pub contract_ticker_map: hm<IstmId, &'static str>,
}

impl CtpQueryRes {

    pub fn send_data_recv<T>(&self, data: T)
    where
         for<'l> (&'l Self, T): ApiConvert<DataRecv>,
    {
        let Some(data_recv) = (self, data).api_convert() else {
            return;
        };
        match &data_recv {
            DataRecv::TickData(c, _) => {
                for (k, data_recv_on) in self.contract_data_receive_map.iter() {
                    if *c == k.tick_data_id {
                        data_recv_on.push(data_recv.clone());
                        data_recv_on.notify_all();
                    }
                }
            }
            DataRecv::OrderRecv(order_recv) => {
                for (k, data_recv_on) in self.contract_data_receive_map.iter() {
                    if order_recv.id.len() < ORDER_RET_ID_LEN {
                        return;
                    }
                    if order_recv.id[..ORDER_RET_ID_LEN] == k.order_return_id {
                        data_recv_on.push(data_recv.clone());
                        data_recv_on.notify_all();
                    }
                }
            }
            DataRecv::OrderRecvHis(_) => {
                for (_, data_recv_on) in self.contract_data_receive_map.iter() {
                    data_recv_on.push(data_recv.clone());
                    data_recv_on.notify_all();
                }
            }
        }

    }
}