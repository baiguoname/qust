use ctp_futures::*;
use super::utiles::*;
use qust::std_prelude::*;
use self::trader_api::CThostFtdcTraderSpiOnRspOrderInsertPacket;
use qust_ds::prelude::*;
use encoding::{ DecoderTrap, Encoding, all::GBK };
use anyhow::{ anyhow, Result };

pub(super) type _MdBox                    = Box<CThostFtdcMdApi>;
pub(super) type _TdBox                    = Box<CThostFtdcTraderApi>;
pub(super) type IstmId                   = TThostFtdcInstrumentIDType;
pub(super) type OrderIdHm                = hm<[i8; 13], CThostFtdcOrderField>;

#[allow(dead_code)]
pub(super) type QryInstrumentField         = CThostFtdcQryInstrumentField;
pub(super) type InstrumentField            = CThostFtdcInstrumentField;
pub(super) type QryTradingAccountField     = CThostFtdcQryTradingAccountField;
pub(super) type InvestorPositionField      = CThostFtdcInvestorPositionField;
pub(super) type TradingAccountField        = CThostFtdcTradingAccountField;
pub(super) type QryInvestorPositionField   = CThostFtdcQryInvestorPositionField;
pub(super) type OrderField                 = CThostFtdcOrderField;
pub(super) type InputOrderField            = CThostFtdcInputOrderField;
pub(super) type InputOrderActionField      = CThostFtdcInputOrderActionField;
pub(super) type DepthMarketDataField       = CThostFtdcDepthMarketDataField;
pub(super) type ReqUserLoginField          = CThostFtdcReqUserLoginField;
pub(super) type UserLogoutField            = CThostFtdcUserLogoutField;
pub(super) type ReqAuthenticateField       = CThostFtdcReqAuthenticateField;
pub(super) type SettlementInfoConfirmField = CThostFtdcSettlementInfoConfirmField;
pub(super) type QryTradeField              = CThostFtdcQryTradeField;
pub(super) type RspInfoField               = CThostFtdcRspInfoField;
pub(super) type OnRspOrderInsertPacket     = CThostFtdcTraderSpiOnRspOrderInsertPacket;


pub trait FromCtpType<T> {
    fn from_ctp_type(value: T) -> Self;
}
pub trait ToCtpType<T> {
    fn to_ctp_type(self) -> T;
}

impl<T, N> ToCtpType<N> for T
where
    N: FromCtpType<T>,
{
    fn to_ctp_type(self) -> N {
        N::from_ctp_type(self)
    }
}


#[derive(Debug)]
pub enum CombineOffset {
    Open,
    Close,
    ForceClose,
    CloseToday,
    CloseYesterday,
    ForceOff,
    LocalForceClose,
}

impl FromCtpType<i8> for CombineOffset {
    fn from_ctp_type(value: i8) -> Self {
        match value {
            48 => Open,
            49 => Close,
            50 => ForceClose,
            51 => CloseToday,
            52 => CloseYesterday,
            53 => ForceOff,
            54 => LocalForceClose,
            _ => panic!("unkown combine_offset"),
        }

    }
}


#[derive(Debug)]
pub enum OrderStatusCtp {
    AllTraded,
    PartTradedQueueing,
    PartTradedNotQueueing,
    NoTradeQueueing,
    NoTradeNotQueueing,
    Canceled,
    Unknown,
    NotTouched,
    Touched,
}
use OrderStatusCtp::*;
impl FromCtpType<i8> for OrderStatusCtp {
    fn from_ctp_type(value: i8) -> Self {
        match value {
            48 => AllTraded,
            49 => PartTradedQueueing,
            50 => PartTradedNotQueueing,
            51 => NoTradeQueueing,
            52 => NoTradeNotQueueing,
            53 => Canceled,
            97 => Unknown,
            98 => NotTouched,
            99 => Touched,
            _ => panic!("unknown order_status"),
        }
    }
}

use CombineOffset::*;

use PosiDirection::*;
#[derive(Debug)]
pub enum PosiDirection {
    PosiLong,
    PosiShort,
}
impl FromCtpType<i8> for PosiDirection {
    fn from_ctp_type(value: i8) -> Self {
        match value {
            50 => PosiLong,
            51 => PosiShort,
            _ => panic!("unkown posi_direction"),
        }
    }
}

use Direction::*;
#[derive(Debug)]
pub enum Direction {
    DireLong,
    DireShort,
}

impl FromCtpType<i8> for Direction {
    fn from_ctp_type(value: i8) -> Self {
        match value {
            48 => DireLong,
            49 => DireShort,
            _ => panic!("unkown direction"),
        }
    }
}

#[derive(Debug)]
pub enum CombOffsetFlag {
    Open,
    Close,
    CloseYd,
}

impl FromCtpType<i8> for CombOffsetFlag {
    fn from_ctp_type(value: i8) -> Self {
        match value {
            48 | 50 => CombOffsetFlag::Open,
            49 | 51 => CombOffsetFlag::Close,
            52 => CombOffsetFlag::CloseYd,
            other => panic!("unkown CombineOffsetFlag, {:?}", other),
        }
    }
}

pub trait IntoIstmId {
    fn into_istm_id(self) -> IstmId;
}
impl IntoIstmId for &str {
    fn into_istm_id(self) -> IstmId {
        // let mut res = IstmId::default();
        let mut res = [0; 81];
        set_cstr_from_str_truncate_i8(&mut res, self);
        res
    }
}


pub trait SeeString {
    fn see_string(&self) -> String;
}

impl SeeString for CThostFtdcInputOrderField {
    fn see_string(&self) -> String {
        format!(
            "investor_unit_id: {} input order: contract => {:<6}   direction => {:<5?}  action => {:<8?} volume => {:<5} limit_price: {:.0}",  
            self.InvestUnitID.to_str_0(),
            self.InstrumentID.to_str_0(),
            Direction::from_ctp_type(self.Direction),
            CombOffsetFlag::from_ctp_type(self.CombOffsetFlag[0]),
            self.VolumeTotalOriginal,
            self.LimitPrice,
        )
    }
}

impl SeeString for CThostFtdcInvestorPositionField {
    fn see_string(&self) -> String {
        format!(
            "Position: contract: {:<6} PositionDate: {} YdPosition: {:<4} AllPosition: {:<4} Position: {:<4} PosiDirection: {:<6?} OpenVolume: {:<4} CloseVolume: {:<4}",
            self.InstrumentID.to_str_0(),
            self.PositionDate,
            self.YdPosition,
            self.Position,
            self.TodayPosition,
            PosiDirection::from_ctp_type(self.PosiDirection),
            self.OpenVolume,
            self.CloseVolume,
        )
    }
}

impl SeeString for CThostFtdcOrderField {
    fn see_string(&self) -> String {
        format!(
            "RntOrder: contract: {:<7} Direction: {:<5?} OrderStatusCtp: {:<20?} VolumeTraded: {:<4} ToTalOrignalVolume: {:<4} LimitPrice: {:.0}",
            self.InstrumentID.to_str_0(),
            Direction::from_ctp_type(self.Direction),
            OrderStatusCtp::from_ctp_type(self.OrderStatus),
            self.VolumeTraded,
            self.VolumeTotalOriginal,
            self.LimitPrice,
        )
    }
}

impl SeeString for OrderIdHm {
    fn see_string(&self) -> String {
        self.values()
            .map(|x| x.see_string() + "\n")
            .collect::<Vec<_>>()
            .concat()
    }
}

pub struct RequestId(pub i32);
impl RequestId {
    pub fn accu(&mut self) -> i32 {
        self.0 += 1;
        self.0
    }
}

pub(super) trait ToStrV {
    fn to_str_v(&self) -> Result<String>;
    fn to_str_0(&self) -> String
    where
        Self: std::fmt::Debug,
    {
        match self.to_str_v() {
            Err(e) => {
                panic!("to_str_0 ---------- {:?}, {:?}", self, e);
            }
            Ok(s) => {
                s.replace('\0', "")
            }
        }
    }
}

impl ToStrV for [i8] {
    fn to_str_v(&self) -> Result<String> {
        let binding = self
            .iter()
            .map(|x| *x as u8)
            // .map(|x| x.unsigned_abs())
            .collect::<Vec<u8>>();
        // let res = std::str::from_utf8(&binding);
        let res = GBK.decode(&binding, DecoderTrap::Strict).map_err(|err| anyhow!(err))?;
        Ok(res)
    }
    fn to_str_0(&self) -> String {
        gb18030_cstr_to_str_i8(self).to_string()
    }
}

pub async fn sleep1(n: u64) {
    tokio::time::sleep(std::time::Duration::from_secs(n)).await;
}

pub fn sleep2(n: u64) {
    std::thread::sleep(std::time::Duration::from_secs(n));
}

pub async fn sleep1millis(n: u64) {
    tokio::time::sleep(std::time::Duration::from_millis(n)).await;
}

pub fn sleep2millis(n: u64) {
    std::thread::sleep(std::time::Duration::from_millis(n));
}