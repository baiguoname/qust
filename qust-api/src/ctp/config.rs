use qust::prelude::*;
use serde::{ Serialize, Deserialize };
use std::collections::HashMap as hm;
use anyhow::Result;


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TickerContractMap(pub hm<Ticker, String>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CtpAccountConfigType<T> {
    pub broker_id: T,
    pub account: T,
    pub name_server: T,
    pub trade_front: T,
    pub md_front: T,
    pub auth_code: T,
    pub user_product_info: T,
    pub app_id: T,
    pub password: T,
}
pub type CtpAccountStr = CtpAccountConfigType<&'static str>;
pub type CtpAccountConfig = CtpAccountConfigType<String>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateDiConfig {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataConfig {
   pub dil_path: String,
   pub stral_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MoneyConfig {
    pub money: f64,
    pub is_need_adjust: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracingConfig {
   pub path: String,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self { path: "./logs".into() }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
   pub ticker_contract_map: Option<TickerContractMap>,
   pub update_di: Option<UpdateDiConfig>,
   pub ctp_account_config: CtpAccountConfig,
   pub data_config: Option<DataConfig>,
   pub money_config: Option<MoneyConfig>,
   pub algo: Option<Box<dyn Algo>>,
   #[serde(default)]
   pub tracing_config: TracingConfig,
}


pub fn get_config() -> Result<Config> {
    use std::env;
    let args = env::args().collect::<Vec<_>>();
    let config_path = if args.len() > 1 {
        &args[1]
    } else {
        "config.toml"
    };
    let to_parsed_string = std::fs::read_to_string(config_path)?;
    let config = toml::from_str(&to_parsed_string)?;
    Ok(config)
}

pub trait ConfigParse {
    type Output;
    fn config_parse(&self) -> Self::Output;
}

impl ConfigParse for [&'static str] {
    type Output = hm<Ticker, &'static str>;
    fn config_parse(&self) -> Self::Output {
        use regex::Regex;
        let re = Regex::new(r"([a-zA-Z]+)").unwrap();
        let mut res = hm::new();
        self.iter()
            .for_each(|&x| {
                match re.captures(x) {
                    Some(ticker_str) => {
                        let ticker = ticker_str[1].into_ticker().unwrap();
                        res.insert(ticker, x);
                    }
                    None => {
                        panic!("{} can be recoginised as a ticker", x);
                    }
                }
            });
        res
            
    }
}

impl ConfigParse for CtpAccountStr {
    type Output = CtpAccountConfig;
    fn config_parse(&self) -> Self::Output {
       Self::Output {
            broker_id: self.broker_id.into(),
            account: self.account.into(),
            name_server: self.name_server.into(),
            trade_front: self.trade_front.into(),
            md_front: self.md_front.into(),
            auth_code: self.auth_code.into(),
            user_product_info: self.user_product_info.into(),
            app_id: self.app_id.into(),
            password: self.password.into(),
        }
    }
}



pub struct SimnowAccount(pub &'static str, pub &'static str);

impl ConfigParse for SimnowAccount {
    type Output = CtpAccountConfig;
    fn config_parse(&self) -> Self::Output {
        CtpAccountStr {
            broker_id : "9999",
            account : self.0,
            password : self.1,
            trade_front : "tcp://180.168.146.187:10201",
            md_front : "tcp://180.168.146.187:10211",
            name_server : "",
            auth_code : "0000000000000000",
            user_product_info : "",
            app_id : "simnow_client_test", 
        }.config_parse()
    }
}