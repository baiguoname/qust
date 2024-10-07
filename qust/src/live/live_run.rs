use std::sync::Arc;
use super::prelude::{StraApi, TradeApi};
use anyhow::Result;
use qust_ds::prelude::logging_service;


pub trait ServiceApi {
    fn start(&self, trade_api: Vec<Arc<TradeApi>>) -> Result<()>;
    fn stop(&self, trade_api: Vec<Arc<TradeApi>>) -> Result<()>;
}


impl ServiceApi for StraApi {
    fn start(&self, trade_api_vec: Vec<Arc<TradeApi>>) -> Result<()> {
        trade_api_vec.iter().for_each(|x| {
            self.start_spy_on_data_send(Arc::clone(x));
            self.start_spy_on_data_receive(Arc::clone(x));
        });
        Ok(())
    }

    fn stop(&self, trade_api_vec: Vec<Arc<TradeApi>>) -> Result<()> {
        trade_api_vec.iter().for_each(|trade_api| {
            trade_api.data_send.stop();
            trade_api.data_receive.stop();
        });
        Ok(())
    }
}

pub struct RunningApi<T, N> {
    pub stra_api: T,
    pub service_api: N,
    pub log_path: Option<String>,
    pub trade_api: Vec<Arc<TradeApi>>,
}

impl<T, N> RunningApi<T, N>
where
    T: ServiceApi,
    N: ServiceApi,
{
    pub fn init(&self) -> Result<()> {
        if let Some(log_path) = &self.log_path {
            let ticker_vec: Vec<_> = self.trade_api.iter().map(|x| x.ticker.to_string()).collect();
            logging_service(log_path.clone(), ticker_vec);
        }
        Ok(())
    }

    pub fn start(&self) -> Result<()> {
        self.stra_api.start(self.trade_api.clone())?;
        self.service_api.start(self.trade_api.clone())?;
        Ok(())
    }

    pub fn stop(&self) -> Result<()> {
        self.stra_api.stop(self.trade_api.clone())?;
        self.service_api.stop(self.trade_api.clone())?;
        Ok(())
    }
}