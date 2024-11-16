use std::collections::VecDeque;
use super::live_ops::*;
use anyhow::Result;
use qust_ds::prelude::logging_service;

pub trait ServiceApi {
    fn start(&self, trade_api: Vec<TradeApi>) -> Result<()>;
    fn stop(&self, trade_api: Vec<TradeApi>) -> Result<()>;
}
pub struct RunningApi<T, N> {
    pub stra_api: T,
    pub service_api: N,
    pub log_path: Option<String>,
    pub trade_api: Vec<TradeApi>,
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

pub trait ApiBridge: Send + Sync {
    fn gen_trade_api(&self) -> Vec<TradeApi>;

    fn handle_notify<'a>(&'a self) -> Box<dyn FnMut(VecDeque<DataRecv>) + 'a>;

    fn data_recv_get(&self) -> NotifyDataRecv;
    fn start_service(&self) -> Option<()> {
        let data = self.data_recv_get();
        let mut data_ops = self.handle_notify();
        loop {
            let (mut guard, is_started) = data.wait_or_exit("aaa");
            if !is_started {
                break;
            }
            let mut data_receive_vec = VecDeque::default();
            data_receive_vec.append(&mut guard);
            drop(guard);
            data_ops(data_receive_vec);
        }
        Some(())
    }

    fn api_bridge_box(self) -> Box<dyn ApiBridge> 
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

pub type ApiBridgeBox = Box<dyn ApiBridge>;
