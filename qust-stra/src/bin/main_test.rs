use std::sync::Arc;

use qust_api::prelude::*;
use qust_ds::prelude::*;
use qust::prelude::*;
use chrono::Timelike;



#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CondTest1 {
    pub cond: Vec<Ticker>,
}

impl CondCrossTarget for CondTest1 {
    fn cond_cross_target(&self) -> RetFnCrossTarget {
        let data_len = self.cond.len();
        Box::new(move |stream| {
            if stream[0].t.minute() % 2 == 0 {
                vec![OrderTarget::Lo(1.); data_len]
            } else {
                vec![OrderTarget::No; data_len]
            }

        })
    }
}

impl GetTickerVec for CondTest1 {
    fn get_ticker_vec(&self) -> Vec<Ticker> {
        self.cond.clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CondTest2;


impl CondType7 for CondTest2 {
    fn cond_type7(&self) -> RetFnCondType7 {
        Box::new(move |stream| {
            if stream.t.minute() % 2 == 0 {
                OrderTarget::Sh(10.)
            } else {
                OrderTarget::No
            }
        })
    }
}


#[tokio::main]
async fn main() {
    let ticker_contract_map = ["eb2501", "eg2501"].config_parse();

    let stra1 = CondTest1 { cond: vec![eber, eger] }
        .with_info(AlgoTarget)
        .with_info(AllEmergedQue(2))
        .pip(|x| TradeCross::new(x, &ticker_contract_map))
        .api_bridge_box();

    let stra2 = CondTest2
        .cond_type7_box()
        .with_info(AlgoTarget)
        .pip(|x| TradeOne::new(x, eber, &ticker_contract_map))
        .api_bridge_box();

    let stra_api = vec![stra1, stra2].to_stra_api();
    let account = SimnowAccount("171808", "Tangjihede00").config_parse();
    let running_api = running_api_ctp(stra_api, account);
    run_ctp(running_api).await;
}