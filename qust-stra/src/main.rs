use qust_derive::*;
use qust_ds::prelude::*;
use qust::prelude::*;
use qust_api::prelude::*;
use ta::{ Next, indicators::SimpleMovingAverage as SMA };

#[ta_derive2]
pub struct TwoMaTickOrderAction;

impl ApiType for TwoMaTickOrderAction {
    fn api_type(&self) -> RetFnApi {
        let mut short_ma = SMA::new(1200).unwrap();
        let mut long_ma = SMA::new(2400).unwrap();
        let mut last_short_value = 0f64;
        let mut last_long_value = 0f64;
        Box::new(move |stream_api| {
            let c = stream_api.tick_data.c as f64;
            let short_value = short_ma.next(c);
            let long_value = long_ma.next(c);
            let hold = stream_api.hold.sum();
            let mut res = OrderAction::No;
            if hold == 0 {
                match last_short_value != 0. && last_short_value < last_long_value && short_value >= long_value {
                    true => {
                        res = OrderAction::LoOpen(1, stream_api.tick_data.bid1);
                    }
                    false => (),
                }
            } else if hold > 0 && short_value < long_value {
                res = OrderAction::ShClose(hold, stream_api.tick_data.ask1);
            }
            last_short_value = short_value;
            last_long_value = long_value;
            res
        })
    }
}

#[tokio::main]
async fn main() {
    let live_stra_pool = TwoMaTickOrderAction.to_live_stra_pool(vec![aler, eber]);
    let ticker_contract_map = ["al2401", "eb2401"].config_parse();
    let stra_api = StraApi::new( live_stra_pool, ticker_contract_map);
    let account = SimnowAccount("171808", "Tangjihede00").config_parse();//account , password
    let running_api = running_api_ctp(stra_api, account);
    run_ctp(running_api).await;
}
