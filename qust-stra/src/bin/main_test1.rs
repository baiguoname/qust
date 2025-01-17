#![allow(unused)]
use std::sync::Arc;

use qust::live::bt::{ApiType, BtKline, BtTick, RetFnApi};
use qust_derive::*;
use qust_ds::prelude::*;
use qust::prelude::*;
use qust_api::prelude::*;
use qust_io::prelude::*;
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
            if hold == 0. {
                match last_short_value != 0. && last_short_value < last_long_value && short_value >= long_value {
                    true => {
                        res = OrderAction::LoOpen(1., stream_api.tick_data.bid1);
                    }
                    false => (),
                }
            } else if hold > 0. && short_value < long_value {
                res = OrderAction::ShClose(hold, stream_api.tick_data.ask1);
            }
            last_short_value = short_value;
            last_long_value = long_value;
            res
        })
    }
}

#[ta_derive2]
pub struct TwoMaStra {
    pub short_period: usize,
    pub long_period: usize,
}

#[typetag::serde]
impl Ktn for TwoMaStra {
    fn ktn(&self,_di: &Di) -> RetFnKtn {
        let mut last_norm_hold = NormHold::No;
        let mut short_ma = SMA::new(self.short_period).unwrap();
        let mut long_ma = SMA::new(self.long_period).unwrap();
        let mut last_short_value = 0f64;
        let mut last_long_value = 0f64;
        Box::new(move |di_kline| {
            let c = di_kline.di.c()[di_kline.i] as f64;
            let short_value = short_ma.next(c);
            let long_value = long_ma.next(c);
            match last_norm_hold {
                NormHold::No if di_kline.i != 0 => {
                    if last_short_value < last_long_value && short_value >= long_value {
                        last_norm_hold = NormHold::Lo(1.);
                    }
                }
                NormHold::Lo(_) if short_value < long_value => {
                    last_norm_hold = NormHold::No;
                }
                _ => {}
            }
            last_short_value = short_value;
            last_long_value = long_value;
            last_norm_hold.clone()
        })
    }
}

async fn backtest_kline() {
    let di = read_remote_kline_data().await;
    let two_ma_stra = TwoMaStra { short_period: 10, long_period: 20 };
    let two_ma_stra_ptm: Ptm = two_ma_stra.ktn_box().to_ktn().to_ptm();
    let pnl_res_dt: PnlRes<dt> = two_ma_stra_ptm.bt_kline((&di, cs2));
    pnl_res_dt.to_csv("pnl_res_dt.csv"); // save the pnl to local csv;
}

async fn backtest_tick() {
    // let tick_data = read_remote_tick_data().await;
    // TwoMaTickOrderAction
    //     .with_info::<BtMatchBox>(Box::new(MatchSimple))
    //     .bt_tick(&tick_data)
    //     .with_info(aler)
    //     .into_pnl_res()
    //     .to_csv("pnl_res_tick.csv");
    todo!();
}


#[tokio::main]
async fn main() {
    backtest_kline().await;
    backtest_tick().await;

    let ticker_contract_map = ["al2501", "eb2501"].config_parse();

    let stra1 = TwoMaTickOrderAction
        .pip(|x| TradeOne::new(x, aler, &ticker_contract_map))
        .api_bridge_box();
    let stra2 = TwoMaTickOrderAction
        .pip(|x| TradeOne::new(x, eber, &ticker_contract_map))
        .api_bridge_box();
    
    let stra_api = vec![stra1, stra2].to_stra_api();
    let account = SimnowAccount("171808", "Tangjihede00").config_parse();//account , password
    let running_api = running_api_ctp(stra_api, account);
    run_ctp(running_api).await;
}
