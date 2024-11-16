use std::sync::{Arc, RwLock};

use qust::prelude::*;
use qust_api::prelude::*;
use qust_ds::prelude::*;
use qust_io::prelude::*;
use stra::prelude::rl5mall;


#[tokio::main]
async fn main()  {
    let gen_di = GenDi("/root/qust/data");
    let dil = gen_di.gen((rl5mall.clone().tri_box(), vec![aler, ier, eber]), y2024.clone());
    let di = dil.dil[1].clone();
    let k = stra::p05::condt.with_info(RwLock::new(di)).with_info(AlgoTarget.algo_box());
    let ticker_contract_map = ["i2501"].config_parse();
    let stra1: ApiBridgeBox = TradeOne::new(k, ier, &ticker_contract_map).pip(Box::new);
    let stra_api = StraApi {
        pool: vec![
            Arc::new(stra1),
        ],
    };
    let account = SimnowAccount("171808", "Tangjihede00").config_parse();
    let running_api = running_api_ctp(stra_api, account);
    run_ctp(running_api).await;
}