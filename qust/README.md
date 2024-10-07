# Qust
Qust is a Rust libraries for building live-trading and back-test systems. It has the following features:
* **Fast**: It's way to handle or to save the kline data, tick data and strategy makes the backtest and live trading fast.
* **Extensible**: It provide many ways to build a strategy, and new ways can be implemented by needs, so that you can focus what you care. You can build a simple strategy or complicated one, then backtest it on kline data(for quick scruch) or tick data, on put it on live trading directly. For example, you can build a strategy by following ways:
    1. Accept kline flow, and return a target position.
    2. Accept tick data flow, and return a target position.
    3. Accept tick data flow, and return an order action.
    4. Accept kline and tick flow, return a target positon or an order action.
    5. Accept kline flow, and return a bool.(a least two of it make a  strategy, one for open position, another for close)
    6. Add filter conditions to an existed strategy.
    7. Add algorithm method to an existed strategy.
    8. Add order matching methods when backtest a strategy.
    9. Add valitality manager to strategies.
    10. Add portoflio manager to a pool of strategies.
    and so on.


See this [notebook Example](https://github.com/baiguoname/qust/blob/main/examples/git_test/git_test.ipynb) for more detail.

# Examples
Add this to `Cargo.toml`:
```rust
qust-derive = { version = ">=0.1" }
qust-ds = { version = ">=0.1" }
qust = { version = ">=0.1" }
qust-api = { version = ">=0.1"}
qust-io = {  version = ">=0.1"}
serde = "*"
serde_json = "*"
itertools = "*"
typetag = "*"
tokio = "*"
ta = { version = "0.5.0" }
```
You can build a strategy basing on kline data:
```rust
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
```
Then backtest on kline data like this:
```rust
async fn backtest_kline() {
    let di = read_remote_kline_data().await;
    let two_ma_stra = TwoMaStra { short_period: 10, long_period: 20 };
    let two_ma_stra_ptm: Ptm = two_ma_stra.ktn_box().to_ktn().to_ptm();
    let pnl_res_dt: PnlRes<dt> = two_ma_stra_ptm.bt_kline((&di, cs2));
    pnl_res_dt.to_csv("pnl_res_dt.csv"); // save the pnl to local csv;
}
```
Or put in live trading:
```rust
#[tokio::main]
async fn main() {
    // backtest_kline().await;
    // backtest_tick().await;
    let live_stra_pool = TwoMaTickOrderAction.to_live_stra_pool(vec![aler, eber]);
    let ticker_contract_map = ["al2401", "eb2401"].config_parse();
    let stra_api = StraApi::new( live_stra_pool, ticker_contract_map);
    let account = SimnowAccount("171808", "Tangjihede00").config_parse();//account , password
    let running_api = running_api_ctp(stra_api, account);
    run_ctp(running_api).await;
}
```