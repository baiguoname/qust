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
You can build a strategy basing on kline data and backtest in on kline:
```rust
use qust_derive::*;
use qust_ds::prelude::*;
use qust::prelude::*;
use qust_api::prelude::*;
use qust_io::prelude::*;
use ta::{ Next, indicators::SimpleMovingAverage as SMA };

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

#[tokio::main]
async fn main() {
    let di = read_remote_kline_data().await;
    let two_ma_stra = TwoMaStra { short_period: 9, long_period: 20 };
    let two_ma_stra_ptm: Ptm = two_ma_stra.ktn_box().to_ktn().to_ptm();
    let pnl_res_dt: PnlRes<dt> = two_ma_stra_ptm.bt_kline((&di, cs1));
    pnl_res_dt.to_csv("pnl_res_dt.csv"); // save the pnl to local csv;
}

```

# 更新 
## version: 0.1.5
1. 支持tick级别的横截面，目前不支持k线级别，可以在tick里面手动更新k线。需要指定各个ticker的到达时间，详见[例子](https://github.com/baiguoname/qust/qust-stra/src/bin/main_test.rs);
2. 每个策略(`ApiBridgeBox`)都有自身的订单管理，api程序停止运行后，到下次重开程序，中间过程中如果没有手动开平仓，历史的订单会被读取