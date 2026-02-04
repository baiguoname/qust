# Qust
支持流式计算的查询引擎，底层基于rust, 应用层用的python
-----
* 流式计算，算子有状态保留，支持流式计算
* 性能高，大多数情况下速度比polars高，内存消耗更少
* 算子丰富，内置丰富的金融算子，比如k线合成、回测、组合优化等等
* 可拓展性强，底层基于rust的`datafusion`, 拓展到分布式很方便.

# 安装
```python
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple qust
```

# 使用


```python
import qust as qs
from qust import col
import polars as pl
import numpy as np
```


```python
n = 10
data = pl.DataFrame({
    "factor": np.random.randn(n),
    "code": np.random.choice(["a", "b", "c"], size=n, replace=True),
})
data_next = pl.DataFrame({
    "factor": np.random.randn(n),
    "code": np.random.choice(["a", "b", "c"], size=n, replace=True),
})

df = qs.with_cols(
    col("factor").mean().expanding().alias("cum_mean"),
    col("factor").mean().rolling(3).alias("rolling_mean"),
    col("factor").mean().expanding().over("code").alias("cum_mean_over")
)
```


```python
print(df.calc_data(data))
```

    shape: (10, 5)
    ┌───────────┬──────┬───────────┬──────────────┬───────────────┐
    │ factor    ┆ code ┆ cum_mean  ┆ rolling_mean ┆ cum_mean_over │
    │ ---       ┆ ---  ┆ ---       ┆ ---          ┆ ---           │
    │ f64       ┆ str  ┆ f64       ┆ f64          ┆ f64           │
    ╞═══════════╪══════╪═══════════╪══════════════╪═══════════════╡
    │ 0.111683  ┆ c    ┆ 0.111683  ┆ null         ┆ 0.111683      │
    │ 1.585938  ┆ b    ┆ 0.848811  ┆ null         ┆ 1.585938      │
    │ -1.154133 ┆ b    ┆ 0.181163  ┆ 0.181163     ┆ 0.215903      │
    │ -1.311661 ┆ b    ┆ -0.192043 ┆ -0.293285    ┆ -0.293285     │
    │ 1.56433   ┆ b    ┆ 0.159231  ┆ -0.300488    ┆ 0.171118      │
    │ -1.293334 ┆ c    ┆ -0.082863 ┆ -0.346889    ┆ -0.590826     │
    │ -2.050297 ┆ c    ┆ -0.363925 ┆ -0.593101    ┆ -1.077316     │
    │ -0.891518 ┆ b    ┆ -0.429874 ┆ -1.411717    ┆ -0.041409     │
    │ 0.378405  ┆ c    ┆ -0.340065 ┆ -0.85447     ┆ -0.713386     │
    │ -0.341524 ┆ b    ┆ -0.340211 ┆ -0.284879    ┆ -0.091428     │
    └───────────┴──────┴───────────┴──────────────┴───────────────┘



```python
print(df.calc_data(data_next)) # df 里面的算子都状态保留
```

    shape: (10, 5)
    ┌───────────┬──────┬───────────┬──────────────┬───────────────┐
    │ factor    ┆ code ┆ cum_mean  ┆ rolling_mean ┆ cum_mean_over │
    │ ---       ┆ ---  ┆ ---       ┆ ---          ┆ ---           │
    │ f64       ┆ str  ┆ f64       ┆ f64          ┆ f64           │
    ╞═══════════╪══════╪═══════════╪══════════════╪═══════════════╡
    │ -0.643198 ┆ c    ┆ -0.367756 ┆ -0.202106    ┆ -0.699348     │
    │ -0.097377 ┆ b    ┆ -0.345224 ┆ -0.3607      ┆ -0.092278     │
    │ 2.340298  ┆ c    ┆ -0.138645 ┆ 0.533241     ┆ -0.192741     │
    │ 0.269679  ┆ b    ┆ -0.109479 ┆ 0.837533     ┆ -0.047033     │
    │ 1.169616  ┆ b    ┆ -0.024206 ┆ 1.259864     ┆ 0.08815       │
    │ 0.303631  ┆ c    ┆ -0.003717 ┆ 0.580975     ┆ -0.12183      │
    │ 0.404867  ┆ b    ┆ 0.020318  ┆ 0.626038     ┆ 0.119821      │
    │ 1.007454  ┆ c    ┆ 0.075159  ┆ 0.571984     ┆ 0.01933       │
    │ 0.51271   ┆ b    ┆ 0.098188  ┆ 0.641677     ┆ 0.155539      │
    │ 1.670847  ┆ b    ┆ 0.176821  ┆ 1.06367      ┆ 0.281814      │
    └───────────┴──────┴───────────┴──────────────┴───────────────┘


# 与polars语法比较


```python
data = pl.DataFrame({
    "price": range(5),
    "code": ["a", "a", "a", "b", "b"]
})
df = qs.with_cols(
    col("price").sum().expanding().alias("cum_sum_otters"),
    pl.col("price").cum_sum().alias("cum_sum_polars"),
    col("price").sum().expanding().over("code").alias("cum_sum_otters_over"),
    pl.col("price").cum_sum().over("code").alias("cum_sum_polars_over")
)
df.calc_data(data)
```




<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (5, 6)</small><table border="1" class="dataframe"><thead><tr><th>price</th><th>code</th><th>cum_sum_otters</th><th>cum_sum_polars</th><th>cum_sum_otters_over</th><th>cum_sum_polars_over</th></tr><tr><td>i64</td><td>str</td><td>i64</td><td>i64</td><td>i64</td><td>i64</td></tr></thead><tbody><tr><td>0</td><td>&quot;a&quot;</td><td>0</td><td>0</td><td>0</td><td>0</td></tr><tr><td>1</td><td>&quot;a&quot;</td><td>1</td><td>1</td><td>1</td><td>1</td></tr><tr><td>2</td><td>&quot;a&quot;</td><td>3</td><td>3</td><td>3</td><td>3</td></tr><tr><td>3</td><td>&quot;b&quot;</td><td>6</td><td>6</td><td>3</td><td>3</td></tr><tr><td>4</td><td>&quot;b&quot;</td><td>10</td><td>10</td><td>7</td><td>7</td></tr></tbody></table></div>



# 与polars性能比较


```python
import time
n = 2000000
data = pl.DataFrame({
    "factor": np.random.randn(n),
    "code": np.random.choice(["a", "b"], size=n, replace=True),
})
```

### 1. qust单线程 vs polars多线程


```python
s = time.time()
_ = qs.select(
    col("factor").rank().rolling(10).over("code")
).calc_data(data)
print(f"qust: {(time.time() - s) * 1000.0}.ms")

s = time.time()
_ = data.select(
    pl.col("factor").rolling_rank(10).over("code")
)
print(f"polars: {(time.time() - s) * 1000.0}.ms")
```

    qust: 100.04281997680664.ms
    polars: 157.47618675231934.ms


### 2. qust多线程 vs polars多线程



```python
s = time.time()
_ = qs.select(
    col(*[col("factor").mean().alias(f"mean_{i}") for i in range(50)]).rolling(10).over("code")
).calc_data(data)
print(f"qust: {(time.time() - s) * 1000.0}.ms")

s = time.time()
_ = data.select(
    [pl.col("factor").rolling_mean(10).over("code").alias(f"mean_{i}") for i in range(50)]
)
print(f"polars: {(time.time() - s) * 1000.0}.ms")
```

    qust: 112.96653747558594.ms
    polars: 298.112154006958.ms


### 3. qust自定义算子 vs polars自定义算子


```python
class MeanUdf(qs.UdfRow):

    def __init__(self):
        self.sum = 0.0
        self.count = 0.0

    def output_schema(self, input_schema):
        return [("mean_res", pl.Float64)]
    
    def update(self, value):
        self.sum += value
        self.count += 1.0

    def calc(self):
        return [self.sum / self.count]

    def retract(self, value):
        self.sum -= value
        self.count -= 1.0

s = time.time()
_ = qs.select(
    col("factor").udf.row(MeanUdf()).rolling(10).over("code")
).calc_data(data)
print(f"qust: {(time.time() - s)}.s")

s = time.time()
_ = data.select(
    pl.col("factor").rolling_map(lambda x: x.mean(), 10).over("code")
)
print(f"polars: {(time.time() - s)}.s")
```

    qust: 1.298762321472168.s
    polars: 53.331793785095215.s


>--------
| 算子 | qust | polars | 提速 |
|----|------|-------------|---|
| 单个算子 | 100ms | 157ms | 1.5倍 |
| 多个算子 | 110ms | 290ms | 2.5倍 |
| 自定义rolling算子 | 1.5s | 53s | 40倍 | 

# 为什么有polars，还要写qust？

### 1. 流式计算

写量化策略的时候，一般有下面两种方法

1. 向量化计算

2. 事件驱动

如果策略用向量化计算，在实盘的时候就很慢，因为要重复计算历史数据, 而且很多策略没法向量化

如果策略用的事件驱动，回测的时候就很慢，而且事件驱动写法特别麻烦

流计算就是把算子都写成事件驱动的形式。比如计算移动平均，在算子里面存储两个状态 `(sum, count)`, 每有一个行新数据`value`过来，更新算子的内部状态:

`sum = sum + value`

`count = count + 1`

在需要计算结果的时候就用  `sum / count`

```python
data = pl.DataFrame({
    "value": [1, 2, 3, 4, 5]
})
data_next = pl.DataFrame({
    "value": [6, 7, 8]
})

df = qs.with_cols(
    col("value").mean().rolling(3).alias("rolling_mean"),
    col("value").std().expanding().alias("cum_std"),
)

print(df.calc_data(data))
shape: (5, 3)
┌───────┬──────────────┬──────────┐
│ value ┆ rolling_mean ┆ cum_std  │
│ ---   ┆ ---          ┆ ---      │
│ i64   ┆ f64          ┆ f64      │
╞═══════╪══════════════╪══════════╡
│ 1     ┆ null         ┆ null     │
│ 2     ┆ null         ┆ 0.707107 │
│ 3     ┆ 2.0          ┆ 1.0      │
│ 4     ┆ 3.0          ┆ 1.290994 │
│ 5     ┆ 4.0          ┆ 1.581139 │
└───────┴──────────────┴──────────┘
print(df.calc_data(data_next))
shape: (3, 3)
┌───────┬──────────────┬──────────┐
│ value ┆ rolling_mean ┆ cum_std  │
│ ---   ┆ ---          ┆ ---      │
│ i64   ┆ f64          ┆ f64      │
╞═══════╪══════════════╪══════════╡
│ 6     ┆ 5.0          ┆ 1.870829 │
│ 7     ┆ 6.0          ┆ 2.160247 │
│ 8     ┆ 7.0          ┆ 2.44949  │
└───────┴──────────────┴──────────┘
```
在第一个调用`df.calc_data(data)`的时候，df内部的算子都有状态保留，所以在第二个调用`df.calc_data(data_next)`时候，没有重新计算

实际情况是，绝大多数算子都有对应的事件驱动形式，少量的算子比如`pl.col("a").rank()`, 看起来不是事件驱动的形式（当前行的值受到未来行的值的影响），但是其实也可以变换成事件驱动形式，
* 转换成行算子，比如 a 列有a1，a2，a3三个元素，就是`col(a1, a2, a3).rank(axis=1)` 

* 事件驱动形式的批算子，每次计算的时候保证传入的数据完整，比如计算`pl.col("a").rank().over("date")`, 保证每次计算传入的数据包含整天的所有数据

`polars`不是也支持streaming吗？我看了polars的底层，觉得polars的streaming不是真正意义上的流式计算，只是为了避免out of memory，而且局限性大(比如`over`是用的 切割 -> 计算 -> 拼接)。如果polars要实现真正的流式计算，我估计底层得推倒重来改成`datafusion`的那种框架


### 2. 表达式解耦

`polars`的`Expr`用的`enum`, 这样就导致每实现一个算子，底层很多代码都要改, 这样就不难理解为什么一个简单的`pl.col("a").rolling_rank(10)`算子直到最近才实现，而且速度比我一个简单的实现慢一倍。

`datafusion`聚合算子用的`Box<dyn trait>`, 然后根据上下文选择不同路径的`ExecutionPlan`, 这样添加算子很方便，而且优化路径也很清晰，性能还不受影响。

`polars`这种写法还有个缺点，就是导致同样的逻辑写法割裂，比如求和逻辑有下面写法:
* `pl.col("a").sum()`

* `pl.col("a").cum_sum()`

* `pl.col("a").rolling_sum(10)`

* `df.group_by("b").agg([pl.col("a").sum()])`

如果说 `sum()` 和 `rolling_sum(10)`, 都是求和逻辑, 前一个是针对整列，后一个是针对滚动，但是 `rank()`和`rolling_rank(10)`, 又是两个不想关的算子, 而且并不存在`cum_rank()`这个算子，这样逻辑就很割裂，为什么能存在`cum_sum`, 但是不能存在`cum_rank`, `cum_skew`, `cum_cov`? 

相反用`datafusion`的上下文逻辑，写法就比较一致:
* `col("a").sum()`

* `col("a").sum().expanding()`

* `col("a").sum().rolling(10)`

* `col("a").sum().group_by("b")`


### 3. 多列返回

`polars` 和 `datafusion` 对单个算子都不支持多列返回，但是`datafusion`提供了插件接口，能改成多列返回:
```python
n = 7
data = pl.DataFrame({
    "y": np.random.randn(n),
    "x1": np.random.randn(n),
    "x2": np.random.randn(n),
})
res = qs.with_cols(
    col("y", "x1", "x2").stock.ols().rolling(4).add_suffix("rolling_beta"),
).calc_data(data)
print(res)
shape: (7, 5)
┌───────────┬───────────┬───────────┬─────────────────┬─────────────────┐
│ y         ┆ x1        ┆ x2        ┆ x1_rolling_beta ┆ x2_rolling_beta │
│ ---       ┆ ---       ┆ ---       ┆ ---             ┆ ---             │
│ f64       ┆ f64       ┆ f64       ┆ f64             ┆ f64             │
╞═══════════╪═══════════╪═══════════╪═════════════════╪═════════════════╡
│ 0.522261  ┆ -0.376497 ┆ -0.594123 ┆ null            ┆ null            │
│ 1.325991  ┆ -0.723979 ┆ 2.626444  ┆ null            ┆ null            │
│ 1.502309  ┆ -2.089571 ┆ 0.28167   ┆ null            ┆ null            │
│ -0.322316 ┆ 0.00877   ┆ -0.213895 ┆ -0.731707       ┆ 0.271784        │
│ -0.733964 ┆ -0.750248 ┆ -0.592936 ┆ -0.47639        ┆ 0.465733        │
│ 0.445435  ┆ -0.559213 ┆ -0.44069  ┆ -0.56446        ┆ 1.174467        │
│ 1.735427  ┆ -2.403888 ┆ 1.207053  ┆ -0.29973        ┆ 0.849167        │
└───────────┴───────────┴───────────┴─────────────────┴─────────────────┘
```
多列返回我能想到以下好处
* 多列返回在用一些比如k线合成算子，策略信号算子之类的比较方便

* 另一个是避免用`struct`, 如果底层依赖从`arror-rs`改成[`MinArrow`](https://github.com/pbower/minarrow), 估计内存占用能到原来的一半，并且耗时减少

### 4. `datafusion` 功能更齐全，比如:
* 支持`DataFrame` Api 和 sql相互转换，`polars`不行

* 原生支持`arrow`, `datafusion`是`arrow`的一部分，未来生态会更丰富, `polars`自己写了一个`polars-arrow`, 生态割裂

* `datafusion` 有成熟的分布式应用，而且全部开源，`polars` 前期是基于`datafusion`的二次开发，目前分布式刚起步，而且闭源，貌似已经**把主要精力放在商业闭源上面去了**

>--------
`qust`是用`rust`写的一个`datafusion`插件，主要目的是尝试用`DataFrame api`去写事件驱动量化策略，并且保持向量化计算的高性能.

所以主要是添加一些能够状态保留的算子，其他一些无需状态保留的算子，还是依赖于`polars`的算子，比如:
```python
col("a") + 1
```
会报错:
```
TypeError: unsupported operand type(s) for +: 'Expr' and 'int'
```
只能用`polars`的算子:
```python
qs.select(
    pl.col("a") + 1,
    pl.col("a").rank().over("code")
    col("a").select(pl.col("a") + 1).over("code")
)
```

当然，上面说的只是我个人的理解，对这方面有兴趣的朋友可以加我微信交流，微信号: aruster


# 写策略

### 1. 有k线数据，实现一个双均线策略


```python
# 策略逻辑
stra = (
    col(
        col("close"),
        col("datetime"),
        col("close").future.two_ma(10, 20), # 通过算子生成信号
    )
        .with_cols(col("cross_up", "cross_down").future.to_hold_always().alias("hold")) # 通过信号生成目标持仓
)
# 回测
df_bt = qs.select(
    stra.with_cols(
        col("close", "hold").future.backtest()
    ).expanding().select(
        col("pnl").sum().group_by(pl.col("datetime").dt.date().alias("date"))
    ).with_cols(
        col("pnl").sum().alias("pnl_cum").expanding() 
    )
)

# 实盘
df_live = qs.select(stra.expanding().select("hold").last_value())
```


```python
data = pl.read_parquet("https://github.com/baiguoname/qust/blob/main/examples/data/300_1min_vnpy.parquet?raw=true") # 从github读取数据，速度较慢
# 假设历史数据
data_his = data[:600000]
# 假设实盘数据流
data_live = [data[600000:601000], data[601000:602000]]
```


```python
# 回测
df_bt.calc_data(data_his)
```




<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (2_500, 3)</small><table border="1" class="dataframe"><thead><tr><th>date</th><th>pnl</th><th>pnl_cum</th></tr><tr><td>date</td><td>f64</td><td>f64</td></tr></thead><tbody><tr><td>2009-01-05</td><td>0.01188</td><td>0.01188</td></tr><tr><td>2009-01-06</td><td>-0.001413</td><td>0.010467</td></tr><tr><td>2009-01-07</td><td>0.010793</td><td>0.02126</td></tr><tr><td>2009-01-08</td><td>0.020342</td><td>0.041603</td></tr><tr><td>2009-01-09</td><td>-0.001545</td><td>0.040058</td></tr><tr><td>&hellip;</td><td>&hellip;</td><td>&hellip;</td></tr><tr><td>2019-04-11</td><td>0.007004</td><td>-0.338696</td></tr><tr><td>2019-04-12</td><td>-0.008302</td><td>-0.346997</td></tr><tr><td>2019-04-15</td><td>0.006445</td><td>-0.340553</td></tr><tr><td>2019-04-16</td><td>0.004031</td><td>-0.336522</td></tr><tr><td>2019-04-17</td><td>-0.022107</td><td>-0.358629</td></tr></tbody></table></div>




```python
# 实盘
df_live.calc_data(data_his)
for data_live_ in data_live:
    print(df_live.calc_data(data_live_))
```

    shape: (1, 1)
    ┌──────┐
    │ hold │
    │ ---  │
    │ f64  │
    ╞══════╡
    │ 1.0  │
    └──────┘
    shape: (1, 1)
    ┌──────┐
    │ hold │
    │ ---  │
    │ f64  │
    ╞══════╡
    │ 1.0  │
    └──────┘


### 2. 有数据源，这个数据源不断获取多个品种的tick数据，策略需要分品种将数据不断合成1min k线，并且生成双均线的开仓逻辑，然后用0.01止损作为出场


```python
# 策略逻辑
col_tick = col("t", "c", "v", "bid1", "ask1", "bid1_v", "ask1_v")
stra = (
    col(
        col("c"),
        col("t"),
        col_tick.future.kline(qs.KlineType.future_ra1m).with_cols(
            col("close").future.two_ma(10, 20).filter_cb("is_finished")
        ),
    ).with_cols(
        col(
            col("cross_up", "c").future.exit_by_pct(0.01, False).alias("take_profit_long"),
            col("cross_up", "c").future.exit_by_pct(0.01, True).alias("stop_loss_long"),
        )
            .with_cols(
                (pl.col("take_profit_long") | pl.col("stop_loss_long")).alias("exit_long_sig") 
            ),
        col(
            col("cross_down", "c").future.exit_by_pct(0.01, True).alias("take_profit_short"),
            col("cross_down", "c").future.exit_by_pct(0.01, False).alias("stop_loss_short"),
        )   
            .with_cols(
                (pl.col("take_profit_short") | pl.col("stop_loss_short")).alias("exit_short_sig")
            )
    ).with_cols(
        col("cross_up", "exit_long_sig", "cross_down", "exit_short_sig")
            .future
            .to_hold_two_sides()
            .alias("hold")
    )
)

# 价格回测
df_bt_price = (
    qs.select(
        stra
            .with_cols(
                col("c", "hold").future.backtest(),
            )
            .expanding()
            .over("ticker")
            .select(
                col("pnl").sum().group_by(pl.col("t").dt.date().alias("date"))
            )
            .with_cols(
                col("pnl").sum().alias("pnl_cum").expanding()
            )
    )
)

# tick回测
df_bt_tick = (
    qs.select(
        col(
            "bid1",
            "ask1",
            stra,
        )
            .with_cols(
                col("hold", "c", "bid1", "ask1")
                    .future
                    .backtest_tick(qs.TradePriceType.queue, qs.MatchPriceType.simnow)
                    # .backtest_tick(qs.TradePriceType.last_price, qs.MatchPriceType.void)
            )
            .expanding()
            .over("ticker")
            .select(
                col("pnl").sum().group_by(pl.col("t").dt.date().alias("date"))
            )
            .with_cols(
                col("pnl").sum().alias("pnl_cum").expanding()
            )
    )
)
```


```python
data = pl.read_parquet("https://github.com/baiguoname/qust/tree/main/examples/data/tick_data.parquet?raw=true")
```


```python
df_bt_price.calc_data(data).plot.line(x = "date", y = "pnl_cum")
```





<style>
  #altair-viz-299e8347ee214eeeb09cc19ab5aa4660.vega-embed {
    width: 100%;
    display: flex;
  }

  #altair-viz-299e8347ee214eeeb09cc19ab5aa4660.vega-embed details,
  #altair-viz-299e8347ee214eeeb09cc19ab5aa4660.vega-embed details summary {
    position: relative;
  }
</style>
<div id="altair-viz-299e8347ee214eeeb09cc19ab5aa4660"></div>
<script type="text/javascript">
  var VEGA_DEBUG = (typeof VEGA_DEBUG == "undefined") ? {} : VEGA_DEBUG;
  (function(spec, embedOpt){
    let outputDiv = document.currentScript.previousElementSibling;
    if (outputDiv.id !== "altair-viz-299e8347ee214eeeb09cc19ab5aa4660") {
      outputDiv = document.getElementById("altair-viz-299e8347ee214eeeb09cc19ab5aa4660");
    }

    const paths = {
      "vega": "https://cdn.jsdelivr.net/npm/vega@6?noext",
      "vega-lib": "https://cdn.jsdelivr.net/npm/vega-lib?noext",
      "vega-lite": "https://cdn.jsdelivr.net/npm/vega-lite@6.1.0?noext",
      "vega-embed": "https://cdn.jsdelivr.net/npm/vega-embed@7?noext",
    };

    function maybeLoadScript(lib, version) {
      var key = `${lib.replace("-", "")}_version`;
      return (VEGA_DEBUG[key] == version) ?
        Promise.resolve(paths[lib]) :
        new Promise(function(resolve, reject) {
          var s = document.createElement('script');
          document.getElementsByTagName("head")[0].appendChild(s);
          s.async = true;
          s.onload = () => {
            VEGA_DEBUG[key] = version;
            return resolve(paths[lib]);
          };
          s.onerror = () => reject(`Error loading script: ${paths[lib]}`);
          s.src = paths[lib];
        });
    }

    function showError(err) {
      outputDiv.innerHTML = `<div class="error" style="color:red;">${err}</div>`;
      throw err;
    }

    function displayChart(vegaEmbed) {
      vegaEmbed(outputDiv, spec, embedOpt)
        .catch(err => showError(`Javascript Error: ${err.message}<br>This usually means there's a typo in your chart specification. See the javascript console for the full traceback.`));
    }

    if(typeof define === "function" && define.amd) {
      requirejs.config({paths});
      let deps = ["vega-embed"];
      require(deps, displayChart, err => showError(`Error loading script: ${err.message}`));
    } else {
      maybeLoadScript("vega", "6")
        .then(() => maybeLoadScript("vega-lite", "6.1.0"))
        .then(() => maybeLoadScript("vega-embed", "7"))
        .catch(showError)
        .then(() => displayChart(vegaEmbed));
    }
  })({"config": {"view": {"continuousWidth": 300, "continuousHeight": 300}}, "data": {"name": "data-e139825d3e527d90d317ace5a2d3ec07"}, "mark": {"type": "line", "tooltip": true}, "encoding": {"x": {"field": "date", "type": "temporal"}, "y": {"field": "pnl_cum", "type": "quantitative"}}, "params": [{"name": "param_1e9efca18e7a2868", "select": {"type": "interval", "encodings": ["x", "y"]}, "bind": "scales"}], "$schema": "https://vega.github.io/schema/vega-lite/v6.1.0.json", "datasets": {"data-e139825d3e527d90d317ace5a2d3ec07": [{"date": "2024-01-06T00:00:00", "pnl": 0.0018277783535297631, "pnl_cum": 0.0018277783535297631}, {"date": "2024-01-08T00:00:00", "pnl": 0.003912573572573463, "pnl_cum": 0.0057403519261032265}, {"date": "2024-01-09T00:00:00", "pnl": 0.009277687511560329, "pnl_cum": 0.015018039437663555}, {"date": "2024-01-10T00:00:00", "pnl": -0.0012600611958496488, "pnl_cum": 0.013757978241813906}, {"date": "2024-01-11T00:00:00", "pnl": 0.02169537149855416, "pnl_cum": 0.03545334974036807}, {"date": "2024-01-12T00:00:00", "pnl": -0.011017120457941232, "pnl_cum": 0.024436229282426836}, {"date": "2024-01-13T00:00:00", "pnl": 0.0017924758546213893, "pnl_cum": 0.026228705137048225}, {"date": "2024-01-15T00:00:00", "pnl": -0.009166224313604254, "pnl_cum": 0.01706248082344397}, {"date": "2024-01-16T00:00:00", "pnl": 0.008733752532386951, "pnl_cum": 0.025796233355830922}, {"date": "2024-01-17T00:00:00", "pnl": 0.0063110738388879595, "pnl_cum": 0.03210730719471888}, {"date": "2024-01-18T00:00:00", "pnl": 0.0009795279391042744, "pnl_cum": 0.033086835133823156}, {"date": "2024-01-19T00:00:00", "pnl": -0.004024871543863884, "pnl_cum": 0.029061963589959272}, {"date": "2024-01-20T00:00:00", "pnl": -0.001027746259070983, "pnl_cum": 0.02803421733088829}, {"date": "2024-01-22T00:00:00", "pnl": 0.018770235252460243, "pnl_cum": 0.04680445258334853}, {"date": "2024-01-23T00:00:00", "pnl": 0.007934696929051466, "pnl_cum": 0.0547391495124}, {"date": "2024-01-24T00:00:00", "pnl": 0.009947013241739744, "pnl_cum": 0.06468616275413974}, {"date": "2024-01-25T00:00:00", "pnl": 0.009984975091812398, "pnl_cum": 0.07467113784595214}, {"date": "2024-01-26T00:00:00", "pnl": -0.004940224463215626, "pnl_cum": 0.06973091338273651}, {"date": "2024-01-27T00:00:00", "pnl": 0.0006826591765081158, "pnl_cum": 0.07041357255924463}, {"date": "2024-01-29T00:00:00", "pnl": -0.007033320575090762, "pnl_cum": 0.06338025198415387}, {"date": "2024-01-30T00:00:00", "pnl": -0.0011836664823088316, "pnl_cum": 0.062196585501845036}, {"date": "2024-01-31T00:00:00", "pnl": 0.001537502588681905, "pnl_cum": 0.06373408809052694}, {"date": "2024-02-01T00:00:00", "pnl": -0.009550703509586644, "pnl_cum": 0.0541833845809403}]}}, {"mode": "vega-lite"});
</script>




```python
df_bt_tick.calc_data(data).plot.line(x = "date", y = "pnl_cum")
```





<style>
  #altair-viz-7cc8441c481b438c84210801c992fc17.vega-embed {
    width: 100%;
    display: flex;
  }

  #altair-viz-7cc8441c481b438c84210801c992fc17.vega-embed details,
  #altair-viz-7cc8441c481b438c84210801c992fc17.vega-embed details summary {
    position: relative;
  }
</style>
<div id="altair-viz-7cc8441c481b438c84210801c992fc17"></div>
<script type="text/javascript">
  var VEGA_DEBUG = (typeof VEGA_DEBUG == "undefined") ? {} : VEGA_DEBUG;
  (function(spec, embedOpt){
    let outputDiv = document.currentScript.previousElementSibling;
    if (outputDiv.id !== "altair-viz-7cc8441c481b438c84210801c992fc17") {
      outputDiv = document.getElementById("altair-viz-7cc8441c481b438c84210801c992fc17");
    }

    const paths = {
      "vega": "https://cdn.jsdelivr.net/npm/vega@6?noext",
      "vega-lib": "https://cdn.jsdelivr.net/npm/vega-lib?noext",
      "vega-lite": "https://cdn.jsdelivr.net/npm/vega-lite@6.1.0?noext",
      "vega-embed": "https://cdn.jsdelivr.net/npm/vega-embed@7?noext",
    };

    function maybeLoadScript(lib, version) {
      var key = `${lib.replace("-", "")}_version`;
      return (VEGA_DEBUG[key] == version) ?
        Promise.resolve(paths[lib]) :
        new Promise(function(resolve, reject) {
          var s = document.createElement('script');
          document.getElementsByTagName("head")[0].appendChild(s);
          s.async = true;
          s.onload = () => {
            VEGA_DEBUG[key] = version;
            return resolve(paths[lib]);
          };
          s.onerror = () => reject(`Error loading script: ${paths[lib]}`);
          s.src = paths[lib];
        });
    }

    function showError(err) {
      outputDiv.innerHTML = `<div class="error" style="color:red;">${err}</div>`;
      throw err;
    }

    function displayChart(vegaEmbed) {
      vegaEmbed(outputDiv, spec, embedOpt)
        .catch(err => showError(`Javascript Error: ${err.message}<br>This usually means there's a typo in your chart specification. See the javascript console for the full traceback.`));
    }

    if(typeof define === "function" && define.amd) {
      requirejs.config({paths});
      let deps = ["vega-embed"];
      require(deps, displayChart, err => showError(`Error loading script: ${err.message}`));
    } else {
      maybeLoadScript("vega", "6")
        .then(() => maybeLoadScript("vega-lite", "6.1.0"))
        .then(() => maybeLoadScript("vega-embed", "7"))
        .catch(showError)
        .then(() => displayChart(vegaEmbed));
    }
  })({"config": {"view": {"continuousWidth": 300, "continuousHeight": 300}}, "data": {"name": "data-4b85095c1420e1683a4462158603b157"}, "mark": {"type": "line", "tooltip": true}, "encoding": {"x": {"field": "date", "type": "temporal"}, "y": {"field": "pnl_cum", "type": "quantitative"}}, "params": [{"name": "param_1e9efca18e7a2868", "select": {"type": "interval", "encodings": ["x", "y"]}, "bind": "scales"}], "$schema": "https://vega.github.io/schema/vega-lite/v6.1.0.json", "datasets": {"data-4b85095c1420e1683a4462158603b157": [{"date": "2024-01-06T00:00:00", "pnl": 0.0, "pnl_cum": 0.0}, {"date": "2024-01-08T00:00:00", "pnl": 0.0, "pnl_cum": 0.0}, {"date": "2024-01-09T00:00:00", "pnl": 116.0, "pnl_cum": 116.0}, {"date": "2024-01-10T00:00:00", "pnl": 0.0, "pnl_cum": 116.0}, {"date": "2024-01-11T00:00:00", "pnl": 0.0, "pnl_cum": 116.0}, {"date": "2024-01-12T00:00:00", "pnl": 151.0, "pnl_cum": 267.0}, {"date": "2024-01-13T00:00:00", "pnl": 0.0, "pnl_cum": 267.0}, {"date": "2024-01-15T00:00:00", "pnl": -75.0, "pnl_cum": 192.0}, {"date": "2024-01-16T00:00:00", "pnl": 95.0, "pnl_cum": 287.0}, {"date": "2024-01-17T00:00:00", "pnl": 0.0, "pnl_cum": 287.0}, {"date": "2024-01-18T00:00:00", "pnl": 0.0, "pnl_cum": 287.0}, {"date": "2024-01-19T00:00:00", "pnl": 0.0, "pnl_cum": 287.0}, {"date": "2024-01-20T00:00:00", "pnl": 0.0, "pnl_cum": 287.0}, {"date": "2024-01-22T00:00:00", "pnl": 87.0, "pnl_cum": 374.0}, {"date": "2024-01-23T00:00:00", "pnl": 58.0, "pnl_cum": 432.0}, {"date": "2024-01-24T00:00:00", "pnl": 0.0, "pnl_cum": 432.0}, {"date": "2024-01-25T00:00:00", "pnl": 36.0, "pnl_cum": 468.0}, {"date": "2024-01-26T00:00:00", "pnl": 0.0, "pnl_cum": 468.0}, {"date": "2024-01-27T00:00:00", "pnl": 0.0, "pnl_cum": 468.0}, {"date": "2024-01-29T00:00:00", "pnl": -122.0, "pnl_cum": 346.0}, {"date": "2024-01-30T00:00:00", "pnl": 0.0, "pnl_cum": 346.0}, {"date": "2024-01-31T00:00:00", "pnl": 0.0, "pnl_cum": 346.0}, {"date": "2024-02-01T00:00:00", "pnl": -105.0, "pnl_cum": 241.0}]}}, {"mode": "vega-lite"});
</script>



### 3. 一个更复杂的策略，接受tick数据，同时合成5min和30min的k线，双周期共振的均线策略


```python
# 策略逻辑
col_tick = col("t", "c", "v", "bid1", "ask1", "bid1_v", "ask1_v")
stra =  (
    col(
        col("c"),
        col("t"),
        col_tick.future.kline(qs.KlineType.rl5m)
            .with_cols(
                col("close").future.two_ma(10, 20).filter_cb("is_finished")
            )
            .add_suffix("m5"),
        col_tick.future.kline(qs.KlineType.rl30m)
            .with_cols(
                col("close").future.two_ma(10, 20).filter_cb("is_finished")
            )
            .add_suffix("m30")
    )
        .with_cols(
            col("cross_up_m30", "cross_down_m30").ffill()
        )
        .with_cols(
            col(pl.col("cross_up_m5") & pl.col("cross_up_m30")).alias("open_long_sig"),
            col(pl.col("cross_down_m5") & pl.col("cross_down_m30")).alias("open_short_sig"),
        )
        .with_cols(
            col(
                col("open_long_sig", "c").future.exit_by_pct(0.05, False).alias("take_profit_long"),
                col("open_long_sig", "c").future.exit_by_pct(0.02, True).alias("stop_loss_long"),
            )
                .select(
                    (pl.col("take_profit_long") | pl.col("stop_loss_long")).alias("exit_long_sig") 
                ),
            col(
                col("open_short_sig", "c").future.exit_by_pct(0.05, True).alias("take_profit_short"),
                col("open_short_sig", "c").future.exit_by_pct(0.02, False).alias("stop_loss_short"),
            )   
                .select(
                    (pl.col("take_profit_short") | pl.col("stop_loss_short")).alias("exit_short_sig")
                )
        )
        .with_cols(
            col("open_long_sig", "exit_long_sig", "open_short_sig", "exit_short_sig")
                .future
                .to_hold_two_sides()
                .alias("hold")
        )
)

# tick回测逻辑
df_bt_tick = (
    qs.select(
        col(
            "bid1",
            "ask1",
            stra,
        )
            .with_cols(
                col("hold", "c", "bid1", "ask1")
                    .future
                    .backtest_tick(qs.TradePriceType.queue, qs.MatchPriceType.simnow)
                    # .backtest_tick(qs.TradePriceType.last_price, qs.MatchPriceType.void)
            )
            .expanding()
            .over("ticker")
            .select(
                col("pnl").sum().group_by(pl.col("t").dt.date().alias("date"))
            )
            .with_cols(
                col("pnl").sum().alias("pnl_cum").expanding()
            )
    )
)
```


```python
df_bt_tick.calc_data(data).plot.line(x = "date", y = "pnl_cum")
```





<style>
  #altair-viz-344527d5910e47fdbae740f4c22ad502.vega-embed {
    width: 100%;
    display: flex;
  }

  #altair-viz-344527d5910e47fdbae740f4c22ad502.vega-embed details,
  #altair-viz-344527d5910e47fdbae740f4c22ad502.vega-embed details summary {
    position: relative;
  }
</style>
<div id="altair-viz-344527d5910e47fdbae740f4c22ad502"></div>
<script type="text/javascript">
  var VEGA_DEBUG = (typeof VEGA_DEBUG == "undefined") ? {} : VEGA_DEBUG;
  (function(spec, embedOpt){
    let outputDiv = document.currentScript.previousElementSibling;
    if (outputDiv.id !== "altair-viz-344527d5910e47fdbae740f4c22ad502") {
      outputDiv = document.getElementById("altair-viz-344527d5910e47fdbae740f4c22ad502");
    }

    const paths = {
      "vega": "https://cdn.jsdelivr.net/npm/vega@6?noext",
      "vega-lib": "https://cdn.jsdelivr.net/npm/vega-lib?noext",
      "vega-lite": "https://cdn.jsdelivr.net/npm/vega-lite@6.1.0?noext",
      "vega-embed": "https://cdn.jsdelivr.net/npm/vega-embed@7?noext",
    };

    function maybeLoadScript(lib, version) {
      var key = `${lib.replace("-", "")}_version`;
      return (VEGA_DEBUG[key] == version) ?
        Promise.resolve(paths[lib]) :
        new Promise(function(resolve, reject) {
          var s = document.createElement('script');
          document.getElementsByTagName("head")[0].appendChild(s);
          s.async = true;
          s.onload = () => {
            VEGA_DEBUG[key] = version;
            return resolve(paths[lib]);
          };
          s.onerror = () => reject(`Error loading script: ${paths[lib]}`);
          s.src = paths[lib];
        });
    }

    function showError(err) {
      outputDiv.innerHTML = `<div class="error" style="color:red;">${err}</div>`;
      throw err;
    }

    function displayChart(vegaEmbed) {
      vegaEmbed(outputDiv, spec, embedOpt)
        .catch(err => showError(`Javascript Error: ${err.message}<br>This usually means there's a typo in your chart specification. See the javascript console for the full traceback.`));
    }

    if(typeof define === "function" && define.amd) {
      requirejs.config({paths});
      let deps = ["vega-embed"];
      require(deps, displayChart, err => showError(`Error loading script: ${err.message}`));
    } else {
      maybeLoadScript("vega", "6")
        .then(() => maybeLoadScript("vega-lite", "6.1.0"))
        .then(() => maybeLoadScript("vega-embed", "7"))
        .catch(showError)
        .then(() => displayChart(vegaEmbed));
    }
  })({"config": {"view": {"continuousWidth": 300, "continuousHeight": 300}}, "data": {"name": "data-22397b8a905396f7a8898ee0713a2426"}, "mark": {"type": "line", "tooltip": true}, "encoding": {"x": {"field": "date", "type": "temporal"}, "y": {"field": "pnl_cum", "type": "quantitative"}}, "params": [{"name": "param_1e9efca18e7a2868", "select": {"type": "interval", "encodings": ["x", "y"]}, "bind": "scales"}], "$schema": "https://vega.github.io/schema/vega-lite/v6.1.0.json", "datasets": {"data-22397b8a905396f7a8898ee0713a2426": [{"date": "2024-01-06T00:00:00", "pnl": 0.0, "pnl_cum": 0.0}, {"date": "2024-01-08T00:00:00", "pnl": 0.0, "pnl_cum": 0.0}, {"date": "2024-01-09T00:00:00", "pnl": 0.0, "pnl_cum": 0.0}, {"date": "2024-01-10T00:00:00", "pnl": 0.0, "pnl_cum": 0.0}, {"date": "2024-01-11T00:00:00", "pnl": 0.0, "pnl_cum": 0.0}, {"date": "2024-01-12T00:00:00", "pnl": 0.0, "pnl_cum": 0.0}, {"date": "2024-01-13T00:00:00", "pnl": 0.0, "pnl_cum": 0.0}, {"date": "2024-01-15T00:00:00", "pnl": 0.0, "pnl_cum": 0.0}, {"date": "2024-01-16T00:00:00", "pnl": 0.0, "pnl_cum": 0.0}, {"date": "2024-01-17T00:00:00", "pnl": 0.0, "pnl_cum": 0.0}, {"date": "2024-01-18T00:00:00", "pnl": 0.0, "pnl_cum": 0.0}, {"date": "2024-01-19T00:00:00", "pnl": 0.0, "pnl_cum": 0.0}, {"date": "2024-01-20T00:00:00", "pnl": 0.0, "pnl_cum": 0.0}, {"date": "2024-01-22T00:00:00", "pnl": -119.0, "pnl_cum": -119.0}, {"date": "2024-01-23T00:00:00", "pnl": 0.0, "pnl_cum": -119.0}, {"date": "2024-01-24T00:00:00", "pnl": 0.0, "pnl_cum": -119.0}, {"date": "2024-01-25T00:00:00", "pnl": 0.0, "pnl_cum": -119.0}, {"date": "2024-01-26T00:00:00", "pnl": 0.0, "pnl_cum": -119.0}, {"date": "2024-01-27T00:00:00", "pnl": 0.0, "pnl_cum": -119.0}, {"date": "2024-01-29T00:00:00", "pnl": 0.0, "pnl_cum": -119.0}, {"date": "2024-01-30T00:00:00", "pnl": 0.0, "pnl_cum": -119.0}, {"date": "2024-01-31T00:00:00", "pnl": 0.0, "pnl_cum": -119.0}, {"date": "2024-02-01T00:00:00", "pnl": 0.0, "pnl_cum": -119.0}]}}, {"mode": "vega-lite"});
</script>




```python

```
