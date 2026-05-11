# Qust 0.9.0 更新说明

Qust 0.9.0 的重点是把研究表达式从单机脚本推进到可私有化部署的研究执行环境：远程计算、流式 join、多数据源接入和 ClickHouse 下推能力变得更完整。这个版本适合需要在私有数据环境中运行大量策略、维护统一算子体系、分析策略并实时监控策略状态的场景。

## 适用场景

0.9.0 更适合以下场景：

1. 数据在服务器或内网数据库中，不适合下载到本机。
2. 需要对 Parquet、ClickHouse、Python DataFrame、UDF 数据源使用同一套表达式。
3. 策略研究依赖多表 join，例如 trades/quotes、行情/因子、信号/成交、持仓/风控状态。
4. 需要对持续到达的数据做流式计算和实时监控。
5. 需要在私有化环境中管理大量策略的运行、回测、结果归档和监控面板。

## 安装和服务端启动

客户端和服务端都安装同一个 Python 包；区别只在进程角色。服务端是常驻进程，负责访问服务器本地文件、内网 ClickHouse、内部行情服务和 UDF 运行环境；客户端只写表达式并把 remote server URL 传给 `Expr.remote(...)`。

```bash
pip install qust
```

国内镜像：

```bash
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple qust
```

服务端机器启动 remote server：

```bash
python -m qust.remote.server --host 0.0.0.0 --port 8899
```

客户端机器只需要知道服务端地址：

```python
REMOTE_URL = "http://research-server:8899"
```

生产环境推荐把 remote server 交给 systemd、supervisor、Docker、Kubernetes 或运维平台托管。版本文档只展示服务端/客户端分离部署；本机临时启动 server 的 context manager 只适合测试，不代表生产形态。

## 1. 远程计算

远程计算用于把表达式和数据源发送到远端 runtime 执行。它解决的不是“把本地 DataFrame 传来传去”，而是让计算靠近数据：ClickHouse、Parquet 文件、内部行情服务或服务器上的 UDF 数据源可以在远端恢复并执行。

### 典型使用场景

- 数据量大，本机不适合拉取全量数据。
- 数据源只能在内网服务器访问。
- 多个研究进程或 notebook 共享同一个远程计算服务。
- 策略监控需要常驻 runtime，而不是每个 notebook 单独启动。
- 想在客户端组合表达式，但让重计算在远端完成。

### 示例：客户端连接独立 remote server 并执行表达式

```python
import polars as pl
import qust as qs
from qust import col

data = pl.DataFrame({
    "a": [1, 2, 3],
    "b": [10, 20, 30],
})

expr = col(
    (col("a") + col("b")).alias("sum_ab"),
    (col("a") * col("b")).alias("mul_ab"),
)

REMOTE_URL = "http://research-server:8899"

remote_expr = expr.remote(REMOTE_URL, data)
out = remote_expr.runtime().calc_data(None)

print(out)
```

这里 `expr.remote(...)` 返回的仍然是普通表达式，所以可以继续组合、alias、select 或和本地表达式拼接。

### 示例：远程结果和本地输入组合

```python
import polars as pl
import qust as qs
from qust import col

local_data = pl.DataFrame({
    "a": [5, 6, 7],
    "local_weight": [0.1, 0.2, 0.3],
})

remote_data = pl.DataFrame({
    "x": [1, 2, 3],
    "y": [10, 20, 30],
})

remote_expr = col(
    (col("x") + col("y")).mean().alias("remote_mean")
)

REMOTE_URL = "http://research-server:8899"

e = col(
    col("a"),
    col("local_weight"),
    remote_expr.remote(REMOTE_URL, remote_data),
)
out = e.runtime().calc_data(local_data)

print(out)
```

这种模式适合客户端 notebook 做轻量控制，远端负责读大数据或跑重计算。

## 2. Join 能力

0.9.0 强化了 join 在研究表达式中的位置。Join 不再只是 DataFrame 外部的一步预处理，而是可以进入表达式图，和窗口计算、过滤、下推、远程执行、流式计算一起组合。

当前表达式层重点覆盖以下 join 形态：

- `asof_join`：适合 trades/quotes、行情/因子、信号/成交这类按时间找最近可用记录的场景。
- `stream_join`：适合有明确时间列和状态保留窗口的流式等值 join。
- `interval_join`：适合按时间区间匹配，例如事件发生前后 N 秒内的行情或成交。
- `window_join`：适合按滚动、跳跃或会话窗口聚合后匹配。
- `temporal_join`：适合带时间语义的状态表匹配。
- `lookup_join`：适合右表是维表或最新状态表的场景，例如账户状态、风控阈值、合约元信息。

### 常见 join 场景

- trades 与 quotes 做 asof join，计算成交时刻附近的盘口。
- 因子表与行情表按 `date/code` join，做 IC、分层收益和事件分析。
- 信号表与成交表 join，分析信号触发后的真实执行。
- 持仓表与风控阈值表 join，做实时风险监控。
- 多数据源 join，例如本地参数表 + 远端 ClickHouse 行情表。

### 示例：多输入 DataSet join

```python
import polars as pl
import qust as qs
from qust import col

trades = pl.DataFrame({
    "time": [1, 3, 5],
    "ticker": ["A", "A", "B"],
    "price": [10.1, 10.4, 20.2],
})

quotes = pl.DataFrame({
    "time": [1, 2, 3, 4, 5],
    "ticker": ["A", "A", "A", "B", "B"],
    "bid": [10.0, 10.1, 10.3, 20.0, 20.1],
    "ask": [10.2, 10.3, 10.5, 20.2, 20.4],
})

trade_src = qs.DataSource("trades").select(col("time", "ticker", "price"))
quote_src = qs.DataSource("quotes").select(col("time", "ticker", "bid", "ask"))

expr = trade_src.asof_join(
    quote_src,
    on="time",
    by="ticker",
).with_cols(
    ((col("ask") + col("bid")) / 2.0).alias("mid"),
    (col("price") - ((col("ask") + col("bid")) / 2.0)).alias("slippage"),
)

out = expr.runtime().calc_data(
    qs.DataSet(trades=trades, quotes=quotes)
)

print(out)
```

这个例子适合分析成交价格相对盘口中间价的偏离。

### 示例：因子表和行情表 asof join

```python
import polars as pl
import qust as qs
from qust import col

factor = pl.DataFrame({
    "date": [20240101, 20240101, 20240102],
    "code": ["A", "B", "A"],
    "factor": [0.8, -0.2, 1.1],
})

price = pl.DataFrame({
    "date": [20240101, 20240102, 20240102],
    "code": ["A", "A", "B"],
    "close": [10.0, 10.5, 20.1],
})

price_src = qs.DataSource("price").select(col("date", "code", "close"))
factor_src = qs.DataSource("factor").select(col("date", "code", "factor"))

expr = price_src.asof_join(
    factor_src,
    on="date",
    by="code",
    strategy="backward",
).select(
    "date",
    "code",
    col("close").alias("price"),
    "factor",
)

out = expr.runtime().calc_data(qs.DataSet(price=price, factor=factor))
print(out)
```

这种写法适合将“最新已发布因子”贴到行情行上，避免在研究脚本里手工对齐发布日期、股票代码和行情日期。

## 3. 流计算与流式 join

流计算适合持续到达的数据，例如实时行情、逐批成交、分钟级信号、在线风控状态。Qust 的流式执行保留表达式模型，使用批次输入推进运行时状态。

### 使用场景

- 逐批处理行情数据并更新 monitor。
- trades/quotes 流式 asof join。
- 实时计算策略状态、持仓状态、风控阈值。
- 对大量策略做统一实时监控。

### 示例：分批输入并 flush

```python
import polars as pl
import qust as qs
from qust import col

expr = col(
    col("price").mean().expanding().alias("running_mean"),
    col("price").std().expanding().alias("running_std"),
)

runtime = expr.runtime()

batch1 = pl.DataFrame({"price": [10.0, 10.2, 10.3]})
batch2 = pl.DataFrame({"price": [10.1, 10.5]})

print(runtime.calc_stream(batch1))
print(runtime.calc_stream(batch2))
print(runtime.calc_stream(qs.DataSet(main=pl.DataFrame({"price": []})).flush()))
```

实际私有化部署时，输入批次可以来自行情服务、消息队列、ClickHouse 增量查询或内部数据接口。

## 4. 数据源体系

0.9.0 的目标是让不同来源的数据进入同一套表达式系统，而不是在每种数据源上写一套不同的处理逻辑。

支持的常见数据来源包括：

- `polars.DataFrame`
- `qs.DataSet(...)` 多输入数据集
- Parquet datasource
- ClickHouse datasource
- Python UDF datasource
- 远程 datasource
- 流式批次输入

### 示例：Polars DataFrame

```python
import polars as pl
from qust import col

data = pl.DataFrame({
    "close": [10.0, 10.2, 10.1, 10.6],
    "volume": [100, 120, 90, 150],
})

expr = col(
    col("close").mean().rolling(3).alias("ma3"),
    col("volume").sum().rolling(3).alias("vol3"),
)

out = expr.runtime().calc_data(data)
print(out)
```

### 示例：Parquet datasource

```python
import qust.datasource as qds
from qust import col

source = qds.read_parquet(
    "./data/kline.parquet",
    chunk_size=4096,
)

expr = col(
    col("datetime"),
    col("code"),
    col("close"),
).filter(
    col("close") > 0
)

out = expr.runtime().calc_data(source)
print(out)
```

Parquet datasource 适合本地或服务器上的大文件研究。和远程计算结合时，文件路径按远端 server 的工作目录解释。

## 5. ClickHouse 数据源

ClickHouse 是 0.9.0 的重点数据源之一，适合把大规模行情、因子、成交、信号和回测样本放在列式数据库里，然后用 Qust 表达式触发 projection/filter pushdown。

### 使用场景

- 日内行情和 tick 数据量太大，不适合拉到本地再过滤。
- 因子库或策略样本库已经在 ClickHouse 中。
- 多个研究进程、服务或 notebook 共享同一套数据库。
- 远程计算服务和 ClickHouse 在同一内网，减少数据搬运。
- 需要把表达式下推成更少列、更少行的读取。

### 示例：读取 ClickHouse 表

```python
import qust as qs
import qust.datasource as qds
from qust import col

ch = qds.ChConfig("http://clickhouse-host:8123", "default", "password")
source = ch.as_datasource(
    "market",
    "kline_1m",
    chunk_size=10000,
)

expr = col(
    "datetime",
    "code",
    "open",
    "high",
    "low",
    "close",
    "volume",
).filter(
    (col("code") == col.lit("IF2406")) &
    (col("datetime") >= col.lit("2024-01-01"))
)

out = expr.runtime().calc_data(source)
print(out)
```

这个例子只选择需要的列，并加上过滤条件，适合触发 projection/filter pushdown。

### 示例：ClickHouse + 远程计算

```python
import qust as qs
import qust.datasource as qds
from qust import col

ch = qds.ChConfig("http://clickhouse-host:8123", "default", "password")
source = ch.as_datasource("market", "kline_1m", chunk_size=20000)

expr = col(
    col("datetime"),
    col("code"),
    col("close").mean().rolling(20).over("code").alias("ma20"),
    col("close").std().rolling(20).over("code").alias("std20"),
).filter(
    col("datetime") >= col.lit("2024-01-01")
)

REMOTE_URL = "http://research-server:8899"

remote_expr = expr.remote(REMOTE_URL, source, mode="stream")
out = remote_expr.runtime().calc_data(None)

print(out)
```

这种模式适合把计算放在接近 ClickHouse 的服务器上，客户端只保留表达式编写和结果查看。`source` 会作为 remote 表达式的数据源描述发送到服务端，服务端按自己的网络、权限和工作目录访问 ClickHouse。

### 示例：ClickHouse 查询源

如果需要先通过 SQL 定义基础范围，也可以把查询作为 datasource，再继续用 Qust 表达式做投影、过滤和计算：

```python
import qust as qs
import qust.datasource as qds
from qust import col

source = qds.clickhouse_query(
    """
    SELECT date, code, factor, close
    FROM factor_daily
    WHERE date >= '2024-01-01'
    """,
    url="http://clickhouse-host:8123",
    database="research",
    username="default",
    password="password",
    chunk_size=50000,
)

expr = col(
    "date",
    "code",
    "factor",
    col("close").alias("price"),
).with_cols(
    col("factor").batch.rank().over("date").alias("factor_rank")
)

out = expr.runtime().calc_data(source)
print(out)
```

建议只把非常稳定的基础范围写在 SQL 中，研究逻辑尽量保留在 Qust 表达式里，便于复用和审计。

## 6. 策略分析和实时监控

Monitor 在 0.9.0 中的定位更明确：它不是普通画图工具，而是用于分析策略和实时监控策略。

### 使用场景

- 回测后查看净值、回撤、交易点、持仓周期、收益分布。
- 参数变化后重新计算并刷新策略分析页面。
- 在散点或 K 线上选择异常交易，回查原始行情和信号。
- 监控大量策略的实时运行状态、异常信号和关键指标。

### 示例：信号策略分析

```python
import qust as qs
from qust import col
from qust import stra_assets

monitor = qs.Monitor(background="black")
strategies = stra_assets.get_all_strategy_exprs()

expr = col.with_cols(
    pms(strategies).as_expr().over("ticker")
).select(
    col.all.bt.sig_stra_analysis(monitor)
)

runtime = expr.runtime()
runtime.plot(data, monitor)
```

策略分析通常适合在研究阶段使用；实时监控则适合私有化部署时把运行状态持续写入 monitor。

## 7. 大量策略运行与管理

0.9.0 的能力组合面向大量策略运行场景。可以用统一表达式组织策略，配合远程计算、ClickHouse、流式输入和 monitor，把大量策略的运行结果统一管理。

### 使用场景

- 批量运行几百到上万条策略表达式。
- 对不同参数版本做统一回测和结果归档。
- 对大量策略做实时状态监控。
- 发现异常策略后，通过 monitor 回查样本。
- 在私有化环境里扩展自定义策略算子和数据源。

### 示例：批量构建策略表达式

```python
import qust as qs
from qust import col

windows = [10, 20, 40, 80]
thresholds = [1.0, 1.5, 2.0]

exprs = []
for window in windows:
    for threshold in thresholds:
        signal = (
            (col("close") - col("close").mean().rolling(window))
            / col("close").std().rolling(window)
        )
        exprs.append(
            col(
                (signal > threshold).alias(f"open_long_w{window}_t{threshold}"),
                (signal < -threshold).alias(f"open_short_w{window}_t{threshold}"),
            )
        )

runtime = col(*exprs).runtime()
out = runtime.calc_data(data)
print(out)
```

在私有化部署中，这类表达式可以进一步包装成策略模板，并接入远程计算和统一 monitor。

## 升级建议

1. 将 `qust` 升级到 `0.9.0`。
2. 先用本地 DataFrame 验证表达式语义。
3. 对大数据表切换到 Parquet 或 ClickHouse datasource。
4. 对服务器数据和共享计算切换到 remote。
5. 对实时数据使用 stream/update 执行模式。
6. 对策略分析和实时策略监控统一接入 monitor。
7. 私有化部署时优先封装常用算子和数据源，避免重复写胶水代码。
