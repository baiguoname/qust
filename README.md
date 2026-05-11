# Qust / Otters

Qust 是一个面向量化研究、因子分析、策略回测、组合分析和交互式调参的高性能计算框架。

你可以把它理解成：**用 Python 写 DataFrame 风格表达式，用 Rust 底层执行有状态的流式计算，再用 Monitor 和 Wasm 把结果变成可交互、可分享的研究环境。**

Qust 的 Python 包名是 `qust`；Otters 是当前底层表达式执行、Monitor、Wasm 和 plot runtime 的核心项目名。对普通用户来说，主要入口仍然是：

```python
import qust as qs
from qust import col, pms, Monitor
```

## 在线入口

| 内容 | 地址 |
| --- | --- |
| 在线文档 | https://baiguoname.github.io/qust/examples/docs/ |
| Wasm 在线运行 | https://baiguoname.github.io/qust/examples/wasm/ |
| Wasm 使用说明 | https://baiguoname.github.io/qust/examples/docs/wasm.html |
| 算子索引 | https://baiguoname.github.io/qust/examples/docs/operators/ |
| GitHub 仓库 | https://github.com/baiguoname/qust |

如果你不想安装本地环境，可以直接打开 Wasm 页面：

```text
https://baiguoname.github.io/qust/examples/wasm/
```

这个页面可以在浏览器中运行 Qust 示例、加载数据、写表达式、调参数和查看 Monitor 图表。

## Qust 解决什么问题

量化研究里经常会遇到一个矛盾：

- DataFrame 写起来很舒服，但很多策略和指标本质上是有状态的。
- 事件驱动模型更接近实盘，但写研究代码不够直观。
- 回测、实盘、调参、图表和分享，经常变成几套不同代码。

Qust 想做的是把这些体验统一起来：

- 像 Polars / Pandas 一样写表达式。
- 像事件流一样保留 rolling、expanding、over、分组、状态和上下文。
- 用 Rust 执行核心计算，减少 Python 循环。
- 本地 Python、Notebook、Wasm 页面和 Monitor 尽量使用同一套表达式。
- 回测、因子分析、图表、参数调优不再割裂。

## 核心特点

### 1. Python 表达式 API

Qust 的用户 API 接近 DataFrame 表达式系统：

```python
import qust as qs
from qust import col

df = qs.with_cols(
    col("factor").mean().rolling(20).over("code").alias("factor_ma20"),
    col("price").pct_change().alias("ret"),
)
```

表达式不是马上执行的 Python 循环，而是一份可以被保存、组合、调参、绘图和重复运行的计算计划。

### 2. 有状态流式计算

同一个 `DataFrame` 执行计划可以连续接收多批数据。rolling、expanding、累计统计、分组状态等都可以保留上下文：

```python
result1 = df.calc_data(data_1)
result2 = df.calc_data(data_2)
```

这对行情流、tick、分钟线、日频截面、实时信号和回测都很重要。

### 3. 分组上下文 `over`

量化数据通常天然分组，例如：

- 按股票代码 `code` 分组。
- 按交易日 `date` 做截面。
- 按策略、账户、品种、合约分组。

Qust 用 `over(...)` 表示分组上下文：

```python
df = qs.with_cols(
    col("close").mean().rolling(20).over("code").alias("ma20"),
    col("factor").rank().over("date").alias("rank_cs"),
)
```

时间序列和截面语义可以清楚地区分：

- `rolling(...).over("code")`：每只股票自己的时间序列窗口。
- `rank().over("date")`：每个交易日的横截面排序。

### 4. rolling / expanding / batch

Qust 支持常见状态型计算：

- `rolling(n)`：固定窗口。
- `expanding()`：从开始到当前的扩展窗口。
- `over(...)`：分组状态。
- `batch`：当前 batch / 当前截面上的计算。

示例：

```python
df = qs.with_cols(
    col("ret").mean().rolling(60).over("code").alias("ret_mean_60"),
    col("ret").std().rolling(60).over("code").alias("ret_std_60"),
    col("factor").rank().over("date").alias("factor_rank"),
)
```

### 5. Rust 底层执行

用户写 Python，但核心计算尽量在 Rust 侧完成。这样做的好处是：

- 避免大量 Python for-loop。
- 有状态算子可以长期保存内部状态。
- 多批数据执行时，不需要每次从头算。
- 更适合后续接入实时数据、回测和自动化任务。

### 6. 与 Polars 互补

Qust 不试图替代 Polars。它更像是一个面向状态计算和量化语义的表达式层。

你仍然可以在 Python 里使用 Polars 数据：

```python
import polars as pl
import qust as qs
from qust import col

data = pl.DataFrame({
    "code": ["A", "A", "B", "B"],
    "price": [10.0, 10.5, 20.0, 20.3],
})

df = qs.with_cols(
    col("price").mean().expanding().over("code").alias("price_mean")
)

df.calc_data(data)
```

当你的需求是一次性批处理、普通 SQL / DataFrame 变换时，Polars 很好；当你的需求涉及流式状态、分组上下文、策略状态、Monitor 调参和 Wasm 分享时，Qust 更适合承载这部分语义。

#### Qust 里使用 Polars 表达式

Qust 可以直接接收 `polars.Expr`。这适合把普通列变换交给 Polars，把有状态、分组、窗口、Monitor 和参数交给 Qust。

```python
import polars as pl
import qust as qs
from qust import col

data = pl.DataFrame({
    "value": [1, 2, 3, 4, 5],
})

df = qs.with_cols(
    # 直接把 Polars Expr 放进 Qust 计划
    (pl.col("value") + 1).alias("value_plus_1"),

    # Qust Expr 可以用 .pl 临时转成 Polars Expr
    (col("value").pl + 2).alias("value_plus_2"),

    # Qust 继续负责有状态计算
    col("value").mean().expanding().alias("value_expanding_mean"),
)

out = df.calc_data(data)
```

这条路径适合：

- 已经有一段 Polars 表达式，不想重写。
- 普通列运算用 Polars，更复杂的 rolling / over 状态用 Qust。
- 需要把 Polars 算出来的列继续接 Qust 的 Monitor 或参数。

需要注意：Polars 的 `over`、rolling、rank 等仍然是 Polars 自己的语义；Qust 的 `over(...)`、`rolling(...)` 是 Qust 的流式状态语义。两者可以混用，但不要把它们当成完全相同的东西。

#### Polars 里使用 Qust 表达式

Qust 表达式可以通过 `.pl` 转成 `polars.Expr`，放进 Polars 的 `select` / `with_columns`。

```python
import polars as pl
import qust as qs
from qust import col

data = pl.DataFrame({
    "value": [1, 2, 3, 4, 5],
})

out = data.select(
    # Qust rolling mean 转成 Polars Expr 执行
    col("value").mean().rolling(3).alias("value_mean_qs").pl,

    # 也可以把 Qust 的列表达式转成 Polars Expr 后继续写 Polars 方法
    col("value").pl.rolling_mean(3).alias("value_mean_pl"),
)
```

这条路径适合：

- 主流程已经是 Polars，只想嵌入一个 Qust 单列表达式。
- 做一次性 `select` / `with_columns`。
- 快速比较 Polars 写法和 Qust 写法。

如果你想让 Polars DataFrame 更方便地调用 Qust，可以开启命名空间桥接：

```python
import qust as qs
from qust import col

qs.enable_polars_namespace()

out = data.qs.with_cols(
    col("value").mean().rolling(3).alias("value_mean")
)

out2 = data.qs.select(
    col("value").sum().expanding().alias("value_sum")
)
```

也可以把已经构造好的 Qust `DataFrame` 计划应用到 Polars 数据上：

```python
df = qs.with_cols(
    col("value").mean().rolling(3).alias("value_mean")
)

out = data.qs.df(df)
```

#### 有状态 Qust 表达式嵌入 Polars 时的提醒

Qust 的强项是“执行计划 + 状态”。如果你把 Qust 表达式转成单个 `polars.Expr`，它就会被放进 Polars 的执行流程里，适合简单嵌入；但如果你需要跨多次输入保留状态，更推荐用 Qust 的 `DataFrame` 计划：

```python
df = qs.with_cols(
    col("value").mean().rolling(3).alias("value_mean")
)

out1 = df.calc_data(data_1)
out2 = df.calc_data(data_2)
```

如果确实要在 Polars 里复用某个有状态 Qust 表达式，可以给它一个 cache id：

```python
e = col("value").mean().rolling(3).cache("value_mean_rolling_3")

out = data.select(
    e.alias("value_mean").pl
)

qs.clear_cache("value_mean_rolling_3")
```

有两个经验规则：

- 需要 Qust 的分组语义时，先写 `e.over("code")`，再转 `.pl`；不要转成 Polars 后再接 Polars 的 `.over(...)`。
- 如果一个 Qust 表达式会返回多列，优先用 `qs.select(...)`、`qs.with_cols(...)` 或 `data.qs.select(...)`，不要强塞进单个 Polars 表达式槽里。

#### 选择哪条路

| 场景 | 推荐写法 |
| --- | --- |
| 普通列变换 | Polars |
| 有状态 rolling / expanding | Qust |
| 分组状态计算 | Qust `over(...)` |
| 主流程是 Qust，少量普通表达式 | 在 Qust 中直接传 `pl.Expr` |
| 主流程是 Polars，嵌入单个 Qust 表达式 | `qust_expr.pl` |
| Polars DataFrame 直接跑 Qust 计划 | `data.qs.with_cols(...)` / `data.qs.select(...)` |
| 多批数据连续计算 | Qust `DataFrame.calc_data(...)` |
| 多列返回、Monitor、参数联动 | Qust 计划 |

### 7. Monitor 交互式可视化

Qust 内置 Monitor，用来展示图表、表格、参数面板和 callback。

典型用法：

```python
import qust as qs
from qust import col, Monitor

monitor = Monitor(background="white")

df = qs.with_cols(
    col("date", "equity")
        .monitor
        .line(title="equity")
        .attach_to_monitor(monitor)
)

monitor.plot(df, data)
```

Monitor 适合：

- 查看净值曲线。
- 分析因子表现。
- 展示回测结果。
- 观察参数变化后的图表刷新。
- 点击图表后钻取局部明细。

### 8. 参数系统 `pms`

Qust 的参数不是简单 Python 变量，而是可以进入表达式、进入 Monitor、进入 Wasm 的可调对象。

```python
from qust import col, pms

window = pms.int(5, 120, default=20, step=5).alias("window")

df = qs.with_cols(
    col("close").mean().rolling(window).over("code").alias("ma")
)
```

参数系统适合：

- 策略参数调优。
- 因子窗口扫描。
- Monitor 参数面板。
- Wasm 页面交互式试验。
- 把固定数字替换成可控范围。

### 9. callback 下钻交互

Monitor 支持 callback，让图表不只是静态结果，而是继续分析的入口。

常见交互：

- `x_slider`：在图上框选一个 x 区间，重新计算局部视图。
- `scatter_select`：点击 scatter 上的某个点，查看这个点对应的明细。

适合的场景：

- 净值曲线上选中一段回撤，看那段时间的交易。
- 点击某笔交易，弹出这笔交易附近的 kline。
- 点击异常因子点，查看该股票当天的上下文。
- 从总览图钻到局部细节图。

### 10. Wasm / Pyodide 在线运行

Wasm 页面是 Qust 很重要的能力。打开下面链接即可试用：

```text
https://baiguoname.github.io/qust/examples/wasm/
```

Wasm 页面可以：

- 在浏览器中运行 Qust。
- 选择内置示例。
- 上传或加载 parquet 数据。
- 写 Python 表达式。
- 查看表格结果。
- 打开 Monitor 图表。
- 调参数。
- 分享 demo 给别人。

本地 Python 更适合大数据和正式研究；Wasm 更适合演示、教学、分享和快速体验。

### 11. 因子分析和 alphalen

Qust 可以把因子分析流程组织成表达式：

```python
import qust as qs
from qust import col, Monitor

monitor = Monitor(background="white")

df = qs.with_cols(
    col("date", "code", "factor", "price")
        .alpha()
        .alphalen_analysis(monitor)
)

monitor.plot(df, data)
```

这类功能适合：

- 因子分组。
- 多持有期收益。
- IC / rank IC。
- quantile 收益。
- 换手、自相关。
- 图表化展示。

重点是：它仍然是 Qust 表达式的一部分，所以可以接参数、接 Monitor、在 Wasm 中演示。

### 12. snapshot_by

股票截面数据经常不是一次到齐的。比如同一个交易日，先来了 A/B 两只股票，后面又补进 C。如果第一次就按不完整截面算 rank，后面结果可能需要修正。

`snapshot_by` 用来处理这种“分批、回流、补数”的场景：

```python
df = qs.with_cols(
    col("factor")
        .rank()
        .over("date")
        .alias("rank_cs")
).snapshot_by("date", keep_labels=256, need_active=True)
```

它的目标不是让单次计算更快，而是让流式分批结果尽量接近一次性全量计算的语义。

### 13. 金融领域算子

Qust / Otters 的方向不是只做通用 DataFrame，而是把常见金融研究语义沉淀成算子：

- K 线合成。
- tick / kline 处理。
- 回测。
- 交易信号。
- 持仓转换。
- 因子分析。
- OLS / residual。
- 组合分析。
- 未来可接入更多 portfolio optimization / risk model / attribution 能力。

这些能力应该尽量保持为表达式，而不是散落成一次性的 Python 脚本。

## 安装

本地 Python 使用：

```bash
pip install qust
```

如果网络访问慢，可以使用镜像：

```bash
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple qust
```

常用依赖：

```python
import qust as qs
from qust import col, pms, Monitor
import polars as pl
```

如果只是体验或演示，可以不安装，直接使用 Wasm：

```text
https://baiguoname.github.io/qust/examples/wasm/
```

## 快速开始

### 1. 构造数据

```python
import numpy as np
import polars as pl
import qust as qs
from qust import col

n = 20
data = pl.DataFrame({
    "date": np.repeat(["2024-01-01", "2024-01-02"], n // 2),
    "code": np.random.choice(["A", "B", "C"], size=n),
    "factor": np.random.randn(n),
    "price": 100 + np.random.randn(n).cumsum(),
})
```

### 2. 写表达式

```python
df = qs.with_cols(
    col("factor").rank().over("date").alias("factor_rank"),
    col("price").mean().rolling(5).over("code").alias("price_ma5"),
)
```

### 3. 执行

```python
result = df.calc_data(data)
print(result)
```

### 4. 加参数

```python
from qust import pms

window = pms.int(3, 60, default=20, step=1).alias("window")

df = qs.with_cols(
    col("price").mean().rolling(window).over("code").alias("ma")
)
```

### 5. 加 Monitor

```python
from qust import Monitor

monitor = Monitor(background="white")

df = qs.with_cols(
    col("date", "price")
        .monitor
        .line(title="price")
        .attach_to_monitor(monitor)
)

monitor.plot(df, data)
```

## 常用写法

### 选择列

```python
qs.select(
    col("date"),
    col("code"),
    col("price"),
)
```

### 新增列

```python
qs.with_cols(
    col("price").pct_change().alias("ret"),
    col("ret").mean().rolling(20).over("code").alias("ret_ma20"),
)
```

### 分组累计

```python
qs.with_cols(
    col("ret").sum().expanding().over("code").alias("cum_ret")
)
```

### 截面排序

```python
qs.with_cols(
    col("factor").rank().over("date").alias("rank")
)
```

### 多表达式组合

```python
qs.with_cols(
    (col("close") / col("open") - 1).alias("intraday_ret"),
    (col("high") - col("low")).alias("range"),
)
```

### 常量

```python
qs.with_cols(
    (col("price") > col.lit(100)).alias("above_100")
)
```

如果要表达数字常量，推荐用 `col.lit(...)`，避免和列索引等语义混淆。

## Wasm 使用提醒

Wasm 页面地址：

```text
https://baiguoname.github.io/qust/examples/wasm/
```

Wasm 里最需要注意的是数据来源：

```python
from qust import load_data

# 页面数据池中已经存在 demo.parquet
data = load_data("demo.parquet")

# 从远程 URL 读取
data = load_data("https://example.com/demo.parquet", name="demo")
```

浏览器不能像本地 Python 一样随意读取 `/home/user/data.parquet`。如果是本地文件，通常需要通过页面上传；如果是远程数据，需要是浏览器能访问的 HTTP/HTTPS 地址。

GitHub Pages 是静态环境，部分浏览器下多线程 Wasm 可能受限制。首次加载 wheel、Pyodide 和 wasm 文件需要一些时间，这是正常现象。

## 适合的场景

Qust 适合：

- 因子研究。
- 截面排序。
- 时间序列滚动指标。
- 多资产分组状态计算。
- tick / kline 数据处理。
- 策略信号生成。
- 回测结果分析。
- 交互式调参。
- Monitor dashboard。
- 浏览器中分享研究 demo。
- 把一次性 Python 分析流程沉淀成可复用表达式。

不一定适合：

- 只做一次性小表格清洗。
- 完全不需要状态的普通 ETL。
- 极端依赖大型 Python 第三方库且必须在 Wasm 中运行的任务。
- 只想用静态图片，不需要参数和交互的简单画图。

## 与传统写法的区别

传统 Python 研究代码经常这样写：

```python
for code, sub_df in data.group_by("code"):
    # 手写 rolling、状态、过滤、画图
    ...
```

这种方式短期快，但后面会遇到问题：

- 逻辑散落在 Python 循环里。
- 不容易接 Monitor。
- 不容易做参数重算。
- 不容易放到 Wasm。
- 分批数据和全量数据语义容易不一致。
- 回测和实盘容易变成两套代码。

Qust 更推荐把计算写成表达式：

```python
df = qs.with_cols(
    col("factor").rank().over("date").alias("rank"),
    col("price").mean().rolling(20).over("code").alias("ma20"),
)
```

表达式保留下来了，后续才能统一执行、调参、可视化和分享。

## 文档

完整文档请看：

```text
https://baiguoname.github.io/qust/examples/docs/
```

重点页面：

- 从零上手：https://baiguoname.github.io/qust/examples/docs/
- Wasm：https://baiguoname.github.io/qust/examples/docs/wasm.html
- 算子索引：https://baiguoname.github.io/qust/examples/docs/operators/

## 项目目标

Qust 的长期目标是让量化研究流程更统一：

- 研究时写表达式。
- 回测时复用表达式。
- 调参时复用表达式和参数定义。
- 图表时复用表达式和 Monitor。
- 浏览器演示时复用表达式和 Wasm。
- 后续自动化、实盘或服务化时继续复用表达式。

也就是说，Qust 不只是“再写一个 DataFrame 库”，而是希望把策略研究里的计算、状态、参数、图表和分享放到同一条路径上。

## 常见问题

### Qust 和 Polars 是什么关系？

Qust 可以和 Polars 一起用。Polars 很适合通用 DataFrame 计算；Qust 更强调状态、表达式计划、Monitor、参数和 Wasm。

### 为什么要用表达式？

因为表达式可以被保存、组合、序列化、调参、重算和放到浏览器里执行。如果逻辑都写在 Python 循环里，这些能力很难统一。

### Wasm 能完全替代本地 Python 吗？

不能。Wasm 更适合演示、教学、分享和轻量试验。本地 Python 更适合大数据、批量研究和正式任务。

### Monitor 是必须的吗？

不是。你可以只用 `calc_data` 得到结果。Monitor 是为了交互式看图、调参和下钻分析。

### 参数一定要用 `pms` 吗？

普通固定值可以直接写数字。需要调参、Monitor 面板、Wasm 交互或参数扫描时，推荐用 `pms`。

### 为什么有些 Python 写法不推荐？

例如在 Python 里提前展开 live 参数、手写循环生成一堆列、或者把业务计算藏在普通函数里。这些写法短期能跑，但会损失表达式语义，后续就不容易接流式状态、Monitor 和 Wasm。

## 最短路径

如果你第一次使用：

1. 打开在线文档：https://baiguoname.github.io/qust/examples/docs/
2. 打开 Wasm 页面：https://baiguoname.github.io/qust/examples/wasm/
3. 运行一个内置示例。
4. 在本地安装：`pip install qust`。
5. 从 `qs.with_cols(...)` 和 `col(...)` 开始写第一个表达式。
6. 再加入 `over(...)`、`rolling(...)`、`pms` 和 `Monitor`。
