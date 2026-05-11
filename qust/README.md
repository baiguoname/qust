# qust

`qust` 是项目中的底层包目录之一。普通 Python 用户不需要从这里开始阅读，推荐直接看项目主页和在线文档。

常用入口：

| 内容 | 地址 |
| --- | --- |
| 项目主页 | https://github.com/baiguoname/qust |
| 在线文档 | https://baiguoname.github.io/qust/examples/docs/ |
| Wasm 在线运行 | https://baiguoname.github.io/qust/examples/wasm/ |
| Wasm 使用说明 | https://baiguoname.github.io/qust/examples/docs/wasm.html |

## Qust 是什么

Qust 是一个面向量化研究的表达式计算框架。用户通常在 Python 里使用它：

```python
import qust as qs
from qust import col, pms, Monitor
```

它的特点是：

- 写法接近 DataFrame 表达式。
- 底层由 Rust 执行核心计算。
- 支持有状态流式计算。
- 支持 rolling、expanding、over、batch 等量化常用语义。
- 支持 Monitor 图表和参数调优。
- 支持 Wasm 在线运行。

## 最小 Python 示例

```python
import qust as qs
from qust import col

df = qs.with_cols(
    col("price").mean().rolling(20).over("code").alias("ma20")
)

result = df.calc_data(data)
```

这段代码表达的是：

- 按 `code` 分组。
- 对每个分组里的 `price` 计算 20 窗口均值。
- 新增一列 `ma20`。

## 和 Polars 一起用

Qust 可以直接接收 Polars DataFrame，也可以和 Polars 表达式互相转换：

```python
import polars as pl
import qust as qs
from qust import col

data = pl.DataFrame({"price": [1, 2, 3, 4, 5]})

df = qs.with_cols(
    (pl.col("price") + 1).alias("price_plus_1"),
    col("price").mean().rolling(3).alias("ma3"),
)

out = df.calc_data(data)
```

Qust 表达式也可以用 `.pl` 放进 Polars：

```python
out = data.select(
    col("price").mean().rolling(3).alias("ma3").pl
)
```

如果要从 Polars DataFrame 直接调 Qust：

```python
qs.enable_polars_namespace()

out = data.qs.with_cols(
    col("price").mean().rolling(3).alias("ma3")
)
```

## 为什么底层有 Rust

量化研究里很多计算不是一次性表格变换，而是有状态的：

- rolling 指标。
- expanding 累计统计。
- 按股票、合约、账户分组的状态。
- tick / kline 数据流。
- 回测中的持仓和交易状态。
- 参数变化后的重复计算。

Rust 适合承担这些底层计算；Python 适合提供易用 API。Qust 把这两部分结合起来，让用户写起来像 Python DataFrame，执行时尽量走高性能底层。

## 在线体验

如果你只想试用，不需要安装本地环境：

```text
https://baiguoname.github.io/qust/examples/wasm/
```

Wasm 页面可以直接在浏览器中运行 Qust 示例，适合演示、教学和分享。

## 更多文档

完整用户文档：

```text
https://baiguoname.github.io/qust/examples/docs/
```

如果你在找算子用法，先看：

```text
https://baiguoname.github.io/qust/examples/docs/operators/
```
