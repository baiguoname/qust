# Qust 0.8.0， alphalen因子分析


## 1. Wasm 页面

当前在线地址：

```text
https://baiguoname.github.io/qust/examples/wasm/
```


做 Wasm 页面主要是为了把 qust 变成一个“打开就能跑”的环境：

1. 不用本地装 Python，打开就能跑。
2. wasm虽然在本地跑，但是在本地浏览器跑，与本地系统隔绝，天然安全
3. 打开页面、选择例子、运行，就能看到表格、图、参数面板和 callback。
4. 对策略研究来说，可以把一段完整 demo 直接给别人，不需要先折腾环境。

缺点：
1. 性能大概在python的80%以下
2. 有的浏览器不支持多线程
3. 有17m传输体积，而且是git上，访问不方便

最简单的使用方式：

```python
import qust as qs
from qust import col, load_data

data = load_data("https://example.com/data.parquet")

df = qs.select(
    col("date", "pnl")
        .monitor
        .line()
)

df.plot(data)
```

注意一点：线上 GitHub Pages 是纯静态环境，不能像本地 `8890` 一样配置
`COOP/COEP` header，所以浏览器可能禁用 `SharedArrayBuffer`，也就是 wasm
多线程不一定能开。这会影响性能，但不影响“打开即用”的核心价值。

## 2. Snapshot 上下文算子

`snapshot_by` 解决的是流式、分批、可能回流的数据更新问题。

流计算，股票一般比期货难，主要是因为这个:
你按 batch 收到数据，但同一个 `date` / `code`
后面可能又来了新行。如果直接按当前 batch 算，很多 rolling、expanding、rank
这类结果会因为上下文不完整而不稳定。

`snapshot_by` 的思路是：按一组标签保存快照。后续同一个标签的数据又来了，就从快照状态继续或回放，尽量保证“分批算”和“一次性全量算”的语义一致。

一个简短例子：

```python
import qust as qs
from qust import col

df = qs.select(
    col.all.with_cols(
        col("factor")
            .batch_ta
            .rank()
            .over("date")
            .alias("rank_cs")
    )
        .snapshot_by("date", keep_labels=256, need_active=True)
)
```

这里的意思是：按 `date` 维护最近一批快照，用来处理股票截面数据分批更新时的
`rank_cs`。`keep_labels` 控制保留多少组标签的快照，值越大越稳，但内存也会多一些。

举个更具体的例子。假设同一个交易日的数据不是一次来齐，而是先来两只股票，后面又补了一只。

第一次输入：

```text
shape: (2, 3)
┌────────────┬──────┬────────┐
│ date       │ code │ factor │
│ ---        │ ---  │ ---    │
│ str        │ str  │ f64    │
╞════════════╪══════╪════════╡
│ 2024-01-02 │ A    │ 0.30   │
│ 2024-01-02 │ B    │ 0.10   │
└────────────┴──────┴────────┘
```

如果不用 `snapshot_by`，第一次只能按已经看到的 A/B 算截面 rank：

```text
shape: (2, 4)
┌────────────┬──────┬────────┬─────────┐
│ date       │ code │ factor │ rank_cs │
│ ---        │ ---  │ ---    │ ---     │
│ str        │ str  │ f64    │ f64     │
╞════════════╪══════╪════════╪═════════╡
│ 2024-01-02 │ A    │ 0.30   │ 2.0     │
│ 2024-01-02 │ B    │ 0.10   │ 1.0     │
└────────────┴──────┴────────┴─────────┘
```

第二次又补进来同一天的 C：

```text
shape: (1, 3)
┌────────────┬──────┬────────┐
│ date       │ code │ factor │
│ ---        │ ---  │ ---    │
│ str        │ str  │ f64    │
╞════════════╪══════╪════════╡
│ 2024-01-02 │ C    │ 0.20   │
└────────────┴──────┴────────┘
```

不用 `snapshot_by` 的问题是：第二次通常只会对新来的 C 走当前 batch 计算，A/B
之前那个“不完整截面”的结果不会自然修正：

```text
shape: (1, 4)
┌────────────┬──────┬────────┬─────────┐
│ date       │ code │ factor │ rank_cs │
│ ---        │ ---  │ ---    │ ---     │
│ str        │ str  │ f64    │ f64     │
╞════════════╪══════╪════════╪═════════╡
│ 2024-01-02 │ C    │ 0.20   │ 1.0     │
└────────────┴──────┴────────┴─────────┘
```

用了 `snapshot_by("date")` 后，第二次发现 `2024-01-02` 这个标签回流，会从快照后
的历史重放，得到这个交易日当前已知的完整截面：

```text
shape: (3, 4)
┌────────────┬──────┬────────┬─────────┐
│ date       │ code │ factor │ rank_cs │
│ ---        │ ---  │ ---    │ ---     │
│ str        │ str  │ f64    │ f64     │
╞════════════╪══════╪════════╪═════════╡
│ 2024-01-02 │ A    │ 0.30   │ 3.0     │
│ 2024-01-02 │ B    │ 0.10   │ 1.0     │
│ 2024-01-02 │ C    │ 0.20   │ 2.0     │
└────────────┴──────┴────────┴─────────┘
```

这个例子里，`snapshot_by` 的价值不是让计算更快，而是让“后面补来的同一天股票”
可以把前面不完整的结果校正回来。

所有算子都支持snapshot by上下文算子

## 3. alphalen_analysis

用codex翻译的`alphalen`

`alphalen_analysis` 就是一个普通算子 recipe，没有特殊身份。

它做的事情是把常见的因子分析流程串起来，比如：

1. 因子分组。
2. 不同持有期收益。
3. quantile 表现。
4. IC、IC 分布、换手、自相关等图。
5. 把这些结果挂到一个 monitor 里展示。

典型用法：

```python
import qust as qs
from qust import col

monitor = qs.Monitor(background="white")

df = qs.with_cols(
    col("date", "code", "factor", "price")
        .alpha()
        .alphalen_analysis(monitor)
)

df.plot(data, monitor)
```

要注意的是：它返回的仍然是 `Expr`。它不是 dashboard 对象，也不是一个单独运行时。
它只是把一长串表达式和 monitor plot 帮你组织好，最后仍然走 qust 的正常表达式执行和 monitor 绘图。

所以以后扩展 alphalen，也应该按“扩展表达式 recipe”的方式做，而不是在里面塞一套特殊框架。

## 4. Callback

callback 是 monitor 里很重要的一层交互能力。

简单说，它让图不只是“展示结果”，还可以变成“继续提问的入口”：

1. `x_slider`：在主图上选一个 x 区间，然后用这个区间的数据画子图或表格。
2. `scatter_select`：点中 scatter 上的一个点，然后按这个点携带的信息去过滤 source，弹出更细的图。
3. `col.null.x_slider()` / `col.null.scatter_select(...)`：可以先弹输入框，让用户临时写 callback。

比如一个交易散点图，每个点是一笔交易。点一下某个交易，就可以弹出这笔交易期间的
kline、均线、信号表。这比“先画总图，再手动查日期、再重新写代码过滤”要顺很多。

callback 的优势主要在这里：

1. 从概览图直接钻到细节图。
2. 保留表达式语义，不需要前端手写业务逻辑。
3. 图、参数、source、filter 可以组合，后面能做 linked chart、brush、query、hover drilldown。
4. 很适合策略分析：先看全局分布，再点某个异常点看局部行情。

当前 callback 还在继续演进，但方向很明确：它不是临时按钮，而是 monitor 的交互扩展点。后面 hover、zoom、select、query、slider 都应该走统一的 trait/interface，而不是每种图各自打补丁。
