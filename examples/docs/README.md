# Otters / Qust 文档

这里是面向用户的静态文档站点，入口是 `index.html`。文档视觉和叙事目标是专业金融科技风格：先说明研究链路价值，再给表达式、因子、Monitor 和 runtime 的可验证细节。

文档重点解释三件事：

- 为什么 Qust 用表达式来描述计算，而不是让用户手写循环。
- 每类算子能解决什么问题、输入输出是什么、应该怎么组合。
- Monitor、Wasm、参数和 callback 为什么要和表达式打通。
- Qust 相对普通脚本化 DataFrame 工作流的优势：表达式资产化、批流一致、Monitor 交互、Rust runtime、remote expr 和 Wasm 分享路径。
- 因子分析的专业解释：AlphaLen 图形诊断、分位数组合、IC 稳定性、horizon 衰减和事件回查。

主要入口：

- `index.html`：从零上手、表达式、窗口、Monitor、Wasm、callback、排错。
- `operators/index.html`：全部算子总览。
- `operators/<namespace>/<operator>.html`：每个算子的独立页面，包含用法、参数、输入输出、设计动机和完整示例。
- `wasm.html`：浏览器运行 Qust 的说明。

图文资产：

- `assets/alphalen-fintech-dashboard.png`：Python 生成的 AlphaLen 风格因子诊断面板。
- `assets/expr-execution-pipeline.png`：Python 生成的复杂表达式执行链路图。

维护规范：

- 首页应保持“优势 -> 图形诊断 -> 复杂算子拆解 -> 快速上手”的专业研究叙事，不只做 API 罗列。
- `operators/alpha/alphalen_analysis.html` 必须保留 AlphaLen 深度说明和图片引用。
- 新增图文内容优先放在 `assets/`，文档页面只引用稳定相对路径。

这份目录就是 GitHub Pages 使用的 `qust-py/examples/docs`。
