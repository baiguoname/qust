# Otters / Qust 文档

这里是面向用户的静态文档站点，入口是 `index.html`。

文档重点解释三件事：

- 为什么 Qust 用表达式来描述计算，而不是让用户手写循环。
- 每类算子能解决什么问题、输入输出是什么、应该怎么组合。
- Monitor、Wasm、参数和 callback 为什么要和表达式打通。

主要入口：

- `index.html`：从零上手、表达式、窗口、Monitor、Wasm、callback、排错。
- `operators/index.html`：全部算子总览。
- `operators/<namespace>/<operator>.html`：每个算子的独立页面，包含用法、参数、输入输出、设计动机和完整示例。
- `wasm.html`：浏览器运行 Qust 的说明。

这份目录会同步到 `qust-py/examples/docs`，用于 GitHub Pages 访问。
