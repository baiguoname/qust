#!/usr/bin/env python3
from __future__ import annotations

import ast
import html
import re
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"
SRC = ROOT / "otters-py" / "python" / "qust"


@dataclass
class ApiItem:
    group: str
    group_title: str
    namespace: str
    title: str
    path: str
    source_file: str
    class_name: str | None
    node: ast.FunctionDef
    is_property: bool
    usage_prefix: str
    kind: str


def read_tree(file_name: str) -> ast.Module:
    return ast.parse((SRC / file_name).read_text(encoding="utf-8-sig"))


def class_node(file_name: str, class_name: str) -> ast.ClassDef:
    found: list[ast.ClassDef] = []
    for node in read_tree(file_name).body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            found.append(node)
    if found:
        return found[-1]
    raise KeyError((file_name, class_name))


def function_nodes(file_name: str) -> list[ast.FunctionDef]:
    return [n for n in read_tree(file_name).body if isinstance(n, ast.FunctionDef)]


def public_methods(
    file_name: str,
    class_name: str,
    *,
    include_call: bool = False,
    include_names: set[str] | None = None,
) -> list[ast.FunctionDef]:
    out: list[ast.FunctionDef] = []
    for node in class_node(file_name, class_name).body:
        if not isinstance(node, ast.FunctionDef):
            continue
        if node.name == "__call__" and include_call:
            out.append(node)
            continue
        if include_names and node.name in include_names:
            out.append(node)
            continue
        if node.name.startswith("_"):
            continue
        out.append(node)
    return out


def is_property(node: ast.FunctionDef) -> bool:
    return any(isinstance(d, ast.Name) and d.id == "property" for d in node.decorator_list)


def slug(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "-", name).strip("-").lower()


def unparse(node: ast.AST | None) -> str:
    if node is None:
        return ""
    return ast.unparse(node)


def format_default(node: ast.AST | None) -> str:
    if node is None:
        return ""
    return ast.unparse(node)


def params_for(node: ast.FunctionDef) -> list[dict[str, str]]:
    args = node.args
    positional = list(args.posonlyargs) + list(args.args)
    defaults = [None] * (len(positional) - len(args.defaults)) + list(args.defaults)
    rows: list[dict[str, str]] = []
    for arg, default in zip(positional, defaults):
        if arg.arg in {"self", "cls"}:
            continue
        rows.append(
            {
                "name": arg.arg,
                "kind": "位置参数",
                "type": unparse(arg.annotation) or "未显式标注",
                "default": format_default(default) or "必填",
            }
        )
    if args.vararg:
        rows.append(
            {
                "name": f"*{args.vararg.arg}",
                "kind": "可变位置参数",
                "type": unparse(args.vararg.annotation) or "任意",
                "default": "可选",
            }
        )
    for arg, default in zip(args.kwonlyargs, args.kw_defaults):
        rows.append(
            {
                "name": arg.arg,
                "kind": "关键字参数",
                "type": unparse(arg.annotation) or "未显式标注",
                "default": format_default(default) or "必填",
            }
        )
    if args.kwarg:
        rows.append(
            {
                "name": f"**{args.kwarg.arg}",
                "kind": "可变关键字参数",
                "type": unparse(args.kwarg.annotation) or "任意",
                "default": "可选",
            }
        )
    return rows


def return_type(node: ast.FunctionDef) -> str:
    return unparse(node.returns) or "未显式标注"


def strip_inline_markup(text: str) -> str:
    text = re.sub(r"`([^`]+)`", r"\1", text)
    text = text.replace("**", "").replace("__", "")
    return text.strip()


def heading_key(line: str) -> str:
    line = strip_inline_markup(line).strip()
    line = re.sub(r"[✅📥📤💡⚠️🧭🧠📌]+", "", line)
    line = line.replace("：", ":").strip()
    return line


def heading_matches(line: str, aliases: list[str]) -> bool:
    key = heading_key(line)
    for alias in aliases:
        if key == alias:
            return True
        if key.startswith(alias) and (
            len(key) == len(alias) or key[len(alias)] in {" ", "(", "（", ":", "："}
        ):
            return True
    return False


def is_section_heading(line: str) -> bool:
    stripped = heading_key(line)
    if any(heading_matches(stripped, aliases) for aliases in SECTION_ALIASES.values()):
        return True
    if stripped.endswith("输入") or stripped.endswith("输出") or stripped.endswith("示例"):
        return True
    return False


def first_sentence(doc: str) -> str:
    lines = [line.strip() for line in doc.strip().splitlines()]
    paragraph: list[str] = []
    for line in lines:
        if not line:
            if paragraph:
                break
            continue
        if line.startswith(">>>") or line.startswith("shape:"):
            break
        if set(line) <= {"-", "^", "="}:
            continue
        if is_section_heading(line):
            break
        paragraph.append(strip_inline_markup(line))
    text = " ".join(paragraph).strip()
    if not text:
        return ""
    for sep in ["。", "."]:
        if sep in text:
            return text.split(sep, 1)[0].strip() + ("。" if sep == "。" else ".")
    return text[:120]


def clean_table_text(text: str, fallback: str) -> str:
    stripped = strip_inline_markup(text)
    if not stripped:
        return fallback
    bad_tokens = [">>>", "shape:", "┌", "│", "└", "╞", "DataFrame("]
    if any(token in stripped for token in bad_tokens):
        return fallback
    stripped = re.sub(r"\s+", " ", stripped).strip()
    if len(stripped) > 180:
        return fallback
    return stripped


SECTION_ALIASES = {
    "input": ["输入列", "输入输出", "输入形态", "输入", "DataFrame 输入"],
    "output": ["输出列", "输出语义", "输出", "返回列", "返回"],
    "params": ["参数", "Args", "Arguments"],
    "notes": ["注意", "可能出错的地方", "重要注意", "强制", "约定", "规则"],
    "call": ["调用", "写法", "例子", "示例", "示例 💡"],
}


def extract_doc_section(doc: str, aliases: list[str]) -> str:
    lines = doc.splitlines()
    for idx, line in enumerate(lines):
        if not heading_matches(line, aliases):
            continue
        start = idx + 1
        if start < len(lines) and set(lines[start].strip()) <= {"-", "^", "="}:
            start += 1
        collected: list[str] = []
        for raw in lines[start:]:
            stripped = raw.strip()
            if any(heading_matches(stripped, group) for group in SECTION_ALIASES.values()):
                break
            if stripped and set(stripped) <= {"-", "^", "="}:
                continue
            collected.append(raw.rstrip())
            if len(collected) > 0 and not stripped and len(collected) > 4:
                break
        return "\n".join(collected).strip()
    return ""


PLOT_INPUTS = {
    "line": "x, y1, y2, ...",
    "bar": "x, y1, y2, ...",
    "histogram": "value",
    "violin": "group, value",
    "qq": "value",
    "scatter": "x, y, payload...",
    "fill": "x, y",
    "mark": "x, y, flag?",
    "area": "x, y1, y2, ...",
    "step_line": "x, y",
    "stacked_area": "x, y1, y2, ...",
    "boxplot": "group, value",
    "heatmap": "x, y, value",
    "table": "任意列",
    "kline": "x, open, high, low, close",
}

SPECIAL_INPUTS = {
    ("expr", "if_else"): "x",
    ("ta", "order_flow_gap"): "last_price, volume, bid1, ask1",
    ("stock", "ols"): "y, x1, x2",
    ("stock", "residual"): "y, x1, x2",
    ("bt", "price"): "close, hold",
    ("bt", "tick"): "target, last_price, bid1, ask1",
    ("batch_ta", "rank"): "datetime, code, x",
    ("batch_ta", "zscore"): "datetime, code, x",
    ("batch_ta", "sort"): "datetime, code, x",
    ("batch_ta", "head"): "datetime, code, x",
    ("batch_ta", "lag"): "datetime, code, x",
    ("batch_ta", "lead"): "datetime, code, x",
    ("batch_ta", "slice"): "datetime, code, x",
}

SPECIAL_USAGE = {
    ("expr", "if_else"): 'col(col("x") > 0, col.lit(1.0), col.lit(-1.0)).if_else()',
    ("expr", "dt"): 'col("datetime").dt',
    ("expr", "child_exprs"): '(col("x") + col("y")).child_exprs()',
    ("expr", "projection_name"): 'col("x").alias("x_clean").projection_name()',
    ("expr", "with_metadata"): 'col("x").with_metadata("unit", "price")',
    ("expr", "get_metadata_value"): 'col("x").with_metadata("unit", "price").get_metadata_value("unit")',
    ("expr", "get_metadata"): 'col("x").with_metadata("unit", "price").get_metadata()',
    ("expr", "attach_to_monitor"): 'col("x", "y").monitor("main").scatter().attach_to_monitor(col("x").monitor("hist").histogram().x_slider())',
    ("expr", "x_slider"): 'col("x").monitor("hist").histogram().x_slider()',
    ("expr", "scatter_select"): 'col("t", "open", "high", "low", "close").monitor("detail").kline().scatter_select(col("t").is_between("t0", "t1"))',
    ("expr", "save"): 'col("x").save("/tmp/qust_expr.bin")',
    ("expr", "load"): 'qs.Expr.load("/tmp/qust_expr.bin")',
    ("ta", "order_flow_gap"): 'col("last_price", "volume", "bid1", "ask1").ta.order_flow_gap',
    ("bt", "tick"): 'col("target", "last_price", "bid1", "ask1").bt.tick(qs.TradePriceType.last_price, qs.MatchPriceType.void)',
    ("alpha", "__call__"): 'col("datetime", "code", "alpha", "ret").alpha()',
    ("udf", "row"): "col(\"x\").udf.row(AddOne())",
    ("udf", "batch"): "col(\"x\").udf.batch(PassThroughBatch())",
    ("udf", "dispatch"): "col(\"x\").udf.dispatch(choose_expr)",
    ("dataframe", "select"): 'df.select(col("x"), col("y"))',
    ("dataframe", "with_cols"): 'df.with_cols((col("x") + col("y")).alias("sum_xy"))',
    ("dataframe", "filter"): 'df.filter(col("x") > 1)',
    ("dataframe", "calc_data"): "df.calc_data(data)",
    ("dataframe", "plot"): "df.plot(data, monitor)",
    ("plugin", "get_dataframe"): 'pool.get_dataframe("raw")',
    ("plugin", "id"): "pool.id",
    ("dataframe", "snapshot"): "df.snapshot()",
    ("dataframe", "save"): 'df.save("/tmp/qust_df.bin")',
    ("dataframe", "load"): 'DataFrame.load("/tmp/qust_df.bin")',
    ("params", "title"): 'pms(2, 120).title("window")',
    ("params", "value"): "pms(2, 120).value(20)",
    ("params", "step"): "pms(2, 120).step(2)",
    ("params", "as_expr"): "pms(2, 120).value(20).as_expr()",
    ("params", "as_list_expr"): 'pms(2, 120).value(20).as_list_expr(name="window")',
    ("params", "get_usize"): "pms(2, 120).value(20).get_usize()",
    ("params", "get_f64"): "pms(0.1, 2.0).value(0.5).get_f64()",
    ("params", "get_string"): 'pms(["fast", "slow"], None).value("fast").get_string()',
    ("top-level", "pms"): "qs.pms(2, 120).title(\"window\").value(20).step(1)",
    ("top-level", "select"): 'qs.select(col("x"), col("y"))',
    ("top-level", "with_cols"): 'qs.with_cols((col("x") + col("y")).alias("sum_xy"))',
    ("top-level", "concat_rows"): 'qs.concat_rows(col("x"), col("y"))',
    ("top-level", "check_usize"): "qs.check_usize(5)",
    ("top-level", "check_f64"): "qs.check_f64(0.5)",
}

OPERATOR_USAGE = {
    "__add__": 'col("x") + col("y")',
    "__radd__": '1.0 + col("x")',
    "__sub__": 'col("x") - col("y")',
    "__rsub__": '1.0 - col("x")',
    "__mul__": 'col("x") * col("y")',
    "__rmul__": '2.0 * col("x")',
    "__truediv__": 'col("x") / col("y")',
    "__rtruediv__": '1.0 / col("x")',
    "__mod__": 'col("x") % col("y")',
    "__rmod__": '10 % col("x")',
    "__floordiv__": 'col("x") // col("y")',
    "__rfloordiv__": '10 // col("x")',
    "__pow__": 'col("x") ** 2',
    "__rpow__": '2 ** col("x")',
    "__gt__": 'col("x") > col("y")',
    "__ge__": 'col("x") >= col("y")',
    "__lt__": 'col("x") < col("y")',
    "__le__": 'col("x") <= col("y")',
    "__eq__": 'col("x") == col("y")',
    "__ne__": 'col("x") != col("y")',
    "__and__": '(col("x") > 0) & (col("y") > 0)',
    "__rand__": '(col("x") > 0).__rand__(col("y") > 0)',
    "__xor__": '(col("x") > 0) ^ (col("y") > 0)',
    "__rxor__": '(col("x") > 0).__rxor__(col("y") > 0)',
    "__or__": '(col("x") > 0) | (col("y") > 0)',
    "__ror__": '(col("x") > 0).__ror__(col("y") > 0)',
    "__invert__": '~(col("x") > 0)',
    "__neg__": '-col("x")',
    "__abs__": 'abs(col("x"))',
}

OPERATOR_TITLES = {
    "__add__": "operator.+",
    "__radd__": "operator.r+",
    "__sub__": "operator.-",
    "__rsub__": "operator.r-",
    "__mul__": "operator.*",
    "__rmul__": "operator.r*",
    "__truediv__": "operator./",
    "__rtruediv__": "operator.r/",
    "__mod__": "operator.%",
    "__rmod__": "operator.r%",
    "__floordiv__": "operator.//",
    "__rfloordiv__": "operator.r//",
    "__pow__": "operator.**",
    "__rpow__": "operator.r**",
    "__gt__": "operator.>",
    "__ge__": "operator.>=",
    "__lt__": "operator.<",
    "__le__": "operator.<=",
    "__eq__": "operator.==",
    "__ne__": "operator.!=",
    "__and__": "operator.&",
    "__rand__": "operator.r&",
    "__xor__": "operator.^",
    "__rxor__": "operator.r^",
    "__or__": "operator.|",
    "__ror__": "operator.r|",
    "__invert__": "operator.~",
    "__neg__": "operator.unary-",
    "__abs__": "operator.abs",
}

OPERATOR_SLUGS = {
    "__add__": "add",
    "__radd__": "radd",
    "__sub__": "sub",
    "__rsub__": "rsub",
    "__mul__": "mul",
    "__rmul__": "rmul",
    "__truediv__": "truediv",
    "__rtruediv__": "rtruediv",
    "__mod__": "mod",
    "__rmod__": "rmod",
    "__floordiv__": "floordiv",
    "__rfloordiv__": "rfloordiv",
    "__pow__": "pow",
    "__rpow__": "rpow",
    "__gt__": "gt",
    "__ge__": "ge",
    "__lt__": "lt",
    "__le__": "le",
    "__eq__": "eq",
    "__ne__": "ne",
    "__and__": "and",
    "__rand__": "rand",
    "__xor__": "xor",
    "__rxor__": "rxor",
    "__or__": "or",
    "__ror__": "ror",
    "__invert__": "invert",
    "__neg__": "neg",
    "__abs__": "abs",
}

USER_VISIBLE_TOP_LEVEL = {
    "context.py": {"with_cols", "select", "filter", "load_data", "save_data"},
    "params.py": {"pms", "check_usize", "check_f64"},
    "expr.py": {"concat_rows"},
}


def namespace_scenario(item: ApiItem) -> str:
    ns = item.namespace
    if ns == "operator":
        return "用于表达式之间的 Python 运算符重载，包括算术、比较、逻辑和一元运算。"
    if ns == "expr":
        return "用于构造表达式链路中的核心计算、上下文切换、调试或元数据操作。"
    if ns == "col":
        return "用于从输入 DataFrame 中选择列、构造字面量或按列位置取列，是所有表达式链路的入口。"
    if ns == "dt":
        return "用于 datetime/date/time 列的字段提取，常见于时间分组、盘中切片和展示列构造。"
    if ns == "math":
        return "用于逐元素数学变换，适合对价格、收益率、因子值做连续数值变换。"
    if ns == "ta":
        return "用于技术指标和行情特征计算，常见输入是 close 或 open/high/low/close/volume。"
    if ns == "batch_ta":
        return "用于批内或截面计算，常和 over(\"datetime\")、batch(\"datetime\") 组合。"
    if ns == "batch_stock":
        return "用于股票批处理/批内回测类计算。"
    if ns == "stock":
        return "用于股票统计建模相关表达式，例如 OLS 和残差。"
    if ns == "kline":
        return "用于 K 线、盘口和周期切片相关预处理。"
    if ns == "stra":
        return "用于策略信号、持仓状态、出入场和交易行号生成。"
    if ns == "bt":
        return "用于回测成交价格、tick 级交易模拟或收益序列生成。"
    if ns == "fp":
        return "用于项目内置的 fast path 快捷策略/批量计算路径。"
    if ns == "monitor":
        return "用于把表达式包装成 Monitor 图层，进入浏览器 dashboard 渲染。"
    if ns == "alpha":
        return "用于 Alpha/Alphalens 分析链路，把因子、收益和 monitor 面板串起来。"
    if ns == "udf":
        return "用于接入 Python 自定义 batch/row/dispatch 逻辑。"
    if ns == "dataframe":
        return "用于构造、执行、保存和恢复 Qust DataFrame 执行计划。"
    if ns == "params":
        return "用于定义 live 参数，并让 monitor 参数面板驱动表达式重算。"
    if ns == "dtype":
        return "用于声明 UDF 输出 Arrow 数据类型。"
    if ns == "top-level":
        return "用于 Python 用户的顶层便捷入口。"
    return "用于当前命名空间下的表达式或辅助 API。"


def namespace_design_reason(item: ApiItem) -> str:
    ns = item.namespace
    if ns == "monitor":
        return (
            "Monitor 相关 API 只描述“画什么”和“交互入口是什么”，不在 Python 里直接画图。"
            "这样同一个表达式可以在本地 Python、Wasm 页面和参数面板重算里复用，真正的绘制细节交给 plot exec 和前端能力 trait。"
        )
    if ns == "params":
        return (
            "参数不是普通 Python 数字，而是会进入表达式树的 live value。"
            "这样参数面板改值时，Rust 执行计划能用同一套依赖关系重算，而不是让用户手动重建整段代码。"
        )
    if ns in {"stra", "bt"}:
        return (
            "策略和回测算子被设计成表达式，是为了让信号、持仓、成交、收益可以和 over/rolling/monitor 组合。"
            "这比在 Python for 循环里维护状态更容易复用，也能在分组和流式数据里保持一致语义。"
        )
    if ns in {"batch_ta", "batch_stock", "stock"}:
        return (
            "批内/截面算子把“按日期、代码或批次计算”的边界留在表达式里。"
            "这样排序、rank、lead/lag、回归等操作可以准确接受上下文，而不是提前把 DataFrame 切碎。"
        )
    if ns == "operator":
        return (
            "Python 运算符被重载成表达式节点，让加减乘除、比较和逻辑组合保持惰性描述。"
            "用户写起来像普通 Python，底层仍然能做类型分发、上下文传播和流式执行。"
        )
    if ns == "dataframe":
        return (
            "Qust DataFrame 是执行计划，不是已经计算完的数据。"
            "这样同一个计划可以反复喂不同数据、参数或 monitor session，适合策略研究和浏览器交互。"
        )
    if ns == "plugin":
        return (
            "插件 API 用来承载计算流以外的 side effect，例如保存中间结果到 DataPool。"
            "它让纯计算路径仍然保持清晰，同时给调试、回查和 monitor callback 留出扩展点。"
        )
    if ns == "udf":
        return (
            "UDF 是逃生口：内置算子覆盖不到的逻辑可以先接入表达式系统。"
            "但 UDF 仍然明确输入输出 schema，避免把整条流式执行链路变成不可分析的 Python 黑盒。"
        )
    if ns == "dtype":
        return (
            "DataType API 用来给 UDF 或复杂返回列声明 Arrow 类型。"
            "显式类型能让 Rust 侧提前知道 schema，减少运行时猜测和序列化歧义。"
        )
    return (
        "Qust 把这一类能力做成表达式节点，是为了把“用户想算什么”保留下来。"
        "只要语义还在表达式树里，后续就可以统一处理分组上下文、窗口状态、参数联动、Wasm 执行和 Monitor 可视化。"
    )


def namespace_benefit(item: ApiItem) -> str:
    ns = item.namespace
    if ns == "monitor":
        return "实际收益是：图表能跟参数、callback、slot、hover/zoom/select 等交互能力统一工作，不需要每个图都写一套 Python 分支。"
    if ns == "params":
        return "实际收益是：调参时只改参数值，表达式和 monitor session 可以自动重算，Notebook 与 Wasm 页面的行为保持一致。"
    if ns in {"stra", "bt"}:
        return "实际收益是：策略信号、交易轮次、持仓收益和交易明细可以直接接到图表和 DataPool，方便定位每一笔交易。"
    if ns in {"batch_ta", "batch_stock", "stock"}:
        return "实际收益是：截面和批内计算不会丢掉分组边界，跨股票/合约的分析更容易写对。"
    if ns == "plugin":
        return "实际收益是：你可以在不改变主输出的情况下保存中间 DataFrame，然后用 Python 直接取出来检查。"
    return "实际收益是：代码更短，但语义更完整；后续计算、画图、调参、保存和回调都能复用同一棵表达式树。"


def parse_input_columns(text: str) -> list[str]:
    text = text.strip()
    if not text:
        return []
    cols: list[str] = []

    for match in re.finditer(r"col\(([^)]*)\)", text):
        inside = match.group(1)
        if "..." in inside:
            continue
        quoted = re.findall(r"""["']([a-zA-Z_][a-zA-Z0-9_]*)["']""", inside)
        if quoted:
            cols.extend(quoted)
            continue
        for part in inside.replace("，", ",").split(","):
            name = part.strip().strip("` ")
            if re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
                cols.append(name)

    if not cols and "DataFrame" in text:
        cols.extend(re.findall(r"""["']([a-zA-Z_][a-zA-Z0-9_]*)["']\s*:""", text))

    if not cols:
        for raw in text.splitlines():
            line = raw.strip().replace("，", ",")
            numbered = re.match(r"^\d+\.\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*[:：]", line)
            if numbered:
                cols.append(numbered.group(1))
                continue
            if ":" in line:
                left, right = line.split(":", 1)
                if re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", left.strip("` ")):
                    cols.append(left.strip("` "))
                    continue
                line = right
            for part in line.split(","):
                part = part.strip().strip("` ")
                if re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", part):
                    cols.append(part)

    out: list[str] = []
    for col_name in cols:
        if col_name in {"col", "pl", "qs", "Expr", "DataFrame", "None", "True", "False", "shape"}:
            continue
        if col_name not in out:
            out.append(col_name)
    return out[:12]


def sample_data_for(cols: Iterable[str]) -> str:
    cols = list(cols) or ["x"]
    values = {
        "datetime": '["2026-01-01 09:30:00", "2026-01-01 09:31:00", "2026-01-01 09:32:00"]',
        "t": '["2026-01-01 09:30:00", "2026-01-01 09:31:00", "2026-01-01 09:32:00"]',
        "date": '["2026-01-01", "2026-01-02", "2026-01-03"]',
        "time": '["09:30:00", "09:31:00", "09:32:00"]',
        "code": '["AAA", "AAA", "AAA"]',
        "symbol": '["AAA", "AAA", "AAA"]',
        "open": "[10.0, 10.2, 10.1]",
        "high": "[10.4, 10.5, 10.3]",
        "low": "[9.9, 10.0, 10.0]",
        "close": "[10.2, 10.1, 10.3]",
        "c": "[10.2, 10.1, 10.3]",
        "price": "[10.0, 10.5, 10.2]",
        "last_price": "[10.0, 10.1, 10.2]",
        "volume": "[1000, 1200, 900]",
        "v": "[1000, 1200, 900]",
        "bid1": "[9.9, 10.0, 10.1]",
        "ask1": "[10.1, 10.2, 10.3]",
        "bid1_v": "[100, 120, 110]",
        "ask1_v": "[90, 130, 115]",
        "bid_v_l": "[[100.0, 50.0], [120.0, 60.0], [110.0, 55.0]]",
        "ask_v_l": "[[90.0, 45.0], [130.0, 65.0], [115.0, 58.0]]",
        "ret": "[0.01, -0.02, 0.03]",
        "pnl": "[1.0, -0.5, 0.8]",
        "signal": "[True, False, True]",
        "open_sig": "[True, False, True]",
        "exit_sig": "[False, True, False]",
        "open_long_sig": "[True, False, False]",
        "exit_long_sig": "[False, False, True]",
        "open_short_sig": "[False, True, False]",
        "exit_short_sig": "[False, False, True]",
        "hold": "[0.0, 1.0, 1.0]",
        "target": "[0.0, 1.0, 0.0]",
        "w": "[0.5, 0.3, 0.2]",
        "flag": "[True, True, True]",
        "limit_up": "[11.0, 11.2, 11.1]",
        "limit_down": "[9.0, 9.2, 9.1]",
        "alpha": "[0.1, -0.2, 0.3]",
        "factor": "[0.1, -0.2, 0.3]",
        "y": "[1.0, 2.0, 4.0]",
        "x1": "[1.0, 2.0, 3.0]",
        "x2": "[2.0, 0.5, 1.5]",
        "x": "[1.0, 2.0, 3.0]",
        "y_upper": "[3.0, 4.0, 3.5]",
        "y_lower": "[1.0, 1.5, 1.2]",
        "a": "[3, 1, 2]",
        "b": "[2, 3, 4]",
        "n": "[0, 1, 2]",
        "y": "[2.0, 3.5, 3.0]",
        "value": "[1.0, 3.0, 2.0]",
        "group": '["A", "A", "B"]',
    }
    rows = [f'        "{c}": {values.get(c, "[1.0, 2.0, 3.0]")},' for c in cols]
    conversions = []
    for name in ("datetime", "t"):
        if name in cols:
            conversions.append(f'pl.col("{name}").str.to_datetime()')
    if conversions:
        tail = ").with_columns(" + ", ".join(conversions) + ")"
    else:
        tail = ")"
    return "data = pl.DataFrame(\n    {\n" + "\n".join(rows) + "\n    }\n" + tail


SAMPLE_TABLE_VALUES = {
    "datetime": ["2026-01-01 09:30:00", "2026-01-01 09:31:00", "2026-01-01 09:32:00"],
    "t": ["2026-01-01 09:30:00", "2026-01-01 09:31:00", "2026-01-01 09:32:00"],
    "date": ["2026-01-01", "2026-01-02", "2026-01-03"],
    "time": ["09:30:00", "09:31:00", "09:32:00"],
    "code": ["AAA", "AAA", "AAA"],
    "symbol": ["AAA", "AAA", "AAA"],
    "open": [10.0, 10.2, 10.1],
    "high": [10.4, 10.5, 10.3],
    "low": [9.9, 10.0, 10.0],
    "close": [10.2, 10.1, 10.3],
    "c": [10.2, 10.1, 10.3],
    "price": [10.0, 10.5, 10.2],
    "last_price": [10.0, 10.1, 10.2],
    "volume": [1000, 1200, 900],
    "v": [1000, 1200, 900],
    "bid1": [9.9, 10.0, 10.1],
    "ask1": [10.1, 10.2, 10.3],
    "bid1_v": [100, 120, 110],
    "ask1_v": [90, 130, 115],
    "bid_v_l": [[100.0, 50.0], [120.0, 60.0], [110.0, 55.0]],
    "ask_v_l": [[90.0, 45.0], [130.0, 65.0], [115.0, 58.0]],
    "ret": [0.01, -0.02, 0.03],
    "pnl": [1.0, -0.5, 0.8],
    "signal": [True, False, True],
    "open_sig": [True, False, True],
    "exit_sig": [False, True, False],
    "open_long_sig": [True, False, False],
    "exit_long_sig": [False, False, True],
    "open_short_sig": [False, True, False],
    "exit_short_sig": [False, False, True],
    "hold": [0.0, 1.0, 1.0],
    "target": [0.0, 1.0, 0.0],
    "w": [0.5, 0.3, 0.2],
    "flag": [True, True, True],
    "limit_up": [11.0, 11.2, 11.1],
    "limit_down": [9.0, 9.2, 9.1],
    "alpha": [0.1, -0.2, 0.3],
    "factor": [0.1, -0.2, 0.3],
    "y": [2.0, 3.5, 3.0],
    "x1": [1.0, 2.0, 3.0],
    "x2": [2.0, 0.5, 1.5],
    "x": [1.0, 2.0, 3.0],
    "y_upper": [3.0, 4.0, 3.5],
    "y_lower": [1.0, 1.5, 1.2],
    "a": [3, 1, 2],
    "b": [2, 3, 4],
    "n": [0, 1, 2],
    "value": [1.0, 3.0, 2.0],
    "group": ["A", "A", "B"],
}


def sanitize_display_text(text: str) -> str:
    text = text.replace(str(ROOT), "<project>")
    text = re.sub(r"0x[0-9a-fA-F]+", "0x...", text)
    return text


def preview_value(value) -> str:
    if value is True:
        return "true"
    if value is False:
        return "false"
    if value is None:
        return "null"
    if isinstance(value, float):
        if value.is_integer():
            return f"{value:.1f}"
        return f"{value:.10g}"
    return sanitize_display_text(str(value))


def input_data_preview_table(cols: list[str]) -> str:
    cols = cols or ["x"]
    rows: list[list[str]] = []
    for idx in range(3):
        row = []
        for col_name in cols:
            values = SAMPLE_TABLE_VALUES.get(col_name, SAMPLE_TABLE_VALUES["x"])
            row.append(preview_value(values[idx % len(values)]))
        rows.append(row)
    dtypes = [sample_dtype_for_col(col_name) for col_name in cols]
    return polars_dataframe_table(cols, rows, dtypes)


def html_value(value) -> str:
    return html.escape(preview_value(value))


def dataframe_to_html_table(df, max_rows: int = 8) -> str:
    columns = [str(c) for c in df.columns]
    rows: list[list[str]] = []
    data = df.head(max_rows)
    for idx in range(data.height):
        rows.append([preview_value(data[col][idx]) for col in columns])
    if not rows:
        rows.append(["空结果"] + [""] * max(0, len(columns) - 1))
    dtypes = [polars_dtype_label(df.schema.get(col)) for col in columns]
    return polars_dataframe_table(columns or ["结果"], rows, dtypes or ["null"])


def sample_dtype_for_col(col_name: str) -> str:
    values = SAMPLE_TABLE_VALUES.get(col_name, SAMPLE_TABLE_VALUES["x"])
    if col_name in {"datetime", "t"}:
        return "datetime[μs]"
    if col_name == "date":
        return "date"
    if values and isinstance(values[0], bool):
        return "bool"
    if values and isinstance(values[0], int):
        return "i64"
    if values and isinstance(values[0], float):
        return "f64"
    if values and isinstance(values[0], list):
        return "list[f64]"
    return "str"


def polars_dtype_label(dtype) -> str:
    text = str(dtype)
    mapping = {
        "Float64": "f64",
        "Float32": "f32",
        "Int64": "i64",
        "Int32": "i32",
        "Int16": "i16",
        "Int8": "i8",
        "UInt64": "u64",
        "UInt32": "u32",
        "UInt16": "u16",
        "UInt8": "u8",
        "Boolean": "bool",
        "String": "str",
        "Utf8": "str",
        "Null": "null",
        "Date": "date",
    }
    if text in mapping:
        return mapping[text]
    if text.startswith("Datetime"):
        unit = "μs"
        unit_match = re.search(r"time_unit='([^']+)'", text)
        if unit_match:
            unit = {"us": "μs", "ns": "ns", "ms": "ms"}.get(unit_match.group(1), unit_match.group(1))
        return f"datetime[{unit}]"
    if text.startswith("List"):
        return "list"
    return text


def polars_dataframe_table(headers: list[str], rows: list[list[str]], dtypes: list[str] | None = None) -> str:
    headers = headers or ["结果"]
    dtypes = dtypes or ["?"] * len(headers)
    shape = f"shape: ({len(rows)}, {len(headers)})"
    head_cells = []
    for idx, header in enumerate(headers):
        dtype = dtypes[idx] if idx < len(dtypes) else "?"
        head_cells.append(
            "<th>"
            f'<span class="polars-col">{html.escape(str(header))}</span>'
            f'<span class="polars-dtype">{html.escape(str(dtype))}</span>'
            "</th>"
        )
    body = ""
    for row in rows:
        cells = []
        for value in row:
            cells.append(f"<td>{html.escape(sanitize_display_text(str(value)))}</td>")
        body += "<tr>" + "".join(cells) + "</tr>"
    return (
        '<div class="polars-frame">'
        f'<div class="polars-shape">{html.escape(shape)}</div>'
        '<table class="polars-df-table">'
        "<thead><tr>"
        + "".join(head_cells)
        + "</tr></thead><tbody>"
        + body
        + "</tbody></table></div>"
    )


_RUNTIME_ENV_CACHE: dict[str, object] | None = None


def runtime_env(item: ApiItem) -> dict[str, object]:
    global _RUNTIME_ENV_CACHE
    if _RUNTIME_ENV_CACHE is not None:
        env = dict(_RUNTIME_ENV_CACHE)
    else:
        sys.path.insert(0, str(ROOT / "otters-py" / "python"))
        import polars as pl
        import qust as qs
        from qust import UdfBatch, UdfRow, col, pms
        from qust import dt as qdt
        from qust.dataframe import DataFrame
        import datetime as datetime_mod

        class AddOne(UdfRow):
            def output_schema(self, input_schema):
                return [("x_plus_1", qdt.Float64)]

            def update(self, x):
                self.x = x

            def calc(self):
                return [None if self.x is None else self.x + 1.0]

        class PassThroughBatch(UdfBatch):
            def calc_batch(self, batch):
                return batch

        def choose_expr(schema):
            return col("x") + col.lit(1.0)

        _RUNTIME_ENV_CACHE = {
            "pl": pl,
            "qs": qs,
            "col": col,
            "pms": pms,
            "qdt": qdt,
            "DataFrame": DataFrame,
            "AddOne": AddOne,
            "PassThroughBatch": PassThroughBatch,
            "choose_expr": choose_expr,
            "datetime_mod": datetime_mod,
        }
        env = dict(_RUNTIME_ENV_CACHE)
    env["dt"] = env["qdt"] if item.namespace == "dtype" else env["datetime_mod"]
    return env


def dataframe_from_preview_cols(env: dict[str, object], cols: list[str]):
    pl = env["pl"]
    data = {
        col_name: SAMPLE_TABLE_VALUES.get(col_name, SAMPLE_TABLE_VALUES["x"])
        for col_name in (cols or ["x"])
    }
    df = pl.DataFrame(data)
    conversions = []
    for name in ("datetime", "t"):
        if name in data:
            conversions.append(pl.col(name).str.to_datetime())
    if conversions:
        df = df.with_columns(*conversions)
    return df


def result_to_html_table(result, env: dict[str, object], item: ApiItem, output_expr: str) -> str:
    pl = env["pl"]
    if isinstance(result, pl.DataFrame):
        return dataframe_to_html_table(result)
    if item.namespace == "dtype":
        return polars_dataframe_table(
            ["输出项", "值"],
            [
                ["Python 表示", repr(result)],
                ["Arrow dtype spec", getattr(result, "__qust_arrow_dtype__", str(result))],
            ],
            ["str", "str"],
        )
    if isinstance(result, dict):
        return polars_dataframe_table(["key", "value"], [[str(k), str(v)] for k, v in result.items()], ["str", "str"])
    if isinstance(result, (list, tuple)):
        return polars_dataframe_table(
            ["index", "value"],
            [[str(i), repr(v)] for i, v in enumerate(result[:8])],
            ["u32", "str"],
        )
    return polars_dataframe_table(
        ["输出项", "值"],
        [
            ["输出表达式", output_expr],
            ["运行结果", repr(result)],
        ],
        ["str", "str"],
    )


def runtime_output_result_table(item: ApiItem, output_expr: str, input_cols: list[str]) -> str:
    runtime_skip_namespaces = {
        "monitor",
        "alpha",
        "kline",
        "stra",
        "fp",
        "batch_stock",
        "batch_ta",
        "stock",
    }
    runtime_skip_expr_methods = {
        "attach_to_monitor",
        "batch",
        "group_by",
        "over",
        "rolling",
        "rolling_intra_day",
        "expanding_intra_day",
        "shift_intra",
        "get_by_index",
        "snapshot_by",
        "x_slider",
        "scatter_select",
        "save",
        "load",
        "print",
        "perf",
        "cache",
        "cov",
    }
    runtime_skip_specific = {
        ("ta", "plus_dm"),
        ("ta", "minus_dm"),
    }
    if item.namespace in runtime_skip_namespaces or (
        item.namespace == "expr" and item.node.name in runtime_skip_expr_methods
    ) or (item.namespace, item.node.name) in runtime_skip_specific:
        return polars_dataframe_table(
            ["输出项", "网页显示"],
            [
                ["输出表达式", output_expr],
                ["输出结果", "该算子需要运行时上下文、Monitor、文件、分组/窗口状态或更多真实数据；完整例子会在运行时打印实际输出。"],
            ],
            ["str", "str"],
        )
    try:
        env = runtime_env(item)
        data = dataframe_from_preview_cols(env, input_cols)
        env["data"] = data
        env["df"] = env["qs"].select(env["col"]("x")) if "x" in data.columns else env["qs"].select(env["col"](data.columns[0]))
        result = eval(output_expr, env)
        if hasattr(result, "_expr"):
            result = env["qs"].select(result).calc_data(data)
        elif hasattr(result, "calc_data"):
            result = result.calc_data(data)
        return result_to_html_table(result, env, item, output_expr)
    except BaseException as exc:
        return polars_dataframe_table(
            ["输出项", "网页显示"],
            [
                ["输出表达式", output_expr],
                ["输出结果", "此算子需要运行时上下文、Monitor、文件、未实现路径或更多真实数据；完整例子会在运行时打印实际输出。"],
                ["示例生成状态", type(exc).__name__ + ": " + str(exc)[:160]],
            ],
            ["str", "str"],
        )


def expression_preview_html(
    item: ApiItem,
    *,
    input_expr: str,
    output_expr: str,
    input_cols: list[str],
    example: str,
) -> str:
    expr_rows = [
        ["输入表达式", f"<code>{html.escape(input_expr)}</code>"],
        ["输出表达式", f"<code>{html.escape(output_expr)}</code>"],
    ]
    data_or_param = (
        f"<h4>输入数据表格</h4>{input_data_preview_table(input_cols)}"
        if "data = pl.DataFrame" in example
        else table(
            ["输入项", "网页显示"],
            [["输入对象/参数", f"<code>{html.escape(input_expr)}</code>"]],
        )
    )
    output_rows = [
        ["输出表达式", f"<code>{html.escape(output_expr)}</code>"],
        ["输出变量", "<code>out</code> 或 <code>result</code>"],
        ["返回类型", f"<code>{html.escape(return_type(item.node))}</code>"],
    ]
    result_table = runtime_output_result_table(item, output_expr, input_cols)
    return f"""
  <h3>网页表格预览</h3>
  <p>这里把算子的输入表达式、输出表达式、输入数据和输出结果直接渲染成网页表格，不需要看代码块或控制台。</p>
  <h4>输入/输出表达式表格</h4>
  {table(["项目", "表达式"], expr_rows)}
  {data_or_param}
  <h4>输出表达式表格</h4>
  {table(["项目", "网页显示"], output_rows)}
  <h4>输出结果表格</h4>
  {result_table}
"""


def placeholder_for_param(name: str, typ: str, default: str) -> str:
    if default and default != "必填":
        return default
    lower = f"{name} {typ}".lower()
    if name == "intervals":
        return "[(dt.time(9, 0, 0), dt.time(9, 1, 0))]"
    if name == "arrow_dtype":
        return '"Float64"'
    if name in {"start", "end"} and "dt.time" in lower:
        return "dt.time(9, 0, 0)" if name == "start" else "dt.time(15, 0, 0)"
    if name in {"trade_price"}:
        return "qs.TradePriceType.last_price"
    if name in {"match_price"}:
        return "qs.MatchPriceType.void"
    if name in {"inner", "key", "value", "run_ends", "values"} and "datatype" in lower:
        return "dt.Int32" if name in {"key", "run_ends"} else "dt.Float64"
    if name in {"fields"}:
        return '[(\"value\", dt.Float64)]'
    if name == "type_ids":
        return "[0]"
    if name == "bytes_":
        return "16"
    if name == "precision":
        return "10"
    if name == "scale":
        return "2"
    if name == "n":
        return "2"
    if "monitor" in lower:
        return "monitor"
    if name in {"constant"}:
        return "None"
    if "udfbatch" in lower:
        return "MyBatchUdf()"
    if "udfrow" in lower:
        return "MyRowUdf()"
    if "schema" in lower:
        return "schema"
    if "bool" in lower or name.startswith("reverse"):
        return "False"
    if "str" in lower or name in {"id", "name", "slot_id", "shape", "color", "background", "file", "p", "path", "file_name"}:
        return f'"{name}"'
    if "float" in lower or name in {"alpha", "frac", "fee_rate", "target", "thre"}:
        return "0.5"
    if "int" in lower or "usize" in lower or name in {"n", "size", "window", "timeperiod", "offset", "length"}:
        return "14"
    if name == "dtype":
        return "pl.Float64"
    if name in {"args", "*args", "by", "*by"}:
        return '"code"'
    return name


def call_args(item: ApiItem) -> str:
    parts = []
    for row in params_for(item.node):
        if row["name"].startswith("**"):
            continue
        if row["name"].startswith("*"):
            if row["name"] == "*plots":
                parts.append("plot_expr")
            elif row["name"] in {"*args", "*by"}:
                parts.append('"code"')
            continue
        value = placeholder_for_param(row["name"], row["type"], row["default"])
        if row["kind"] == "关键字参数":
            parts.append(f"{row['name']}={value}")
        elif row["default"] != "必填":
            if value != "None":
                parts.append(value)
        else:
            parts.append(value)
    return ", ".join(parts)


def node_source(item: ApiItem) -> str:
    return ast.unparse(item.node)


def is_not_implemented(item: ApiItem) -> bool:
    return "_not_implemented" in node_source(item)


def source_usage_from_doc(doc: str) -> str | None:
    section = extract_doc_section(doc, SECTION_ALIASES["call"])
    if not section:
        return None
    lines = section.splitlines()
    for idx, raw in enumerate(lines):
        stripped = raw.strip()
        if not stripped.startswith(">>>"):
            continue
        expr = stripped[3:].strip()
        if not expr or expr.startswith("#"):
            continue
        if any(skip in expr for skip in ["obj.", "calc_data(", "print(", "import ", "data ="]):
            continue
        cont_idx = idx + 1
        while cont_idx < len(lines) and lines[cont_idx].strip().startswith("..."):
            expr += "\n" + lines[cont_idx].strip()[3:].rstrip()
            cont_idx += 1
        if "..." in expr or "TradePrceType" in expr:
            continue
        if expr.startswith(("col(", "qs.", "pms(", "dt.", "DataType.")):
            return expr
    return None


def expr_prefix_for(item: ApiItem, input_text: str) -> str:
    if item.namespace == "col":
        return "col"
    if item.namespace == "dt":
        return 'col("datetime").dt'
    if item.namespace == "monitor":
        cols = parse_input_columns(PLOT_INPUTS.get(item.node.name, "")) or ["datetime", "value"]
        return f"col({', '.join(repr(c) for c in cols)}).monitor(\"main\")"
    if item.namespace in {"ta", "kline", "bt"}:
        cols = parse_input_columns(input_text)
        if not cols:
            cols = ["close"]
        return f"col({', '.join(repr(c) for c in cols)}).{item.namespace}"
    if item.namespace == "batch_ta":
        cols = parse_input_columns(input_text) or ["x"]
        value_cols = [c for c in cols if c not in {"datetime", "date", "code", "group", "symbol"}]
        return f"col({', '.join(repr(c) for c in (value_cols or ['x']))}).batch_ta"
    if item.namespace == "stock":
        cols = parse_input_columns(input_text) or ["y", "x1", "x2"]
        return f"col({', '.join(repr(c) for c in cols)}).stock"
    if item.namespace == "batch_stock":
        cols = parse_input_columns(input_text) or ["datetime", "code", "w", "price", "flag", "limit_up", "limit_down"]
        return f"col({', '.join(repr(c) for c in cols)}).batch_stock"
    if item.namespace == "stra":
        cols = parse_input_columns(input_text)
        if cols:
            return f"col({', '.join(repr(c) for c in cols)}).stra"
        if item.node.name == "trade_row":
            return 'col("open_sig", "exit_sig").stra'
        return 'col("open", "high", "low", "close").stra'
    if item.namespace == "math":
        return 'col("x").math'
    if item.namespace == "alpha":
        return 'col("datetime", "code", "alpha", "ret").alpha()'
    if item.namespace == "udf":
        return 'col("x").udf'
    if item.namespace == "fp":
        return 'col("datetime", "code", "ret").fp'
    return 'col("x")'


def usage_for(item: ApiItem, input_text: str) -> str:
    name = item.node.name
    special = SPECIAL_USAGE.get((item.namespace, name))
    if special:
        return special
    doc_usage = source_usage_from_doc(ast.get_docstring(item.node) or "")
    if doc_usage:
        return doc_usage
    if item.namespace == "operator":
        return OPERATOR_USAGE.get(name, f'col("x").{name}(col("y"))')
    args = call_args(item)
    if name == "__call__":
        if item.namespace == "col":
            return 'col("close")'
        return f"{expr_prefix_for(item, input_text)}({args})"
    if item.is_property:
        return f"{expr_prefix_for(item, input_text)}.{name}"
    if item.namespace == "dataframe":
        return f"df.{name}({args})"
    if item.namespace == "params":
        return f"pms(2, 120).{name}({args})"
    if item.namespace == "dtype":
        return f"dt.{name}({args})"
    if item.namespace == "top-level":
        return f"qs.{name}({args})"
    return f"{expr_prefix_for(item, input_text)}.{name}({args})"


def escaped_usage_literal(usage: str) -> str:
    return repr(usage)


def first_top_level_call(text: str, name: str) -> str:
    start = text.find(f"{name}(")
    if start < 0:
        return ""
    depth = 0
    quote: str | None = None
    escape = False
    for idx in range(start, len(text)):
        ch = text[idx]
        if quote:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == quote:
                quote = None
            continue
        if ch in {"'", '"'}:
            quote = ch
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return text[start : idx + 1]
    return ""


def input_expression_for(item: ApiItem, usage: str, input_cols: list[str]) -> str:
    if item.namespace == "operator":
        cols = re.findall(r"""col\(["'][^"']+["']\)""", usage)
        return ", ".join(dict.fromkeys(cols)) or usage
    if item.namespace == "dataframe":
        return "df"
    if item.namespace == "params":
        return 'pms(2, 120).title("window").value(20).step(1)'
    if item.namespace == "dtype":
        args = call_args(item)
        return args or "DataType 参数"
    if item.namespace == "top-level":
        return "顶层 API 调用参数"
    first_col = first_top_level_call(usage, "col")
    if first_col:
        return first_col
    if input_cols:
        return f"col({', '.join(repr(c) for c in input_cols)})"
    return usage


def inject_expression_prints(code: str, input_expr: str, output_expr: str) -> str:
    if 'print("输入表达式:")' in code and 'print("输出表达式:")' in code:
        return code
    block = "\n".join(
        [
            'print("输入表达式:")',
            f"print({escaped_usage_literal(input_expr)})",
            'print("输出表达式:")',
            f"print({escaped_usage_literal(output_expr)})",
        ]
    )
    lines = code.splitlines()
    idx = 0
    while idx < len(lines):
        stripped = lines[idx].strip()
        if not stripped or stripped.startswith("import ") or stripped.startswith("from "):
            idx += 1
            continue
        break
    new_lines = lines[:idx]
    if new_lines and new_lines[-1].strip():
        new_lines.append("")
    new_lines.extend(block.splitlines())
    new_lines.append("")
    new_lines.extend(lines[idx:])
    return "\n".join(new_lines).rstrip()


def example_for(item: ApiItem, usage: str, sample_data: str, safe_alias: str) -> str:
    name = item.node.name
    if is_not_implemented(item):
        return f"""import polars as pl
import qust as qs
from qust import col

{sample_data}

print("输入 DataFrame:")
print(data)
print("调用:")
print({escaped_usage_literal(usage)})

try:
    expr = {usage}
    out = qs.select(expr).calc_data(data)
    print("输出 DataFrame:")
    print(out)
except NotImplementedError as err:
    print("输出异常:")
    print(type(err).__name__ + ": " + str(err))"""

    if item.namespace == "dtype":
        return f"""from qust import dt

print("输入参数:")
print({escaped_usage_literal(usage)})

dtype = {usage}

print("输出 DataType:")
print(dtype)
print("Arrow dtype spec:")
print(dtype.__qust_arrow_dtype__)"""

    if item.namespace == "params":
        return f"""import qust as qs
from qust import pms

print("输入参数对象:")
param = pms(2, 120).title("window").value(20).step(1)
print(param)

print("调用:")
print({escaped_usage_literal(usage)})
result = {usage}

print("输出:")
print(result)"""

    if item.namespace == "dataframe":
        if name == "calc_data":
            return f"""import polars as pl
import qust as qs
from qust import col
from qust.dataframe import DataFrame

data = pl.DataFrame({{"x": [1.0, 2.0, 3.0], "y": [3.0, 2.0, 1.0]}})
df = qs.select((col("x") + col("y")).alias("sum_xy"))

print("输入 DataFrame:")
print(data)
out = {usage}

print("输出 DataFrame:")
print(out)"""
        if name in {"save", "load"}:
            return """import polars as pl
import qust as qs
from qust import col
from qust.dataframe import DataFrame

data = pl.DataFrame({"x": [1.0, 2.0, 3.0]})
path = "/tmp/qust_df.bin"
df = qs.select(col("x").mean())

print("输入 DataFrame:")
print(data)
df.save(path)
loaded = DataFrame.load(path)
out = loaded.calc_data(data)

print("输出 DataFrame:")
print(out)"""
        return f"""import polars as pl
import qust as qs
from qust import col
from qust.dataframe import DataFrame

data = pl.DataFrame({{"x": [1.0, 2.0, 3.0], "y": [3.0, 2.0, 1.0]}})
df = DataFrame()

print("输入 DataFrame:")
print(data)
print("调用:")
print({escaped_usage_literal(usage)})

df2 = {usage}
out = df2.calc_data(data)

print("输出 DataFrame:")
print(out)"""

    if item.namespace == "plugin":
        return """import qust as qs
from qust import col

data = qs.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
pool = qs.DataPool("raw_pool")
df = qs.select(col("x", "y").save_data("raw", pool), (col("x") + col("y")).alias("sum_xy"))

print("输入 DataFrame:")
print(data)
_ = df.calc_data(data)
print("调用:")
print("pool.get_dataframe(\\"raw\\")")
out = pool.get_dataframe("raw")

print("输出 DataFrame:")
print(out)"""

    if item.namespace == "top-level":
        if name in {"select", "with_cols"}:
            return f"""import polars as pl
import qust as qs
from qust import col

data = pl.DataFrame({{"x": [1.0, 2.0, 3.0], "y": [3.0, 2.0, 1.0]}})

print("输入 DataFrame:")
print(data)
df = {usage}
out = df.calc_data(data)

print("输出 DataFrame:")
print(out)"""
        if name == "concat_rows":
            return f"""import polars as pl
import qust as qs
from qust import col

data = pl.DataFrame({{"x": [1.0, 2.0, 3.0], "y": [3.0, 2.0, 1.0]}})

print("输入 DataFrame:")
print(data)
expr = {usage}
out = qs.select(expr).calc_data(data)

print("输出 DataFrame:")
print(out)"""
        if name == "load_data":
            return """import polars as pl
import qust as qs

source = pl.DataFrame({"x": [1.0, 2.0, 3.0]})
path = "/tmp/qust_load_data_example.parquet"
source.write_parquet(path)

print("输入文件:")
print(path)
data = qs.load_data(path)

print("输出 DataFrame:")
print(data)"""
        if name == "save_data":
            return """import polars as pl
import qust as qs

data = pl.DataFrame({"x": [1.0, 2.0, 3.0]})
path = "/tmp/qust_save_data_example.parquet"

print("输入 DataFrame:")
print(data)
info = qs.save_data(data, path)

print("输出保存信息:")
print(info)"""
        return f"""import qust as qs
from qust import col, pms

print("输入:")
print({escaped_usage_literal(usage)})
result = {usage}

print("输出:")
print(result)"""

    if item.namespace == "monitor":
        plot_usage = usage.replace(".monitor.", '.monitor("main").').replace(
            ".monitor()", '.monitor("main")'
        )
        return f"""import polars as pl
import qust as qs
from qust import col, Monitor

{sample_data}

print("输入 DataFrame:")
print(data)

monitor = Monitor(background="white").make_grid([["main"]])
plot = {plot_usage}
df = qs.select(plot)
session = monitor.session(df, data).params().actions()
page = session.show_local(auto_open=True)

print("输出:")
print("Monitor 页面 URL:", page)"""

    if item.namespace == "alpha":
        if name == "__call__":
            return f"""import polars as pl
import qust as qs
from qust import col

data = pl.DataFrame({{
    "datetime": ["2026-01-01", "2026-01-01", "2026-01-02", "2026-01-02"],
    "code": ["AAA", "BBB", "AAA", "BBB"],
    "alpha": [0.1, -0.2, 0.3, 0.2],
    "ret": [0.01, -0.01, 0.02, 0.03],
}}).with_columns(pl.col("datetime").str.to_date())

print("输入 DataFrame:")
print(data)
ns = {usage}

print("输出对象:")
print(ns)"""
        return f"""import polars as pl
import qust as qs
from qust import col, pms, Monitor

data = pl.DataFrame({{
    "datetime": ["2026-01-01", "2026-01-01", "2026-01-02", "2026-01-02"],
    "code": ["AAA", "BBB", "AAA", "BBB"],
    "alpha": [0.1, -0.2, 0.3, 0.2],
    "ret": [0.01, -0.01, 0.02, 0.03],
}}).with_columns(pl.col("datetime").str.to_date())

print("输入 DataFrame:")
print(data)

monitor = Monitor(background="white").make_grid([["summary"]])
expr = {usage}
df = qs.select(expr)
session = monitor.session(df, data).params().actions()
page = session.show_local()

print("输出:")
print("Alpha 分析 Monitor 页面 URL:", page)"""

    if item.namespace == "expr" and name == "dt":
        return f"""import polars as pl
import qust as qs
from qust import col

{sample_data}

print("输入 DataFrame:")
print(data)
expr = col("datetime").dt.year().alias("year")
out = qs.select(expr).calc_data(data)

print("输出 DataFrame:")
print(out)"""

    if item.namespace == "expr" and name in {"child_exprs", "projection_name", "get_metadata_value", "get_metadata"}:
        return f"""from qust import col

print("输入表达式:")
print({escaped_usage_literal(usage)})
result = {usage}

print("输出:")
print(result)"""

    if item.namespace == "expr" and name in {"save", "load"}:
        return """import polars as pl
import qust as qs
from qust import col

data = pl.DataFrame({"x": [1.0, 2.0, 3.0]})
path = "/tmp/qust_expr.bin"

print("输入 DataFrame:")
print(data)
col("x").save(path)
expr = qs.Expr.load(path)
out = qs.select(expr).calc_data(data)

print("输出 DataFrame:")
print(out)"""

    if item.namespace == "expr" and name == "attach_to_monitor":
        return """import polars as pl
import qust as qs
from qust import col

data = pl.DataFrame({"x": [1.0, 2.0, 3.0], "y": [2.0, 3.5, 3.0]})

print("输入 DataFrame:")
print(data)

callback = col("x").monitor("hist").histogram().x_slider()
expr = col("x", "y").monitor("main").scatter().attach_to_monitor(callback)
df = qs.select(expr)
monitor = qs.Monitor().make_grid([["main", "hist"]])
session = monitor.session(df, data).params().actions()

print("输出:")
print(session)"""

    if item.namespace == "expr" and name == "x_slider":
        return """from qust import col

callback = col("x").monitor("hist").histogram().x_slider()

print("输出:")
print(callback)"""

    if item.namespace == "expr" and name == "scatter_select":
        return """from qust import col

callback = (
    col("t", "open", "high", "low", "close")
    .monitor("detail")
    .kline()
    .scatter_select(col("t").is_between("t0", "t1"))
)

print("输出:")
print(callback)"""

    if item.namespace == "udf":
        if name == "batch":
            return f"""import polars as pl
import qust as qs
from qust import col, UdfBatch

class PassThroughBatch(UdfBatch):
    def calc_batch(self, batch):
        return batch

{sample_data}

print("输入 DataFrame:")
print(data)

expr = col("x").udf.batch(PassThroughBatch())
out = qs.select(expr).calc_data(data)

print("输出 DataFrame:")
print(out)"""
        if name == "dispatch":
            return f"""import polars as pl
import qust as qs
from qust import col

def choose_expr(schema):
    return col("x") + col.lit(1.0)

{sample_data}

print("输入 DataFrame:")
print(data)

expr = col("x").udf.dispatch(choose_expr)
out = qs.select(expr).calc_data(data)

print("输出 DataFrame:")
print(out)"""
        return f"""import polars as pl
import qust as qs
from qust import col, UdfRow, dt

class AddOne(UdfRow):
    def output_schema(self, input_schema):
        return [("x_plus_1", dt.Float64)]

    def update(self, x):
        self.x = x

    def calc(self):
        return [None if self.x is None else self.x + 1.0]

{sample_data}

print("输入 DataFrame:")
print(data)

expr = col("x").udf.row(AddOne())
out = qs.select(expr).calc_data(data)

print("输出 DataFrame:")
print(out)"""

    return f"""import polars as pl
import qust as qs
from qust import col

{sample_data}

print("输入 DataFrame:")
print(data)

expr = {usage}
df = qs.select(expr)
out = df.calc_data(data)

print("输出 DataFrame:")
print(out)"""


def html_doc(doc: str) -> str:
    cleaned = clean_doc_for_display(doc)
    if not cleaned.strip():
        return "<p>这个算子没有额外长说明；本页已经根据函数签名、命名空间语义和项目约定补了用法、输入输出、设计动机和示例。</p>"
    return f"<pre><code>{html.escape(cleaned.strip())}</code></pre>"


def clean_doc_for_display(doc: str) -> str:
    doc = doc.replace("TradePrceType", "TradePriceType")
    doc = re.sub(
        r"\n\s*示例\s*💡\s*\n\s*[-^=]+\s*\n\s*>>> # 最小调用示例（请替换为你的实际对象/参数）\s*\n\s*>>> obj\.[^\n]*(?:\n\s*)?",
        "\n",
        doc,
    )
    return doc


def code_block(title: str, code: str) -> str:
    return (
        '<div class="code-block">'
        f'<div class="code-title">{html.escape(title)}</div>'
        f"<pre><code>{html.escape(code.rstrip())}</code></pre>"
        "</div>"
    )


def table(headers: list[str], rows: list[list[str]]) -> str:
    head = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    body = ""
    for row in rows:
        body += "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>"
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def source_example_html(doc: str) -> str:
    doc = clean_doc_for_display(doc)
    blocks: list[str] = []
    for title, aliases in [
        ("文档示例：输入", ["DataFrame 输入"]),
        ("文档示例：调用", ["调用"]),
        ("文档示例：输出", ["输出"]),
    ]:
        text = extract_doc_section(doc, aliases)
        if not text.strip():
            continue
        if not any(marker in text for marker in [">>>", "shape:", "┌", "│", "└"]):
            continue
        blocks.append(code_block(title, text))
    if not blocks:
        return ""
    return "<h3>文档示例输入/输出</h3>\n<p>这一组保留了 DataFrame 打印形态，方便你直接对照运行结果。</p>\n" + "\n".join(blocks)


def rel_prefix(path: str) -> str:
    depth = len(Path(path).parts) - 1
    return "../" * depth


def nav_html(
    items: list[ApiItem],
    prefix: str,
    current_ns: str | None = None,
    current_path: str | None = None,
) -> str:
    groups: dict[str, list[ApiItem]] = {}
    titles: dict[str, str] = {}
    for item in items:
        groups.setdefault(item.namespace, []).append(item)
        titles[item.namespace] = item.group_title
    top = [
        ("首页", "index.html"),
        ("算子总览", "operators/index.html"),
        ("Wasm", "wasm.html"),
        ("设计思想", "index.html#architecture"),
    ]
    html_parts = ['<p class="sidebar-title">Reference</p>', '<div class="nav-section">']
    for label, href in top:
        html_parts.append(f'<a class="nav-link" href="{prefix}{href}">{html.escape(label)}</a>')
    html_parts.append("</div>")
    html_parts.append('<div class="nav-section">')
    html_parts.append('<p class="nav-heading">命名空间</p>')
    for ns in sorted(groups):
        title = titles[ns]
        active = " active" if ns == current_ns else ""
        html_parts.append(
            f'<a class="nav-link{active}" href="{prefix}operators/{slug(ns)}/index.html">{html.escape(title)} <span class="mini">({len(groups[ns])})</span></a>'
        )
    html_parts.append("</div>")

    if current_ns and current_ns in groups:
        ns = current_ns
        title = titles[ns]
        current_items = sorted(groups[ns], key=lambda x: x.title)
        current_item = next((x for x in current_items if x.path == current_path), None)
        html_parts.append('<div class="nav-section">')
        html_parts.append(f'<p class="nav-heading">当前：{html.escape(title)}</p>')
        html_parts.append(
            f'<a class="nav-link" href="{prefix}operators/{slug(ns)}/index.html">命名空间首页</a>'
        )
        if current_item is not None:
            html_parts.append(
                f'<a class="nav-link active" href="{prefix}{current_item.path}">当前算子：{html.escape(current_item.title)}</a>'
            )
            idx = current_items.index(current_item)
            if idx > 0:
                prev_item = current_items[idx - 1]
                html_parts.append(
                    f'<a class="nav-link" href="{prefix}{prev_item.path}">上一个：{html.escape(prev_item.title)}</a>'
                )
            if idx + 1 < len(current_items):
                next_item = current_items[idx + 1]
                html_parts.append(
                    f'<a class="nav-link" href="{prefix}{next_item.path}">下一个：{html.escape(next_item.title)}</a>'
                )
        html_parts.append(
            f'<a class="nav-link" href="{prefix}operators/{slug(ns)}/index.html">查看全部 {len(current_items)} 个算子</a>'
        )
        html_parts.append("</div>")
    return "\n".join(html_parts)


def page(title: str, body: str, items: list[ApiItem], path: str, description: str = "") -> str:
    prefix = rel_prefix(path)
    parts = Path(path).parts
    current_ns = parts[1] if len(parts) >= 3 and parts[0] == "operators" else None
    return f"""<!doctype html>
<html lang="zh-CN">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{html.escape(title)} - Otters / Qust 文档</title>
    <meta name="description" content="{html.escape(description or title)}" />
    <link rel="stylesheet" href="{prefix}assets/styles.css" />
  </head>
  <body>
    <a class="skip-link" href="#main">跳到正文</a>
    <header class="topbar">
      <a class="brand" href="{prefix}index.html"><span class="brand-mark">Q</span><span>Otters / Qust 文档</span></a>
      <nav aria-label="顶部导航">
        <a href="{prefix}operators/index.html">算子总览</a>
        <a href="{prefix}operators/expr/index.html">表达式</a>
        <a href="{prefix}operators/ta/index.html">TA</a>
        <a href="{prefix}operators/monitor/index.html">Monitor</a>
        <a href="{prefix}wasm.html">Wasm</a>
      </nav>
      <label class="search"><input id="site-search" type="search" placeholder="搜索左侧导航..." /></label>
    </header>
    <div class="layout">
      <aside class="sidebar" aria-label="文档导航">
        {nav_html(items, prefix, current_ns, path)}
      </aside>
      <main id="main" class="content">
        {body}
      </main>
      <aside class="toc" aria-label="页内目录">
        <p class="toc-title">Table of contents</p>
        <div id="page-toc"></div>
      </aside>
    </div>
    <script src="{prefix}assets/app.js"></script>
  </body>
</html>
"""


def item_body(item: ApiItem, items: list[ApiItem]) -> str:
    doc = ast.get_docstring(item.node) or ""
    summary = first_sentence(doc) or f"{item.title} 是 {item.group_title} 下的算子。"
    input_text = SPECIAL_INPUTS.get((item.namespace, item.node.name), "")
    if not input_text:
        input_text = extract_doc_section(doc, SECTION_ALIASES["input"])
    if item.namespace == "monitor" and item.node.name in PLOT_INPUTS:
        input_text = PLOT_INPUTS[item.node.name]
    output_text = extract_doc_section(doc, SECTION_ALIASES["output"])
    params_text = extract_doc_section(doc, SECTION_ALIASES["params"])
    notes_text = extract_doc_section(doc, SECTION_ALIASES["notes"])
    usage = usage_for(item, input_text)
    input_cols = parse_input_columns(input_text)
    if item.namespace == "monitor":
        input_cols = parse_input_columns(PLOT_INPUTS.get(item.node.name, "")) or input_cols
    if item.namespace == "operator":
        input_cols = ["x", "y"]
    usage_cols = parse_input_columns(usage)
    if usage_cols and item.namespace not in {"dataframe", "params", "dtype", "top-level"}:
        merged = list(input_cols)
        for col_name in usage_cols:
            if col_name not in merged:
                merged.append(col_name)
        input_cols = merged
    sample_data = sample_data_for(input_cols or ["x"])
    safe_alias = slug(item.title).replace(".", "_").replace("-", "_")
    example = example_for(item, usage, sample_data, safe_alias)
    input_expr = input_expression_for(item, usage, input_cols)
    output_expr = usage
    example = inject_expression_prints(example, input_expr, output_expr)
    source_examples = source_example_html(doc)
    preview_tables = expression_preview_html(
        item,
        input_expr=input_expr,
        output_expr=output_expr,
        input_cols=input_cols,
        example=example,
    )

    param_rows = params_for(item.node)
    param_table = (
        table(
            ["参数", "类型", "默认值", "说明"],
            [
                [
                    f"<code>{html.escape(row['name'])}</code>",
                    f"<code>{html.escape(row['type'])}</code>",
                    f"<code>{html.escape(row['default'])}</code>",
                    html.escape(row["kind"]),
                ]
                for row in param_rows
            ],
        )
        if param_rows
        else "<p>无显式参数；输入来自当前表达式、绑定对象或当前命名空间。</p>"
    )
    io_rows = [
        ["输入表达式", html.escape(clean_table_text(input_text, "当前表达式，例如 col(\"x\")；具体列宽由底层算子校验。"))],
        ["返回类型", f"<code>{html.escape(return_type(item.node))}</code>"],
        ["输出语义", html.escape(clean_table_text(output_text, "返回新的表达式或 API 调用结果；列名通常由输入列名、alias 或底层算子决定。"))],
    ]
    body = f"""
<section class="page-header">
  <p class="eyebrow">Reference / {html.escape(item.group_title)}</p>
  <h1>{html.escape(item.title)}</h1>
  <p class="lead">{html.escape(summary)}</p>
  <div class="badge-row">
    <span class="badge">namespace: {html.escape(item.namespace)}</span>
    <span class="badge">returns: {html.escape(return_type(item.node))}</span>
  </div>
</section>

<section>
  <h2>用法</h2>
  {code_block("Python", usage)}
  <p>如果这个算子返回 <code>Expr</code>，通常可以继续链式调用 <code>.alias(...)</code>、<code>.over(...)</code>、<code>.rolling(...)</code> 或放入 <code>qs.select(...)</code>。</p>
</section>

<section>
  <h2>参数</h2>
  {param_table}
  {("<h3>参数补充说明</h3><pre><code>" + html.escape(params_text) + "</code></pre>") if params_text else ""}
</section>

<section>
  <h2>输入和输出</h2>
  {table(["项目", "说明"], io_rows)}
  {preview_tables}
  {source_examples}
  <h3>最小输入数据模板</h3>
  {code_block("Python", sample_data)}
  <h3>完整例子：打印输入和输出</h3>
  <p>每个算子页面都必须有这一段完整例子：先打印输入表达式和输出表达式，再构造输入数据、打印输入数据、执行算子、打印输出结果。</p>
  {code_block("完整例子 / Python", example)}
</section>

<section>
  <h2>使用场景</h2>
  <p>{html.escape(namespace_scenario(item))}</p>
  <p>当你需要 <code>{html.escape(item.title)}</code> 这种语义时，优先使用这个算子，而不是在 Python 里提前把数据取出来手写循环；这样才能保留流式状态、参数联动、Wasm 路径和 Monitor 重算能力。</p>
</section>

<section>
  <h2>为什么这样设计</h2>
  <p>{html.escape(namespace_design_reason(item))}</p>
  <p>{html.escape(namespace_benefit(item))}</p>
</section>

<section>
  <h2>注意事项</h2>
  {('<div class="callout warn"><strong>状态：</strong>这个 API 当前会抛出 <code>NotImplementedError</code>；页面示例展示的是调用后的异常形态，而不是可计算指标输出。</div>' if is_not_implemented(item) else "")}
  {("<pre><code>" + html.escape(notes_text) + "</code></pre>") if notes_text else "<ul><li>确认输入列顺序和列类型符合页面说明。</li><li>需要分组时，把 <code>over(...)</code> 放在正确层级。</li><li>需要 monitor 参数联动时，不要提前把 <code>Params</code> 转成普通 Python 数字。</li></ul>"}
</section>

<section>
  <h2>补充说明</h2>
  <details class="doc-details">
    <summary>展开查看更长说明</summary>
    {html_doc(doc)}
  </details>
</section>
"""
    return body


def operator_index_body(items: list[ApiItem]) -> str:
    namespaces: dict[str, list[ApiItem]] = {}
    titles: dict[str, str] = {}
    for item in items:
        namespaces.setdefault(item.namespace, []).append(item)
        titles[item.namespace] = item.group_title
    rows = []
    full_sections = []
    for ns in sorted(namespaces):
        ns_items = sorted(namespaces[ns], key=lambda x: x.title)
        rows.append(
            [
                f'<a href="{slug(ns)}/index.html"><code>{html.escape(ns)}</code></a>',
                html.escape(titles[ns]),
                str(len(namespaces[ns])),
                f'<a href="#all-{html.escape(slug(ns))}">查看全部 {len(ns_items)} 个独立页面</a>',
            ]
        )
        links = "\n".join(
            f'<li><a href="{html.escape(str(Path(i.path).relative_to("operators")))}"><code>{html.escape(i.title)}</code></a></li>'
            for i in ns_items
        )
        full_sections.append(
            f"""
  <section class="operator-list-block" id="all-{html.escape(slug(ns))}">
    <h3>{html.escape(titles[ns])} <span class="mini">({len(ns_items)})</span></h3>
    <ul class="operator-link-list">
      {links}
    </ul>
  </section>
"""
        )
    return f"""
<section class="page-header">
  <p class="eyebrow">Reference</p>
  <h1>算子总览</h1>
  <p class="lead">这里按“表达式 / 命名空间 / 具体算子”展开。每个算子都有独立页面，包含参数、输入输出、使用场景、用法、例子和注意事项。</p>
</section>
<section>
  <h2>命名空间</h2>
  {table(["命名空间", "说明", "算子数", "全部页面"], rows)}
</section>
<section>
  <h2>全部算子页面</h2>
  <p>下面每一项都是一个具体算子的独立 HTML 页面；命名空间首页只负责聚合和导航。</p>
  {''.join(full_sections)}
</section>
"""


def namespace_body(ns: str, ns_items: list[ApiItem]) -> str:
    title = ns_items[0].group_title
    cards = []
    for item in sorted(ns_items, key=lambda x: x.title):
        doc = ast.get_docstring(item.node) or ""
        summary = first_sentence(doc) or namespace_scenario(item)
        href = Path(item.path).name
        cards.append(
            f'<article class="card"><h3><a href="{html.escape(href)}">{html.escape(item.title)}</a></h3><p>{html.escape(summary)}</p></article>'
        )
    return f"""
<section class="page-header">
  <p class="eyebrow">Namespace</p>
  <h1>{html.escape(title)}</h1>
  <p class="lead">{html.escape(namespace_scenario(ns_items[0]))}</p>
  <div class="badge-row"><span class="badge">{len(ns_items)} operators</span><span class="badge">namespace: {html.escape(ns)}</span></div>
</section>
<section>
  <h2>算子列表</h2>
  <div class="grid operator-grid">
    {''.join(cards)}
  </div>
</section>
"""


def static_wasm_body() -> str:
    return """
<section class="page-header">
  <p class="eyebrow">Runtime</p>
  <h1>Wasm / Pyodide</h1>
  <p class="lead">Wasm 是浏览器里的 Qust：不用安装 Python，不需要本地环境，打开页面就能加载数据、写表达式、调参数、看 monitor。</p>
</section>
<section>
  <h2>为什么要有 Wasm</h2>
  <p>本地 Python 更适合大规模研究和脚本化运行；Wasm 更适合分享、演示、教学、快速调参和无环境试用。</p>
  <table><thead><tr><th>能力</th><th>能起到什么用</th></tr></thead><tbody>
  <tr><td>浏览器沙盒</td><td>代码在页面里运行，不要求用户先安装本地 Python 环境、底层依赖或项目 wheel。</td></tr>
  <tr><td>同一套表达式 API</td><td>大多数 <code>qs.select</code>、<code>qs.with_cols</code>、<code>col(...)</code>、<code>pms(...)</code>、<code>.monitor</code> 写法和本地 Python 一致。</td></tr>
  <tr><td>数据池</td><td>上传或远程读取后的 parquet 会进入页面数据池，后续 <code>load_data(...)</code> 可以直接复用。</td></tr>
  <tr><td>Monitor 交互</td><td>参数、动作、hover、zoom、slider、scatter_select callback 都可以在浏览器里直接试。</td></tr>
  </tbody></table>
  <div class="callout"><strong>设计重点：</strong>Wasm 只改变数据读取和运行位置，不改变表达式语义。Monitor 仍然消费同一套控制面事件和二进制 plot packet。</div>
</section>
<section>
  <h2>最小脚本</h2>
  <div class="code-block"><div class="code-title">Python in browser</div><pre><code>import qust as qs
from qust import col, load_data, pms

data = load_data("kline_data_all.parquet")
window = pms(2, 120).title("window").value(20).step(1)

print("输入:")
print(data)

monitor = qs.Monitor(background="black").make_grid([["price"]])
df = qs.with_cols(
    col("close").mean().rolling(window, 1).over("code").alias("ma"),
    col("datetime", "close").monitor("price").line(),
    col("datetime", "ma").monitor("price").line(),
)

page = monitor.session(df, data).params().actions().show_local()

print("输出:")
print("Monitor 页面 URL:", page)</code></pre></div>
</section>
<section>
  <h2>数据加载</h2>
  <p>浏览器不能像本地 Python 那样随便读你的磁盘路径，所以 Wasm 里数据要么来自页面数据池，要么来自 HTTP/HTTPS。</p>
  <table><thead><tr><th>写法</th><th>语义</th><th>返回</th></tr></thead><tbody>
  <tr><td><code>load_data("name.parquet")</code></td><td>读取 Wasm 数据池中已经注册的同名数据。</td><td>monitor/session 可用的数据引用。</td></tr>
  <tr><td><code>load_data("https://.../data.parquet", name="demo")</code></td><td>远程读取 parquet 并注册到数据池。</td><td>包含 dataset_id/name 的数据引用。</td></tr>
  <tr><td><code>save_data(data, "out.parquet")</code></td><td>保存已加载或已注册的数据。</td><td>保存元信息。</td></tr>
  </tbody></table>
  <div class="callout warn"><strong>Pages 环境：</strong>静态 Pages 没有本地开发代理。示例里的远程 parquet URL 会优先尝试同站点 <code>data/&lt;filename&gt;</code>，找不到才回退原始 URL。</div>
</section>
<section>
  <h2>常见使用方式</h2>
  <h3>上传数据后画线</h3>
  <div class="code-block"><div class="code-title">Python in browser</div><pre><code>import qust as qs
from qust import col, load_data

data = load_data("my_data.parquet")

df = qs.select(
    col("datetime", "close")
        .monitor
        .line()
)

df.plot(data, open_in_jupyter=False)</code></pre></div>
  <h3>用 live 参数调窗口</h3>
  <div class="code-block"><div class="code-title">Python in browser</div><pre><code>import qust as qs
from qust import col, load_data, pms

data = load_data("kline_data_all.parquet")
window = pms(2, 120).title("window").value(30).step(1)

df = qs.select(
    col.all
        .with_cols(col("close").mean().rolling(window).over("ticker").alias("ma"))
        .select(
            col("datetime", "close", "ma")
                .monitor(over_type="stack")
                .line()
                .over("ticker")
        )
)

df.plot(data, open_in_jupyter=False)</code></pre></div>
</section>
<section>
  <h2>Monitor 事务</h2>
  <p>参数变化、动作点击和 callback 都走同一套事务顺序，前端只在完整 packet 到齐后切换画面，避免半张旧图半张新图。</p>
  <ol>
    <li><code>busy=true</code></li>
    <li><code>ResetRequested</code></li>
    <li>二进制 plot packets</li>
    <li><code>busy=false</code></li>
    <li>前端 staged dashboard 原子切换</li>
  </ol>
</section>
<section>
  <h2>使用时的检查点</h2>
  <ul>
    <li>页面能打开但数据加载失败时，先确认数据是否在数据池，或者远程 URL 是否可被当前站点访问。</li>
    <li>参数拖动后图不变时，检查代码里是否真的使用了 <code>pms(...).as_expr()</code> 或把 <code>Params</code> 传进算子。</li>
    <li>callback 没结果时，检查 plot 输入或 source 是否带了过滤表达式需要的列。</li>
    <li>散点选择需要回查行情时，把 entry/exit 时间、代码、交易编号等 payload 列一起放进 scatter 或 callback 可见的数据里。</li>
  </ul>
</section>
"""


def collect_items() -> list[ApiItem]:
    specs = [
        ("col", "列选择入口", "expr.py", "_ColFactory", "col", "表达式", True),
        ("expr", "表达式核心", "expr.py", "ExprNamespace", "expr", "表达式", False),
        ("expr", "表达式核心", "expr.py", "Expr", "expr", "表达式", False),
        ("dt", "日期时间命名空间", "expr.py", "_DtNamespace", "dt", "表达式", False),
        ("math", "数学命名空间", "math.py", "Math", "math", "命名空间", False),
        ("ta", "技术指标命名空间", "ta.py", "Ta", "ta", "命名空间", False),
        ("batch_ta", "批内技术分析命名空间", "batch_ta.py", "BatchTa", "batch_ta", "命名空间", False),
        ("batch_stock", "批内股票命名空间", "batch_stock.py", "BatchStock", "batch_stock", "命名空间", False),
        ("stock", "股票统计命名空间", "stock.py", "Stock", "stock", "命名空间", False),
        ("kline", "K 线命名空间", "future.py", "Kline", "kline", "命名空间", False),
        ("stra", "策略命名空间", "future.py", "Stra", "stra", "命名空间", False),
        ("bt", "回测命名空间", "future.py", "Bt", "bt", "命名空间", False),
        ("fp", "Fast Path 命名空间", "fast_path.py", "FastPath", "fp", "命名空间", False),
        ("monitor", "Monitor 图表命名空间", "monitor.py", "MonitorNamespace", "monitor", "命名空间", True),
        ("alpha", "Alpha 命名空间", "alpha.py", "AlphaNamespace", "alpha", "命名空间", True),
        ("udf", "UDF 命名空间", "udf.py", "Udf", "udf", "命名空间", False),
        ("dataframe", "DataFrame 执行计划", "dataframe.py", "DataFrame", "dataframe", "执行计划", False),
        ("plugin", "插件 API", "plugin.py", "DataPool", "pool", "插件", False),
        ("params", "参数 API", "params.py", "Params", "params", "参数", False),
        ("dtype", "DataType API", "dtype.py", "DataType", "dtype", "UDF 类型", False),
    ]
    items: list[ApiItem] = []
    seen: set[tuple[str, str]] = set()
    for ns, title, file_name, cls, usage_prefix, group, include_call in specs:
        for node in public_methods(file_name, cls, include_call=include_call):
            display_name = f"{ns}()" if node.name == "__call__" else f"{ns}.{node.name}"
            key = (ns, display_name)
            if key in seen:
                continue
            seen.add(key)
            method_slug = "__call__" if node.name == "__call__" else slug(node.name)
            items.append(
                ApiItem(
                    group=group,
                    group_title=title,
                    namespace=ns,
                    title=display_name,
                    path=f"operators/{slug(ns)}/{method_slug}.html",
                    source_file=file_name,
                    class_name=cls,
                    node=node,
                    is_property=is_property(node),
                    usage_prefix=usage_prefix,
                    kind="method",
                )
            )
    for file_name in ["context.py", "params.py", "expr.py"]:
        for node in function_nodes(file_name):
            if node.name.startswith("_"):
                continue
            if node.name not in USER_VISIBLE_TOP_LEVEL.get(file_name, set()):
                continue
            ns = "top-level"
            title = f"qs.{node.name}"
            items.append(
                ApiItem(
                    group="顶层 API",
                    group_title="顶层 API",
                    namespace=ns,
                    title=title,
                    path=f"operators/top-level/{slug(node.name)}.html",
                    source_file=file_name,
                    class_name=None,
                    node=node,
                    is_property=False,
                    usage_prefix="qs",
                    kind="function",
                )
            )
    op_class = class_node("expr.py", "ExprNamespace")
    for node in op_class.body:
        if not isinstance(node, ast.FunctionDef):
            continue
        if node.name not in OPERATOR_USAGE:
            continue
        items.append(
            ApiItem(
                group="表达式运算符",
                group_title="表达式运算符",
                namespace="operator",
                title=OPERATOR_TITLES[node.name],
                path=f"operators/operator/{OPERATOR_SLUGS[node.name]}.html",
                source_file="expr.py",
                class_name="ExprNamespace",
                node=node,
                is_property=False,
                usage_prefix="operator",
                kind="operator",
            )
        )
    return items


def write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def validate_operator_pages(items: list[ApiItem]) -> None:
    errors: list[str] = []
    for item in items:
        path = DOCS / item.path
        if not path.exists():
            errors.append(f"{item.title}: missing page {item.path}")
            continue
        text = path.read_text(encoding="utf-8")
        required = {
            "完整例子标题": "完整例子：打印输入和输出",
            "完整例子代码块": "完整例子 / Python",
            "网页表格预览": "网页表格预览",
            "输入输出表达式表格": "输入/输出表达式表格",
            "输出表达式表格": "输出表达式表格",
            "输出结果表格": "输出结果表格",
            "Polars 风格表格": 'class="polars-frame"',
            "Polars shape": "shape: (",
            "打印输入表达式": "print(&quot;输入表达式:",
            "打印输出表达式": "print(&quot;输出表达式:",
            "打印输入": "print(&quot;输入",
            "打印输出": "print(&quot;输出",
        }
        for label, needle in required.items():
            if needle not in text:
                errors.append(f"{item.title}: missing {label}")
    index = DOCS / "operators" / "index.html"
    if index.exists():
        index_text = index.read_text(encoding="utf-8")
        link_count = index_text.count('<li><a href="')
        if link_count != len(items):
            errors.append(f"operators/index.html: expected {len(items)} operator links, got {link_count}")
    else:
        errors.append("operators/index.html: missing")
    if errors:
        sample = "\n".join(errors[:80])
        raise RuntimeError(f"operator page validation failed ({len(errors)} errors):\n{sample}")


def main() -> None:
    items = collect_items()
    ops = DOCS / "operators"
    if ops.exists():
        shutil.rmtree(ops)
    for item in items:
        write(DOCS / item.path, page(item.title, item_body(item, items), items, item.path))
    namespaces = sorted({item.namespace for item in items})
    for ns in namespaces:
        ns_items = [item for item in items if item.namespace == ns]
        path = f"operators/{slug(ns)}/index.html"
        write(DOCS / path, page(f"{ns} namespace", namespace_body(ns, ns_items), items, path))
    write(
        DOCS / "operators" / "index.html",
        page("算子总览", operator_index_body(items), items, "operators/index.html"),
    )
    write(DOCS / "wasm.html", page("Wasm / Pyodide", static_wasm_body(), items, "wasm.html"))
    validate_operator_pages(items)
    print(f"generated {len(items)} operator pages across {len(namespaces)} namespaces")


if __name__ == "__main__":
    main()
