from __future__ import annotations

import math
from typing import Callable

import polars as pl

from qust.expr import Expr, col
from qust.params import pms as _pms
from qust.udf import UdfRow

C_CODES = ['c01', 'c02', 'c03', 'c04', 'c05', 'c07', 'c08', 'c09', 'c10', 'c11', 'c12', 'c13', 'c14', 'c15', 'c16', 'c17', 'c18', 'c19', 'c20', 'c21', 'c22', 'c23', 'c24', 'c25', 'c26', 'c27', 'c28', 'c29', 'c30', 'c31', 'c32', 'c33', 'c34', 'c35', 'c36', 'c37', 'c38', 'c39', 'c41', 'c42', 'c44', 'c45', 'c46', 'c47', 'c48', 'c49', 'c50', 'c51', 'c52', 'c53', 'c54', 'c55', 'c56', 'c57', 'c58', 'c59', 'c60', 'c61', 'c62', 'c63', 'c64', 'c65', 'c66', 'c67', 'c68', 'c69', 'c70', 'c71', 'c72', 'c73', 'c74', 'c75', 'c76']
Q_CODES = ['q01', 'q02']
SF_CODES = ['sf01', 'sf02', 'sf03', 'sf04', 'sf05', 'sf06', 'sf07', 'sf08', 'sf09', 'sf10', 'sf11', 'sf12', 'sf13', 'sf14', 'sf15', 'sf16', 'sf17', 'sf18', 'sf19', 'sf20', 'sf21']
T_CODES = ['t01', 't02', 't03', 't04', 't05', 't06', 't07', 't08', 't09', 't10', 't11', 't12', 't13', 't14', 't15', 't16', 't17', 't18', 't19', 't20', 't21', 't22', 't23', 't24', 't25', 't26', 't27', 't28', 't29']
FSH_CODES = ['fshgmz1', 'fshgmz2', 'fshgmz3', 'fshgmz4', 'fshgmz5', 'fshgmz6']
ALL_CODES = C_CODES + Q_CODES + SF_CODES + T_CODES + FSH_CODES


def pms(
    title: str,
    start: int | float,
    end: int | float,
    value: int | float | None = None,
    step: int | float | None = None,
):
    p = _pms(start, end).title(title)
    if value is not None:
        p = p.value(value)
    if step is not None:
        p = p.step(step)
    return p


def _full_kline_expr() -> Expr:
    return col("datetime", "open", "high", "low", "close", "volume")


def _with_meta(e: Expr, code: str, detail: str) -> Expr:
    meta_desc = (
        f"{code.upper()} 策略\n"
        "输入列：datetime, open, high, low, close, volume\n"
        "输出列：open_long_sig, open_short_sig, exit_long_sig, exit_short_sig（bool）\n"
        f"策略说明：{detail}"
    )
    return e.select(
        "open_long_sig",
        "open_short_sig",
        "exit_long_sig",
        "exit_short_sig",
    ).with_metadata("name", code.lower()).with_metadata("description", meta_desc)


def _entry_seed_expr(
    open_long: str = "open_long_sig",
    open_short: str = "open_short_sig",
    entry_long: str = "entry_long_now",
    entry_short: str = "entry_short_now",
    alias: str = "entry_seed",
) -> Expr:
    return (
        col(
            open_long,
            entry_long,
            col(open_short, entry_short, col.null).if_else(),
        )
        .if_else()
        .alias(alias)
    )


def _state_seed_expr(
    long_cond: str,
    short_cond: str,
    long_value: float = 1.0,
    short_value: float = -1.0,
    alias: str = "kg_seed",
) -> Expr:
    return (
        col(
            long_cond,
            long_value,
            col(short_cond, short_value, col.null).if_else(),
        )
        .if_else()
        .alias(alias)
    )


def _if_chain_expr(
    pairs: list[tuple[Expr, Expr]],
    default_value: Expr | float | int | str = col.null,
    alias: str | None = None,
) -> Expr:
    out: Expr | float | int | str = default_value
    for cond, value in reversed(pairs):
        out = col(cond, value, out).if_else()
    if isinstance(out, Expr):
        return out.alias(alias) if alias else out
    res = col.lit(out)
    return res.alias(alias) if alias else res


def _bars_in_pos_expr(
    pos_col: str = "pos_pre",
    bars_col: str = "bars_since_entry",
    alias: str = "bars_in_pos",
) -> Expr:
    return col(col(pos_col) != 0.0, bars_col, col.null).if_else().alias(alias)


def _pos_from_open_raw_expr(
    open_long_col: str = "open_long_raw",
    open_short_col: str = "open_short_raw",
    alias: str = "pos_pre",
) -> Expr:
    return (
        col(open_long_col, open_short_col)
        .stra
        .to_hold_always()
        .expanding()
        .alias(alias)
    )


def _pos_from_four_raw_expr(
    open_long_col: str = "open_long_raw",
    exit_long_col: str = "exit_long_raw",
    open_short_col: str = "open_short_raw",
    exit_short_col: str = "exit_short_raw",
    alias: str = "pos",
) -> Expr:
    return (
        col(open_long_col, exit_long_col, open_short_col, exit_short_col)
        .stra
        .to_hold_two_sides()
        .expanding()
        .alias(alias)
    )


def _four_signals_from_pos_expr(
    pos_col: str = "pos_pre",
) -> Expr:
    return col(pos_col).stra.hold_to_four_signals().expanding()


def _open_pre_exprs(
    open_long_sig: str = "open_long_sig",
    open_short_sig: str = "open_short_sig",
    open_long_pre: str = "open_long_pre",
    open_short_pre: str = "open_short_pre",
) -> tuple[Expr]:
    return (
        col(open_long_sig, open_short_sig).alias(open_long_pre, open_short_pre),
    )


def _entry_now_exprs(
    open_long_pre: str = "open_long_pre",
    open_short_pre: str = "open_short_pre",
    entry_price_col: str = "open",
    entry_long_now: str = "entry_long_now",
    entry_short_now: str = "entry_short_now",
    bars_pos: str = "bars_pos",
) -> tuple[Expr, Expr, Expr]:
    return (
        col(open_long_pre, entry_price_col, col.null).if_else().alias(entry_long_now),
        col(open_short_pre, entry_price_col, col.null).if_else().alias(entry_short_now),
        col(open_long_pre, open_short_pre).any(axis=1).fill_null(False).count_last().expanding().alias(bars_pos),
    )


def _bars_pos_from_open_sig_expr(
    open_long_pre: str = "open_long_sig",
    open_short_pre: str = "open_short_sig",
    bars_pos: str = "bars_pos",
) -> Expr:
    return col(open_long_pre, open_short_pre).any(axis=1).fill_null(False).count_last().expanding().alias(bars_pos)


def _false_exit_pre_exprs(
    exit_long_pre: str = "exit_long_pre",
    exit_short_pre: str = "exit_short_pre",
) -> tuple[Expr, Expr]:
    return (
        col.lit(False).alias(exit_long_pre),
        col.lit(False).alias(exit_short_pre),
    )


def _pick_by_pos_expr(
    pos_col: str,
    long_value: Expr | float | int,
    short_value: Expr | float | int,
    default_value: Expr | float | int = 0.0,
    alias: str | None = None,
) -> Expr:
    e = col(
        col(pos_col) == 1.0,
        long_value,
        col(col(pos_col) == -1.0, short_value, default_value).if_else(),
    ).if_else()
    return e.alias(alias) if alias else e


def _exit_trailing_long_expr(
    pos_col: str = "pos_pre",
    bars_col: str = "bars_pos",
    entry_col: str = "entry_long",
    best_col: str = "highest_after_entry",
    best_prev_col: str = "highest_after_entry_1",
    close_prev_col: str = "close_1",
    trigger_col: str = "low",
    start_col: str = "trailing_start",
    stop_col: str = "trailing_stop",
    start_expr: Expr | None = None,
    stop_expr: Expr | None = None,
    alias: str = "exit_long_raw",
) -> Expr:
    start_v = start_expr if start_expr is not None else col(start_col)
    stop_v = stop_expr if stop_expr is not None else col(stop_col)
    fixed_stop = col(close_prev_col) <= col(entry_col) - col(entry_col) * stop_v / 1000.0
    trail_start = col(best_col) >= col(entry_col) * (1.0 + start_v / 1000.0)
    trail_hit = col(trigger_col) <= col(best_prev_col) - col(entry_col) * stop_v / 1000.0
    return col(
        col(pos_col) == 1.0,
        col(bars_col) > 0,
        col(fixed_stop, col(trail_start, trail_hit).all(axis=1)).any(axis=1),
    ).all(axis=1).fill_null(False).alias(alias)


def _exit_trailing_short_expr(
    pos_col: str = "pos_pre",
    bars_col: str = "bars_pos",
    entry_col: str = "entry_short",
    best_col: str = "lowest_after_entry",
    best_prev_col: str = "lowest_after_entry_1",
    close_prev_col: str = "close_1",
    trigger_col: str = "high",
    start_col: str = "trailing_start",
    stop_col: str = "trailing_stop",
    start_expr: Expr | None = None,
    stop_expr: Expr | None = None,
    alias: str = "exit_short_raw",
) -> Expr:
    start_v = start_expr if start_expr is not None else col(start_col)
    stop_v = stop_expr if stop_expr is not None else col(stop_col)
    fixed_stop = col(close_prev_col) >= col(entry_col) + col(entry_col) * stop_v / 1000.0
    trail_start = col(best_col) <= col(entry_col) * (1.0 - start_v / 1000.0)
    trail_hit = col(trigger_col) >= col(best_prev_col) + col(entry_col) * stop_v / 1000.0
    return col(
        col(pos_col) == -1.0,
        col(bars_col) > 0,
        col(fixed_stop, col(trail_start, trail_hit).all(axis=1)).any(axis=1),
    ).all(axis=1).fill_null(False).alias(alias)


def build_c01_expr() -> Expr:
    stoploss_bp = pms("c01_stoploss_bp", 1, 100, 15).limit(20)
    range_bp = pms("c01_range_bp", 1, 60, 5).limit(20)
    trade_hhmm = pms("c01_trade_hhmm", 0, 2359, 904).limit(20)
    force_flat_hhmm = pms("c01_force_flat_hhmm", 0, 2359, 1458).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            (col("datetime").dt.hour() * 100 + col("datetime").dt.minute()).alias("hhmm"),
            col("open").first_value().expanding().over(col("datetime").dt.date()).alias("day_open"),
        )
        .with_cols(
            (
                (col("hhmm") == trade_hhmm)
                & (((col("open") - col("day_open")) / col("day_open")) > (range_bp * 0.001))
            )
            .fill_null(False)
            .alias("open_long_cond"),
            (
                (col("hhmm") == trade_hhmm)
                & (((col("open") - col("day_open")) / col("day_open")) < (range_bp * -0.001))
            )
            .fill_null(False)
            .alias("open_short_cond"),
            (col("hhmm") >= force_flat_hhmm).fill_null(False).alias("force_exit_cond"),
        )
        .with_cols(
            col("open_long_cond", "open_short_cond").shift(1).expanding().fill_null(False).alias("open_long_raw", "open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "force_exit_cond", "entry_long_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "force_exit_cond", "entry_short_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("close") < (col("entry_long") * (1.0 - stoploss_bp * 0.001)))
            )
            .fill_null(False)
            .alias("exit_long_stop_cond"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("close") > (col("entry_short") * (1.0 + stoploss_bp * 0.001)))
            )
            .fill_null(False)
            .alias("exit_short_stop_cond"),
        )
        .with_cols(
            col("exit_long_stop_cond", "force_exit_cond").any(axis=1).fill_null(False).alias("exit_long_cond"),
            col("exit_short_stop_cond", "force_exit_cond").any(axis=1).fill_null(False).alias("exit_short_cond"),
        )
        .with_cols(
            col("exit_long_cond").shift(1).expanding().fill_null(False).alias("exit_long_raw"),
            col("exit_short_cond").shift(1).expanding().fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c01", "按 C01 源码实现：09:04 与当日开盘偏离触发开仓，14:58 后强制平仓，并按开仓价百分比止损。")

def build_c02_expr() -> Expr:
    len1 = pms("c02_len1", 5, 120, 20).limit(20)
    len2 = pms("c02_len2", 5, 120, 33).limit(20)
    len4 = pms("c02_len4", 2, 40, 6).limit(20)
    len5 = pms("c02_len5", 5, 120, 22).limit(20)
    len6 = pms("c02_len6", 5, 120, 20).limit(20)
    len7 = pms("c02_len7", 5, 240, 50).limit(20)
    stop_scale = pms("c02_stop_scale", 0.2, 5.0, 1.0).limit(20)
    start_hhmm = pms("c02_start_hhmm", 0, 2359, 930).limit(20)
    last_entry_hhmm = pms("c02_last_entry_hhmm", 0, 2359, 1450).limit(20)
    force_flat_hhmm = pms("c02_force_flat_hhmm", 0, 2359, 1458).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            (col("datetime").dt.hour() * 100 + col("datetime").dt.minute()).alias("hhmm"),
            col("high").max().rolling(len1).alias("up_band_raw"),
            col("low").min().rolling(len1).alias("dn_band_raw"),
            col("close").mean().rolling(len4).alias("ma_fast"),
            col("close").mean().rolling(len5).alias("ma_slow"),
            col("high", "low", "close").ta.adx(20).expanding().alias("adx_20"),
            col("high", "low", "close").ta.trange().expanding().alias("tr"),
        )
        .with_cols(
            col('up_band_raw', 'dn_band_raw', 'close', 'ma_fast', 'ma_slow').shift(1).expanding().alias('up_band', 'dn_band', 'close_1', 'ma_fast_1', 'ma_slow_1'),
            col("tr").mean().rolling(len6).alias("atr"),
        )
        .with_cols(
            col("atr").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("atr_1").mean().rolling(len7).alias("atr_ref"),
            col('up_band', 'dn_band').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("hhmm") > start_hhmm)
                & (col("hhmm") < last_entry_hhmm)
                & (col("close_1") <= col("up_band_1"))
                & (col("close") > col("up_band"))
                & (col("adx_20") > len2)
                & (col("ma_fast") > col("ma_slow"))
                & (col("atr") > col("atr_ref"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("hhmm") > start_hhmm)
                & (col("hhmm") < last_entry_hhmm)
                & (col("close_1") >= col("dn_band_1"))
                & (col("close") < col("dn_band"))
                & (col("adx_20") > len2)
                & (col("ma_fast") < col("ma_slow"))
                & (col("atr") > col("atr_ref"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            (col("up_band") - col("dn_band")).abs().alias("band_w"),
            (col("hhmm") >= force_flat_hhmm).fill_null(False).alias("force_flat"),
            (col("ma_fast") < col("ma_slow")).fill_null(False).alias("exit_long_flip"),
            (col("ma_fast") > col("ma_slow")).fill_null(False).alias("exit_short_flip"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("close") <= (col("entry_long") - col("band_w") * stop_scale))
            )
            .fill_null(False)
            .alias("exit_long_sl"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("close") >= (col("entry_short") + col("band_w") * stop_scale))
            )
            .fill_null(False)
            .alias("exit_short_sl"),
        )
        .with_cols(
            col("exit_long_flip", "exit_long_sl", "force_flat").any(axis=1).fill_null(False).alias("exit_long_raw"),
            col("exit_short_flip", "exit_short_sl", "force_flat").any(axis=1).fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c02", "按 C02 源码主逻辑实现：日内上/下轨突破+ADX/均线/ATR过滤开仓，均线反转、带宽止损与尾盘强平离场。")

def build_c03_expr() -> Expr:
    length_short = pms("c03_length_short", 2, 80, 2).limit(20)
    length_long = pms("c03_length_long", 5, 200, 5).limit(20)
    bias_length = pms("c03_bias_length", 1, 50, 1).limit(20)
    n = pms("c03_bias_smooth_n", 1, 20, 1).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").ta.ema(length_short).expanding().alias("ema_short"),
            col("close").ta.ema(length_long).expanding().alias("ema_long"),
        )
        .with_cols(
            col('ema_short', 'ema_long').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("ema_short") - col("ema_long")).alias("value1"),
            ((col("ema_short") > col("ema_long")) & (col("ema_short_1") <= col("ema_long_1")))
            .fill_null(False)
            .alias("cross_up"),
            ((col("ema_short") < col("ema_long")) & (col("ema_short_1") >= col("ema_long_1")))
            .fill_null(False)
            .alias("cross_down"),
            (col("ema_short") > col("ema_long")).fill_null(False).alias("long_regime"),
            (col("ema_short") < col("ema_long")).fill_null(False).alias("short_regime"),
        )
        .with_cols(
            col("value1").ta.ema(n).expanding().alias("bias"),
        )
        .with_cols(
            col("bias").shift(1).expanding().add_suffix("1"),
            col("bias").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            (
                col("long_regime")
                & (col("bias_2") > col("bias_1"))
                & (col("bias_1") < col("bias"))
                & (col("bias") < bias_length)
            )
            .fill_null(False)
            .alias("open_long_cond"),
            (
                col("short_regime")
                & (col("bias_2") < col("bias_1"))
                & (col("bias_1") > col("bias"))
                & (col("bias") < bias_length)
            )
            .fill_null(False)
            .alias("open_short_cond"),
        )
        .with_cols(
            col("open_long_cond", "open_short_cond").shift(1).expanding().fill_null(False).alias("open_long_raw", "open_short_raw"),
            col("cross_down", "cross_up").shift(1).expanding().fill_null(False).alias("exit_long_raw", "exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c03", "按 C03 源码实现：短长 EMA 交叉定义趋势区间，结合 Bias 形态确认开仓，反向交叉平仓。")

def build_c04_expr() -> Expr:
    length1 = pms("c04_length1", 5, 200, 5).limit(20)
    s1 = pms("c04_s1", 1, 20, 1).limit(20)
    s2 = pms("c04_s2", 1, 20, 1).limit(20)
    start_per1 = pms("c04_start_per1", 1.0, 200.0, 1.0).limit(20)
    stop_per1 = pms("c04_stop_per1", 1.0, 200.0, 1.0).limit(20)
    start_per2 = pms("c04_start_per2", 1.0, 200.0, 1.0).limit(20)
    stop_per2 = pms("c04_stop_per2", 1.0, 200.0, 1.0).limit(20)
    start_per3 = pms("c04_start_per3", 1.0, 300.0, 1.0).limit(20)
    stop_per3 = pms("c04_stop_per3", 1.0, 200.0, 1.0).limit(20)
    start_per4 = pms("c04_start_per4", 1.0, 500.0, 1.0).limit(20)
    stop_per4 = pms("c04_stop_per4", 1.0, 200.0, 1.0).limit(20)
    stoploss = pms("c04_stoploss", 0.5, 50.0, 0.5).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").mean().rolling(length1).alias("ma1"),
            col('close', 'high', 'low', 'open').shift(1).expanding().add_suffix("1"),
            col.lit(stoploss).alias("stoploss"),
        )
        .with_cols(
            col("ma1").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("close_1") > col("ma1_1")).fill_null(False).alias("above_ma_1"),
            (col("close_1") < col("ma1_1")).fill_null(False).alias("below_ma_1"),
        )
        .with_cols(
            col(
                col("above_ma_1").to(pl.UInt32).alias("above_cnt_s1"),
                col("below_ma_1").to(pl.UInt32).alias("below_cnt_s1"),
            ).sum().rolling(s1),
            col(
                col("above_ma_1").to(pl.UInt32).alias("above_cnt_s2"),
                col("below_ma_1").to(pl.UInt32).alias("below_cnt_s2"),
            ).sum().rolling(s2),
        )
        .with_cols(
            (col("above_cnt_s1") == s1).fill_null(False).alias("open_long_raw"),
            (col("below_cnt_s1") == s1).fill_null(False).alias("open_short_raw"),
            (col("below_cnt_s2") == s2).fill_null(False).alias("exit_long_base"),
            (col("above_cnt_s2") == s2).fill_null(False).alias("exit_short_base"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "high_1", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("best_long"),
            col("open_short_pre", "exit_short_pre", "low_1", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("best_short"),
        )
        .with_cols(
            col("open_long_pre", "close", "low")
            .stra
            .exit_by_profit_drawdown(start_per1, stop_per1)
            .expanding()
            .fill_null(False)
            .alias("exit_long_tr1"),
            col("open_short_pre", "close", "high")
            .stra
            .exit_by_profit_drawdown(start_per1, stop_per1, True)
            .expanding()
            .fill_null(False)
            .alias("exit_short_tr1"),
            col("open_long_pre", "close", "low")
            .stra
            .exit_by_profit_drawdown(start_per2, stop_per2)
            .expanding()
            .fill_null(False)
            .alias("exit_long_tr2"),
            col("open_short_pre", "close", "high")
            .stra
            .exit_by_profit_drawdown(start_per2, stop_per2, True)
            .expanding()
            .fill_null(False)
            .alias("exit_short_tr2"),
            col("open_long_pre", "close", "low")
            .stra
            .exit_by_profit_drawdown(start_per3, stop_per3)
            .expanding()
            .fill_null(False)
            .alias("exit_long_tr3"),
            col("open_short_pre", "close", "high")
            .stra
            .exit_by_profit_drawdown(start_per3, stop_per3, True)
            .expanding()
            .fill_null(False)
            .alias("exit_short_tr3"),
            col("open_long_pre", "close", "low")
            .stra
            .exit_by_profit_drawdown(start_per4, stop_per4)
            .expanding()
            .fill_null(False)
            .alias("exit_long_tr4"),
            col("open_short_pre", "close", "high")
            .stra
            .exit_by_profit_drawdown(start_per4, stop_per4, True)
            .expanding()
            .fill_null(False)
            .alias("exit_short_tr4"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("low") <= (col("entry_long") * (1.0 - col("stoploss") * 0.01)))
            )
            .fill_null(False)
            .alias("exit_long_sl"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("high") >= (col("entry_short") * (1.0 + col("stoploss") * 0.01)))
            )
            .fill_null(False)
            .alias("exit_short_sl"),
        )
        .with_cols(
            col("exit_long_base", "exit_long_tr1", "exit_long_tr2", "exit_long_tr3", "exit_long_tr4", "exit_long_sl").any(axis=1)
            .fill_null(False)
            .alias("exit_long_raw"),
            col("exit_short_base", "exit_short_tr1", "exit_short_tr2", "exit_short_tr3", "exit_short_tr4", "exit_short_sl").any(axis=1)
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c04", "按 C04 源码实现：均线连续判定开平仓，并叠加四级盈利回撤止盈与固定百分比止损。")

def build_c05_expr() -> Expr:
    fast_length = pms("c05_fast_length", 2, 60, 12).limit(20)
    slow_length = pms("c05_slow_length", 4, 120, 26).limit(20)
    macd_length = pms("c05_macd_length", 1, 60, 9).limit(20)
    l1 = pms("c05_l1", 5, 300, 50).limit(20)
    l2 = pms("c05_l2", 10, 400, 120).limit(20)
    stoploss = pms("c05_stoploss", 0.5, 50.0, 5.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").ta.ema(fast_length).expanding().alias("ema_fast"),
            col("close").ta.ema(slow_length).expanding().alias("ema_slow"),
            col("close").mean().rolling(l1).alias("dma1"),
            col("close").mean().rolling(l2).alias("dma2"),
            col.lit(stoploss).alias("stoploss"),
        )
        .with_cols(
            (col("ema_fast") - col("ema_slow")).alias("macd_value"),
        )
        .with_cols(
            col("macd_value").ta.ema(macd_length).expanding().alias("avg_macd"),
            col("close", "open").shift(1).expanding().add_suffix("1"),
            col("close").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            (col("macd_value") - col("avg_macd")).alias("macd_diff"),
            col('macd_value', 'dma1', 'dma2').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("macd_value_1") > 0.0)
                & (col("dma1_1") > col("dma2_1"))
                & (col("macd_diff").shift(1).expanding() > 0.0)
                & (col("close_1") > col("dma1_1"))
                & (col("close_2") > col("dma1").shift(2).expanding())
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("macd_value_1") < 0.0)
                & (col("dma1_1") < col("dma2_1"))
                & (col("macd_diff").shift(1).expanding() < 0.0)
                & (col("close_1") < col("dma1_1"))
                & (col("close_2") < col("dma1").shift(2).expanding())
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
        )
        .with_cols(
            ((col("macd_value_1") < 0.0) | (col("dma1_1") < col("dma2_1"))).fill_null(False).alias("exit_long_base"),
            ((col("macd_value_1") > 0.0) | (col("dma1_1") > col("dma2_1"))).fill_null(False).alias("exit_short_base"),
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("low") <= (col("entry_long") * (1.0 - col("stoploss") * 0.01)))
            )
            .fill_null(False)
            .alias("exit_long_sl"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("high") >= (col("entry_short") * (1.0 + col("stoploss") * 0.01)))
            )
            .fill_null(False)
            .alias("exit_short_sl"),
        )
        .with_cols(
            col("exit_long_base", "exit_long_sl").any(axis=1).fill_null(False).alias("exit_long_raw"),
            col("exit_short_base", "exit_short_sl").any(axis=1).fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c05", "按 C05 源码实现：MACD 与双均线共振开仓，趋势反转平仓并叠加固定比例止损。")

def build_c07_expr() -> Expr:
    stoploss = pms("c07_stoploss", 1.0, 30.0, 1.0).limit(20)
    m = pms("c07_m", 5, 120, 5).limit(20)
    min_n = pms("c07_min_n", 5, 200, 5).limit(20)
    max_n = pms("c07_max_n", 10, 300, 10).limit(20)

    class _C07DynN(UdfRow):
        def init_start(self):
            self.last_n = None
            self.curr_n = 0

        def output_schema(self, input_schema):
            return [("dynamic_n", pl.UInt32)]

        def update(self, delta, min_n, max_n):
            if min_n is None or max_n is None:
                return
            min_n_v = int(min_n)
            max_n_v = int(max_n)
            if self.last_n is None:
                self.last_n = float(min_n_v)
            if delta is not None:
                self.last_n = self.last_n * (1.0 + float(delta))
            self.last_n = max(float(min_n_v), min(float(max_n_v), self.last_n))
            self.curr_n = int(self.last_n)

        def calc(self):
            return [self.curr_n]

    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col.lit(stoploss).alias("stoploss"),
            col("close").std().rolling(m).alias("volatility"),
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("volatility").shift(1).expanding().alias("vol_1"),
        )
        .with_cols(
            ((col("volatility") - col("vol_1")) / col("volatility")).fill_nan(0.0).fill_null(0.0).alias("delta_vol"),
        )
        .with_cols(
            col("delta_vol", min_n, max_n)
            .udf.row(
                _C07DynN(),
            )
            .expanding()
            .alias("dynamic_n"),
        )
        .with_cols(
            col("close").mean().rolling_dynamic("dynamic_n", window_max=8192, min_samples=1).alias("midband"),
            col("close").std().rolling_dynamic("dynamic_n", window_max=8192, min_samples=1).alias("std_n"),
            col("high").max().rolling_dynamic("dynamic_n", window_max=8192, min_samples=1).alias("hh"),
            col("low").min().rolling_dynamic("dynamic_n", window_max=8192, min_samples=1).alias("ll"),
        )
        .with_cols(
            (col("midband") + 2.0 * col("std_n")).alias("upband"),
            (col("midband") - 2.0 * col("std_n")).alias("downband"),
            col('midband', 'hh', 'll').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col('upband', 'downband').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("high_1") > col("upband_1")) & (col("high_1") >= col("hh_1"))).fill_null(False).alias("open_long_raw"),
            ((col("low_1") < col("downband_1")) & (col("low_1") <= col("ll_1"))).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "high", "entry_long").stra.t02_track_best_since_entry(False).expanding().alias("highest_high_after_entry"),
            col("open_long_pre", "exit_long_pre", "close", "entry_long").stra.t02_track_best_since_entry(False).expanding().alias("highest_close_after_entry"),
            col("open_short_pre", "exit_short_pre", "low", "entry_short").stra.t02_track_best_since_entry(True).expanding().alias("lowest_low_after_entry"),
            col("open_short_pre", "exit_short_pre", "close", "entry_short").stra.t02_track_best_since_entry(True).expanding().alias("lowest_close_after_entry"),
        )
        .with_cols(
            col('highest_high_after_entry', 'highest_close_after_entry', 'lowest_low_after_entry', 'lowest_close_after_entry').shift(1).expanding().add_suffix("1"),
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("close_1") < col("midband_1"))
            )
            .fill_null(False)
            .alias("exit_long_base"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("close_1") > col("midband_1"))
            )
            .fill_null(False)
            .alias("exit_short_base"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("highest_high_after_entry_1") > (col("entry_long") * (1.0 + 2.0 * 0.01 * col("stoploss"))))
                & (col("close_1") < (col("highest_close_after_entry_1") * (1.0 - 0.01 * col("stoploss"))))
            )
            .fill_null(False)
            .alias("exit_long_trail"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("lowest_low_after_entry_1") < (col("entry_short") * (1.0 - 2.0 * 0.01 * col("stoploss"))))
                & (col("close_1") > (col("lowest_close_after_entry_1") * (1.0 + 0.01 * col("stoploss"))))
            )
            .fill_null(False)
            .alias("exit_short_trail"),
        )
        .with_cols(
            col("exit_long_base", "exit_long_trail").any(axis=1).fill_null(False).alias("exit_long_raw"),
            col("exit_short_base", "exit_short_trail").any(axis=1).fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c07", "按 C07 源码主逻辑实现：波动率驱动动态周期布林通道开仓，持仓后中轨反向与“入场后极值+收盘回撤”动态止盈离场。")

def build_c08_expr() -> Expr:
    length = pms("c08_length", 20, 500, 20).limit(20)
    start_pro1 = pms("c08_start_pro1", 1.0, 50.0, 1.0).limit(20)
    stop_pro1 = pms("c08_stop_pro1", 1.0, 200.0, 1.0).limit(20)
    start_pro2 = pms("c08_start_pro2", 1.0, 80.0, 1.0).limit(20)
    stop_pro2 = pms("c08_stop_pro2", 1.0, 200.0, 1.0).limit(20)
    start_pro3 = pms("c08_start_pro3", 1.0, 120.0, 1.0).limit(20)
    stop_pro3 = pms("c08_stop_pro3", 1.0, 100.0, 1.0).limit(20)
    stoploss = pms("c08_stoploss", 0.5, 50.0, 0.5).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("datetime").dt.date().alias("day"),
            col.lit(length).to(pl.UInt32).alias("length_u"),
            col.lit(stoploss).alias("stoploss"),
        )
        .with_cols(
            col(
                col("open").first_value().alias("day_open"),
                col("high").max().alias("day_high_cum"),
                col("low").min().alias("day_low_cum"),
            ).expanding().over(col("day")),
            col("close").mean().rolling(length).alias("ma1"),
        )
        .with_cols(
            col('day', 'day_high_cum', 'day_low_cum').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("day") != col("day_1")).fill_null(True).alias("is_new_day"),
        )
        .with_cols(
            col("is_new_day", "day_high_cum_1", col.null).if_else().alias("prev_day_high_seed"),
            col("is_new_day", "day_low_cum_1", col.null).if_else().alias("prev_day_low_seed"),
        )
        .with_cols(
            col("prev_day_high_seed").ffill().expanding().alias("prev_day_high"),
            col("prev_day_low_seed").ffill().expanding().alias("prev_day_low"),
            col('close', 'high', 'low', 'open', 'ma1').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("prev_day_high") - col("prev_day_low")).alias("band"),
        )
        .with_cols(
            (col("day_open") + col("band")).alias("upline"),
            (col("day_open") - col("band")).alias("lowline"),
        )
        .with_cols(
            col('upline', 'lowline').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("close_1") > col("upline_1")) & (col("upline_1") > col("ma1_1")))
            .fill_null(False)
            .alias("open_long_raw"),
            ((col("close_1") < col("lowline_1")) & (col("lowline_1") < col("ma1_1")))
            .fill_null(False)
            .alias("open_short_raw"),
            ((col("close_1") < col("lowline_1")) | (col("close_1") < col("ma1_1")))
            .fill_null(False)
            .alias("exit_long_base"),
            ((col("close_1") > col("upline_1")) | (col("close_1") > col("ma1_1")))
            .fill_null(False)
            .alias("exit_short_base"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "high_1", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("best_long"),
            col("open_short_pre", "exit_short_pre", "low_1", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("best_short"),
        )
        .with_cols(
            col("open_long_pre", "close", "low")
            .stra
            .exit_by_profit_drawdown(start_pro1, stop_pro1)
            .expanding()
            .fill_null(False)
            .alias("exit_long_tr1"),
            col("open_short_pre", "close", "high")
            .stra
            .exit_by_profit_drawdown(start_pro1, stop_pro1, True)
            .expanding()
            .fill_null(False)
            .alias("exit_short_tr1"),
            col("open_long_pre", "close", "low")
            .stra
            .exit_by_profit_drawdown(start_pro2, stop_pro2)
            .expanding()
            .fill_null(False)
            .alias("exit_long_tr2"),
            col("open_short_pre", "close", "high")
            .stra
            .exit_by_profit_drawdown(start_pro2, stop_pro2, True)
            .expanding()
            .fill_null(False)
            .alias("exit_short_tr2"),
            col("open_long_pre", "close", "low")
            .stra
            .exit_by_profit_drawdown(start_pro3, stop_pro3)
            .expanding()
            .fill_null(False)
            .alias("exit_long_tr3"),
            col("open_short_pre", "close", "high")
            .stra
            .exit_by_profit_drawdown(start_pro3, stop_pro3, True)
            .expanding()
            .fill_null(False)
            .alias("exit_short_tr3"),
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("low") <= (col("entry_long") * (1.0 - col("stoploss") * 0.01)))
            )
            .fill_null(False)
            .alias("exit_long_sl"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("high") >= (col("entry_short") * (1.0 + col("stoploss") * 0.01)))
            )
            .fill_null(False)
            .alias("exit_short_sl"),
        )
        .with_cols(
            col("exit_long_base", "exit_long_tr1", "exit_long_tr2", "exit_long_tr3", "exit_long_sl").any(axis=1)
            .fill_null(False)
            .alias("exit_long_raw"),
            col("exit_short_base", "exit_short_tr1", "exit_short_tr2", "exit_short_tr3", "exit_short_sl").any(axis=1)
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c08", "按 C08 源码实现：前日高低区间+均线过滤开仓，基础反向平仓并叠加三级回撤止盈与固定止损。")

def build_c09_expr() -> Expr:
    length1 = pms("c09_length1", 2, 50, 2).limit(20)
    length2 = pms("c09_length2", 5, 120, 5).limit(20)
    n = pms("c09_n", 0.5, 3.0, 0.5).limit(20)
    trailing_ratio = pms("c09_trailing_ratio", 0.001, 0.12, 0.001).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("high", "low", "close").ta.atr(length1).expanding().alias("atr1"),
            col("high", "low", "close").ta.atr(length2).expanding().alias("atr2"),
        )
        .with_cols(
            (col("atr1") / col("atr2")).alias("kk"),
            col("close").shift(1).expanding().add_suffix("1"),
            col("close").shift(5).expanding().add_suffix("5"),
            col('high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("kk") > n) & (col("high") >= col("high_1")) & (col("close_1") > col("close_5")))
            .fill_null(False)
            .alias("open_long_raw"),
            ((col("kk") > n) & (col("low") <= col("low_1")) & (col("close_1") < col("close_5")))
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
        )
        .with_cols(
            col("open_long_pre", "open", "high", "low", "close")
            .stra
            .exit_by_price_sby(trailing_ratio, True)
            .expanding()
            .fill_null(False)
            .alias("exit_long_raw"),
            col("open_short_pre", "open", "high", "low", "close")
            .stra
            .exit_by_price_sby(trailing_ratio, False)
            .expanding()
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c09", "按 C09 源码实现：ATR 比值放大下的方向突破开仓，离场使用 `exit_by_price_sby` 实现开仓后极值回撤止盈。")

def build_c10_expr() -> Expr:
    nma = pms("c10_nma", 10, 400, 10).limit(20)
    n = pms("c10_n", 2, 120, 2).limit(20)
    start_pro = pms("c10_start_pro", 1.0, 40.0, 1.0).limit(20)
    stop_pro = pms("c10_stop_pro", 1.0, 40.0, 1.0).limit(20)
    stoploss = pms("c10_stoploss", 0.5, 50.0, 0.5).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col.lit(stoploss).alias("stoploss"),
            col("high", "low", "close").ta.atr(14).expanding().alias("atr"),
        )
        .with_cols(
            col("open").mean().rolling(nma).alias("mao"),
            col("close").mean().rolling(nma).alias("mac"),
            col("high").max().rolling(n).alias("hhn"),
            col("low").min().rolling(n).alias("lln"),
        )
        .with_cols(
            col('mao', 'mac', 'hhn', 'lln', 'high', 'low', 'atr').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("mao_1") < col("mac_1")) & (col("high_1") > col("mac_1"))).fill_null(False).alias("setup_long_open"),
            ((col("mao_1") > col("mac_1")) & (col("low_1") < col("mac_1"))).fill_null(False).alias("setup_short_open"),
            ((col("mao_1") > col("mac_1")) & (col("low_1") < col("mac_1"))).fill_null(False).alias("setup_long_exit"),
            ((col("mao_1") < col("mac_1")) & (col("high_1") > col("mac_1"))).fill_null(False).alias("setup_short_exit"),
        )
        .with_cols(
            _pos_from_four_raw_expr("setup_long_open", "setup_long_exit", "setup_short_open", "setup_short_exit", "setup_side"),
        )
        .with_cols(
            (
                (col("setup_side") == 1.0)
                & (col("high_1") >= col("hhn_1"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("setup_side") == -1.0)
                & (col("low_1") <= col("lln_1"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "high_1", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("best_long"),
            col("open_short_pre", "exit_short_pre", "low_1", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("best_short"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 1)
                & (col("best_long") >= (col("entry_long") + start_pro * col("atr_1")))
                & (col("low_1") <= (col("best_long") - stop_pro * col("atr_1")))
            )
            .fill_null(False)
            .alias("exit_long_tr"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 1)
                & (col("best_short") <= (col("entry_short") - start_pro * col("atr_1")))
                & (col("high_1") >= (col("best_short") + stop_pro * col("atr_1")))
            )
            .fill_null(False)
            .alias("exit_short_tr"),
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("low") <= (col("entry_long") * (1.0 - col("stoploss") * 0.01)))
            )
            .fill_null(False)
            .alias("exit_long_sl"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("high") >= (col("entry_short") * (1.0 + col("stoploss") * 0.01)))
            )
            .fill_null(False)
            .alias("exit_short_sl"),
        )
        .with_cols(
            col("exit_long_tr", "exit_long_sl").any(axis=1).fill_null(False).alias("exit_long_raw"),
            col("exit_short_tr", "exit_short_sl").any(axis=1).fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c10", "按 C10 源码实现：均线过滤后的高低突破开仓，ATR 启动回撤止盈并叠加固定比例止损。")

def build_c11_expr() -> Expr:
    length = pms("c11_length", 2, 120, 2).limit(20)
    pcnt = pms("c11_pcnt", 0.05, 10.0, 0.05).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col.lit(length).to(pl.UInt32).alias("length_u"),
            col('high', 'low', 'open', 'close').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("high_1", "low_1", "open_1", "close_1").mean(axis=1).alias("tprice"),
            col("close").std().rolling(10).alias("stds"),
            col("close").std().rolling(60).alias("stdl"),
            col("high", "low", "close").ta.atr(length).expanding().alias("atr_len"),
        )
        .with_cols(
            col("tprice").mean().rolling(length).alias("avg_value"),
            col('atr_len', 'stds', 'stdl').shift(1).expanding().alias('shift_value_1', 'stds_1', 'stdl_1'),
        )
        .with_cols(
            (col("avg_value") + col("shift_value_1")).alias("upper_band"),
            (col("avg_value") - col("shift_value_1")).alias("lower_band"),
        )
        .with_cols(
            (
                (col("high") >= col("upper_band"))
                & (col("stds_1") >= (col("stdl_1") * 0.8))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("low") <= col("lower_band"))
                & (col("stds_1") >= (col("stdl_1") * 0.8))
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "close_1", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("best_long_close"),
            col("open_short_pre", "exit_short_pre", "close_1", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("best_short_close"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (((col("best_long_close") - col("entry_long")) / col("close_1")) >= 0.2)
                & (col("close_1") < col("lower_band"))
            )
            .fill_null(False)
            .alias("exit_long_tp"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (((col("entry_short") - col("best_short_close")) / col("close_1")) >= 0.2)
                & (col("close_1") > col("upper_band"))
            )
            .fill_null(False)
            .alias("exit_short_tp"),
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("close_1") <= (col("entry_long") * (1.0 - pcnt * 0.01)))
            )
            .fill_null(False)
            .alias("exit_long_sl"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("close_1") >= (col("entry_short") * (1.0 + pcnt * 0.01)))
            )
            .fill_null(False)
            .alias("exit_short_sl"),
        )
        .with_cols(
            col("exit_long_tp", "exit_long_sl").any(axis=1).fill_null(False).alias("exit_long_raw"),
            col("exit_short_tp", "exit_short_sl").any(axis=1).fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c11", "按 C11 源码实现：通道突破开仓，盈利阈值+通道回撤止盈，并叠加固定比例止损。")

def build_c12_expr() -> Expr:
    n = pms("c12_n", 5, 500, 5).limit(20)
    sloss = pms("c12_sloss", 0.2, 20.0, 0.2).limit(20)
    m = pms("c12_m", 1, 10, 1).limit(20)
    nm = n * m
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col('high', 'low', 'close').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("high", "low", "close").ta.atr(n).expanding().alias("atr_n"),
            col("close").mean().rolling(n).alias("mac"),
            col("high").max().rolling(n).alias("hh1"),
            col("low").min().rolling(n).alias("ll1"),
        )
        .with_cols(
            col("high").max().rolling(nm).alias("hh2"),
            col("low").min().rolling(nm).alias("ll2"),
        )
        .with_cols(
            (col("mac") + m * col("atr_n")).alias("uband"),
            (col("mac") - m * col("atr_n")).alias("dband"),
            col('hh1', 'll1', 'hh2', 'll2').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col('uband', 'dband').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("high_1") >= col("hh1_1")).fill_null(False).alias("open_long_raw"),
            (col("low_1") <= col("ll1_1")).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "high_1", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("best_high"),
            col("open_short_pre", "exit_short_pre", "low_1", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("best_low"),
        )
        .with_cols(
            (
                (col("pos_pre") == -1.0)
                & (col("close_1") >= (col("entry_short") * (1.0 + sloss * 0.01)))
            )
            .fill_null(False)
            .alias("exit_short_sl"),
            (
                (col("pos_pre") == 1.0)
                & (col("close_1") <= (col("entry_long") * (1.0 - sloss * 0.01)))
            )
            .fill_null(False)
            .alias("exit_long_sl"),
            (
                (col("pos_pre") == 1.0)
                & (col("best_high") >= (col("entry_long") * (1.0 + m * sloss * 0.01)))
                & ((col("close_1") <= col("uband_1")) | (col("high_1") >= col("hh2_1")))
            )
            .fill_null(False)
            .alias("exit_long_tp"),
            (
                (col("pos_pre") == -1.0)
                & (col("best_low") <= (col("entry_short") * (1.0 - m * sloss * 0.01)))
                & ((col("close_1") >= col("dband_1")) | (col("low_1") <= col("ll2_1")))
            )
            .fill_null(False)
            .alias("exit_short_tp"),
        )
        .with_cols(
            col("exit_long_sl", "exit_long_tp").any(axis=1).fill_null(False).alias("exit_long_raw"),
            col("exit_short_sl", "exit_short_tp").any(axis=1).fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c12", "按 C12 源码实现：N 周期高低突破开仓，固定止损与 M 倍扩展阈值后的通道回归止盈。")

def build_c13_expr() -> Expr:
    sloss = pms("c13_sloss", 0.2, 20.0, 0.2).limit(20)
    m = pms("c13_m", 1.0, 10.0, 1.0).limit(20)
    n1 = pms("c13_n1", 10, 200, 10).limit(20)
    n2 = pms("c13_n2", 60, 1000, 60).limit(20)
    sar_n = pms("c13_sar_n", 2, 60, 2).limit(20)
    sar_frac = pms("c13_sar_frac", 0.001, 0.1, 0.001).limit(20)
    start_pro1 = pms("c13_start_pro1", 1.0, 30.0, 1.0).limit(20)
    stop_pro1 = pms("c13_stop_pro1", 1.0, 100.0, 1.0).limit(20)
    start_pro2 = pms("c13_start_pro2", 1.0, 50.0, 1.0).limit(20)
    stop_pro2 = pms("c13_stop_pro2", 1.0, 100.0, 1.0).limit(20)
    start_pro3 = pms("c13_start_pro3", 1.0, 80.0, 1.0).limit(20)
    stop_pro3 = pms("c13_stop_pro3", 1.0, 100.0, 1.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("bar_no"),
        )
        .with_cols(
            col("close").ta.ema(n1).expanding().alias("ma1"),
            col("close").ta.ema(n2).expanding().alias("ma2"),
            col("high", "low", "close").ta.sar(sar_n, sar_frac).expanding().alias("sar_pos"),
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col('sar_pos', 'ma1', 'ma2').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("close") > col("ma1", "ma2").max(axis=1)).fill_null(False).alias("b"),
            (col("close") < col("ma1", "ma2").min(axis=1)).fill_null(False).alias("s"),
            ((col("sar_pos") > 0.0) & (col("sar_pos_1") <= 0.0)).fill_null(False).alias("bsar"),
            ((col("sar_pos") < 0.0) & (col("sar_pos_1") >= 0.0)).fill_null(False).alias("ssar"),
        )
        .with_cols(
            col("b").shift(1).expanding().fill_null(False).add_suffix("1"),
            col("s").shift(1).expanding().fill_null(False).add_suffix("1"),
            col("bsar").shift(1).expanding().fill_null(False).add_suffix("1"),
            col("ssar").shift(1).expanding().fill_null(False).add_suffix("1"),
        )
        .with_cols(
            (
                (col("bar_no") > n2)
                & col("bsar_1")
                & col("b_1")
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("bar_no") > n2)
                & col("ssar_1")
                & col("s_1")
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "high_1", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("best_high"),
            col("open_short_pre", "exit_short_pre", "low_1", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("best_low"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("best_high") >= (col("entry_long") * (1.0 + m * sloss * 0.01)))
                & col("s_1")
            )
            .fill_null(False)
            .alias("exit_long_tp"),
            (
                (col("pos_pre") == -1.0)
                & (col("best_low") <= (col("entry_short") * (1.0 - m * sloss * 0.01)))
                & col("b_1")
            )
            .fill_null(False)
            .alias("exit_short_tp"),
            col("open_long_pre", "close_1", "low")
            .stra
            .exit_by_profit_drawdown(start_pro1, stop_pro1)
            .expanding()
            .fill_null(False)
            .alias("exit_long_tr1"),
            col("open_short_pre", "close_1", "high")
            .stra
            .exit_by_profit_drawdown(start_pro1, stop_pro1, True)
            .expanding()
            .fill_null(False)
            .alias("exit_short_tr1"),
            col("open_long_pre", "close_1", "low")
            .stra
            .exit_by_profit_drawdown(start_pro2, stop_pro2)
            .expanding()
            .fill_null(False)
            .alias("exit_long_tr2"),
            col("open_short_pre", "close_1", "high")
            .stra
            .exit_by_profit_drawdown(start_pro2, stop_pro2, True)
            .expanding()
            .fill_null(False)
            .alias("exit_short_tr2"),
            col("open_long_pre", "close_1", "low")
            .stra
            .exit_by_profit_drawdown(start_pro3, stop_pro3)
            .expanding()
            .fill_null(False)
            .alias("exit_long_tr3"),
            col("open_short_pre", "close_1", "high")
            .stra
            .exit_by_profit_drawdown(start_pro3, stop_pro3, True)
            .expanding()
            .fill_null(False)
            .alias("exit_short_tr3"),
            (
                (col("pos_pre") == 1.0)
                & (col("close_1") <= (col("entry_long") * (1.0 - sloss * 0.01)))
            )
            .fill_null(False)
            .alias("exit_long_sl"),
            (
                (col("pos_pre") == -1.0)
                & (col("close_1") >= (col("entry_short") * (1.0 + sloss * 0.01)))
            )
            .fill_null(False)
            .alias("exit_short_sl"),
        )
        .with_cols(
            col("exit_long_tp", "exit_long_tr1", "exit_long_tr2", "exit_long_tr3", "exit_long_sl").any(axis=1)
            .fill_null(False)
            .alias("exit_long_raw"),
            col("exit_short_tp", "exit_short_tr1", "exit_short_tr2", "exit_short_tr3", "exit_short_sl").any(axis=1)
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c13", "按 C13 源码实现：SAR 方向翻转+双均线趋势入场，固定止损、分段回撤止盈与趋势反转止盈并行。")

def build_c14_expr() -> Expr:
    n = pms("c14_n", 2, 400, 2).limit(20)
    m = pms("c14_m", 0.5, 3.0, 0.5).limit(20)
    hhn = pms("c14_hhn", 1, 100, 1).limit(20)
    trailing_stop_rate = pms("c14_trailing_stop_rate", 0.1, 10.0, 0.1).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close", "high", "low").shift(1).expanding().add_suffix("1"),
            col("high", "low").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            col("close").ta.wma(n).expanding().alias("wma"),
            col("close").mean().rolling(n).alias("mac"),
            (col("close") - col("open")).abs().alias("co_abs"),
        )
        .with_cols(
            col(col("co_abs") == 0.0, 0.0, (col("close") - col("open")) / col("co_abs")).if_else().alias("fh"),
            col(col("co_abs") == 0.0, 0.0, col("co_abs").pow(m)).if_else().alias("tt"),
        )
        .with_cols(
            col(col("tt") == 0.0, col("close"), col("close") + col("fh") * col("tt")).if_else().alias("qq"),
            col("high").shift(hhn).expanding().alias("high_hhn"),
            col("low").shift(hhn).expanding().alias("low_hhn"),
        )
        .with_cols(
            col("qq").mean().rolling(n).alias("zma"),
            col('wma', 'mac').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("zma").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("open", "high_1", "high_2", "high_hhn").max(axis=1).alias("entry_long_now"),
            col("open", "low_1", "low_2", "low_hhn").min(axis=1).alias("entry_short_now"),
            (
                (col("wma_1") > col("mac_1"))
                & (col("close_1") > col("zma_1"))
                & (col("high") >= col("high_1"))
                & (col("high") >= col("high_hhn"))
                & (col("high") >= col("high_2"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("wma_1") < col("mac_1"))
                & (col("close_1") < col("zma_1"))
                & (col("low") <= col("low_1"))
                & (col("low") <= col("low_hhn"))
                & (col("low") <= col("low_2"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            _bars_pos_from_open_sig_expr("open_long_sig", "open_short_sig", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "close_1", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("best_long_close"),
            col("open_short_pre", "exit_short_pre", "close_1", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("best_short_close"),
        )
        .with_cols(
            (col("best_long_close") - col("open") * trailing_stop_rate * 0.01).alias("long_trail_line"),
            (col("best_short_close") + col("open") * trailing_stop_rate * 0.01).alias("short_trail_line"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("low") < col("long_trail_line"))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("high") > col("short_trail_line"))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c14", "按 C14 源码实现：WMA/SMA/ZMA 共同过滤的突破入场，持仓后按入场后收盘极值与开盘价比例构造动态止损。")

def build_c15_expr() -> Expr:
    sloss = pms("c15_sloss", 0.5, 20.0, 0.5).limit(20)
    nc = pms("c15_nc", 5, 200, 5).limit(20)
    n = pms("c15_n", 20, 600, 20).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("bar_no"),
            col("close").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("close").shift(nc).expanding().alias("close_nc"),
        )
        .with_cols(
            (col("volume") * (col("close") - col("close_nc"))).alias("vjq_src"),
            col("close").max().rolling(n).alias("hh"),
            col("close").min().rolling(n).alias("ll"),
        )
        .with_cols(
            col("vjq_src").ta.ema(n).expanding().alias("vjq"),
        )
        .with_cols(
            (col("vjq") > 0.0).fill_null(False).alias("b"),
            (col("vjq") < 0.0).fill_null(False).alias("s"),
            (col("bar_no") > n).fill_null(False).alias("enough_bars"),
        )
        .with_cols(
            (col("enough_bars") & col("b") & (col("high") >= col("hh"))).fill_null(False).alias("open_long_raw"),
            (col("enough_bars") & col("s") & (col("low") <= col("ll"))).fill_null(False).alias("open_short_raw"),
            col("s").fill_null(False).alias("exit_long_base"),
            col("b").fill_null(False).alias("exit_short_base"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("close_1") <= (col("entry_long") * (1.0 - sloss * 0.01)))
            )
            .fill_null(False)
            .alias("exit_long_sl"),
            (
                (col("pos_pre") == -1.0)
                & (col("close_1") >= (col("entry_short") * (1.0 + sloss * 0.01)))
            )
            .fill_null(False)
            .alias("exit_short_sl"),
        )
        .with_cols(
            col("exit_long_base", "exit_long_sl").any(axis=1).fill_null(False).alias("exit_long_raw"),
            col("exit_short_base", "exit_short_sl").any(axis=1).fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c15", "按 C15 源码实现：成交量加权趋势+区间突破开仓，趋势反向或固定比例止损平仓。")

def build_c16_expr() -> Expr:
    threshold = pms("c16_threshold", 1.0, 100.0, 14.0).limit(20)
    new_ma_length = pms("c16_new_ma_length", 5, 240, 30).limit(20)
    begin_hhmm = pms("c16_begin_hhmm", 0, 2359, 945).limit(20)
    end_hhmm = pms("c16_end_hhmm", 0, 2359, 1435).limit(20)
    trailing_stop_rate = pms("c16_trailing_stop_rate", 0.2, 10.0, 1.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            (col("datetime").dt.hour() * 100 + col("datetime").dt.minute()).alias("hhmm"),
            col("datetime").dt.date().alias("day"),
            ((col("high") + col("low") + col("close")) / 3.0).alias("tp"),
        )
        .with_cols(
            col("tp").mean().rolling(new_ma_length).alias("new_ma"),
            col('day', 'high', 'low', 'close').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("day") != col("day_1")).fill_null(True).alias("is_new_day"),
        )
        .with_cols(
            col("is_new_day", "tp", col.null).if_else().alias("middle_seed"),
            col("new_ma").shift(1).expanding().add_suffix("1"),
            col("new_ma").shift(3).expanding().add_suffix("3"),
        )
        .with_cols(
            col("middle_seed").ffill().expanding().alias("middle_line"),
        )
        .with_cols(
            col("middle_line").shift(1).expanding().alias("middle_1"),
        )
        .with_cols(
            (
                (col("hhmm") >= begin_hhmm)
                & (col("hhmm") <= end_hhmm)
                & (col("low_1") >= col("middle_1"))
                & ((col("new_ma_1") - col("new_ma_3")) > threshold)
            )
            .fill_null(False)
            .alias("open_long_cand"),
            (
                (col("hhmm") >= begin_hhmm)
                & (col("hhmm") <= end_hhmm)
                & (col("high_1") <= col("middle_1"))
                & ((col("new_ma_3") - col("new_ma_1")) > threshold)
            )
            .fill_null(False)
            .alias("open_short_cand"),
        )
        .with_cols(
            col("open_long_cand", "open_short_cand").any(axis=1).cast(pl.UInt32).alias("entry_cand_u"),
        )
        .with_cols(
            col("entry_cand_u").sum().expanding().over(col("day")).alias("entry_count_day"),
        )
        .with_cols(
            (col("open_long_cand") & (col("entry_count_day") == 1)).fill_null(False).alias("open_long_raw"),
            (col("open_short_cand") & (col("entry_count_day") == 1)).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "close_1", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("best_long_close"),
            col("open_short_pre", "exit_short_pre", "close_1", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("best_short_close"),
        )
        .with_cols(
            (col("best_long_close") - col("open") * trailing_stop_rate * 0.01).alias("long_trail_line"),
            (col("best_short_close") + col("open") * trailing_stop_rate * 0.01).alias("short_trail_line"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("low") < col("long_trail_line"))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("high") > col("short_trail_line"))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c16", "按 C16 源码实现：日内首笔交易约束下，固定中线+均线斜率过滤入场，持仓后按入场后收盘极值构造动态止损。")

def build_c17_expr() -> Expr:
    sloss = pms("c17_sloss", 0.2, 20.0, 0.2).limit(20)
    n1 = pms("c17_n1", 5, 300, 5).limit(20)
    n2 = pms("c17_n2", 10, 500, 10).limit(20)
    n3 = pms("c17_n3", 15, 700, 15).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("bar_no"),
        )
        .with_cols(
            col("close").mean().rolling(n1).alias("ma1"),
            col("close").mean().rolling(n2).alias("ma2"),
            col("close").mean().rolling(n3).alias("ma3"),
            col("close").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col('ma1', 'ma2', 'ma3', 'close').shift(1).expanding().alias('ma1_1', 'ma2_1', 'ma3_1', 'close_1b'),
            col("close").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            ((col("close") > col("ma2")) & (col("close_1") <= col("ma2").shift(1).expanding())).fill_null(False).alias("cross_up_ma2"),
            ((col("close") < col("ma2")) & (col("close_1") >= col("ma2").shift(1).expanding())).fill_null(False).alias("cross_down_ma2"),
        )
        .with_cols(
            (
                (col("bar_no") > n3)
                & (col("ma1_1") > col("ma2_1"))
                & (col("ma2_1") > col("ma3_1"))
                & (col("close_1b") > col("ma1_1"))
            )
            .fill_null(False)
            .alias("buyk"),
            (
                (col("bar_no") > n3)
                & (col("ma1_1") < col("ma2_1"))
                & (col("ma2_1") < col("ma3_1"))
                & (col("close_1b") < col("ma1_1"))
            )
            .fill_null(False)
            .alias("sellk"),
        )
        .with_cols(
            col("buyk").shift(1).expanding().fill_null(False).alias("open_long_raw"),
            col("sellk").shift(1).expanding().fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & col("cross_down_ma2")
                & (col("ma1") > col("close"))
                & (col("close") >= (col("entry_long") * (1.0 + 2.0 * sloss * 0.01)))
            )
            .fill_null(False)
            .alias("sellp_cond"),
            (
                (col("pos_pre") == -1.0)
                & col("cross_up_ma2")
                & (col("close") > col("ma1"))
                & (col("close") <= (col("entry_short") * (1.0 - 2.0 * sloss * 0.01)))
            )
            .fill_null(False)
            .alias("buyp_cond"),
        )
        .with_cols(
            col("sellp_cond", "buyp_cond").shift(1).expanding().fill_null(False).alias("exit_long_base", "exit_short_base"),
            (
                (col("pos_pre") == 1.0)
                & (col("close_1") <= (col("entry_long") * (1.0 - sloss * 0.01)))
            )
            .fill_null(False)
            .alias("exit_long_sl"),
            (
                (col("pos_pre") == -1.0)
                & (col("close_1") >= (col("entry_short") * (1.0 + sloss * 0.01)))
            )
            .fill_null(False)
            .alias("exit_short_sl"),
        )
        .with_cols(
            col("exit_long_base", "exit_long_sl").any(axis=1).fill_null(False).alias("exit_long_raw"),
            col("exit_short_base", "exit_short_sl").any(axis=1).fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c17", "按 C17 源码实现：三均线排列+趋势过滤开仓，MA2 反向穿越且达到盈亏阈值止盈，叠加固定止损。")

def build_c18_expr() -> Expr:
    direction = pms("c18_direction", -1.0, 1.0, -1.0).limit(20)
    length = pms("c18_length", 2, 200, 2).limit(20)
    offset = pms("c18_offset", 0.1, 4.0, 0.1).limit(20)
    trail_start_pct = pms("c18_trail_start_pct", 0.05, 10.0, 0.05).limit(20)
    param = pms("c18_param", 0.2, 3.0, 0.2).limit(20)
    n = pms("c18_n", 0, 10, 0).limit(20)
    pcnt = pms("c18_pcnt", 0.1, 10.0, 0.1).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("bar_no"),
            col("high", "low").shift(1).expanding().add_suffix("1"),
            col("high", "low").shift(2).expanding().add_suffix("2"),
            col("volume").shift(1).expanding().alias("vol_1"),
        )
        .with_cols(
            col("close").mean().rolling(length).alias("midline"),
            col("close").std().rolling(length).alias("band"),
        )
        .with_cols(
            (col("midline") + offset * col("band")).alias("upline"),
            (col("midline") - offset * col("band")).alias("downline"),
            ((2.0 * offset * col("band")) / col("midline")).alias("range_v"),
        )
        .with_cols(
            col("range_v").mean().rolling(length).alias("avg_range"),
            col("volume").mean().rolling(length).alias("avg_vol"),
            col("upline", "downline").shift(1).expanding().add_suffix("1"),
            col("upline", "downline").shift(2).expanding().add_suffix("2"),
            col("range_v").shift(1).expanding().alias("range_1"),
        )
        .with_cols(
            col('avg_range', 'avg_vol').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("open", "high_1").max(axis=1).alias("entry_long_now"),
            col("open", "low_1").min(axis=1).alias("entry_short_now"),
            (
                (col("bar_no") > length)
                & (col("range_1") > param * col("avg_range_1"))
                & (col("vol_1") > param * col("avg_vol_1"))
                & (col("high_1") > col("upline_1"))
                & (col("high_2") > col("upline_2"))
                & (col("high") >= col("high_1"))
                & (direction >= 0)
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("bar_no") > length)
                & (col("range_1") > param * col("avg_range_1"))
                & (col("vol_1") > param * col("avg_vol_1"))
                & (col("low_1") < col("downline_1"))
                & (col("low_2") < col("downline_2"))
                & (col("low") <= col("low_1"))
                & (direction <= 0)
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            _bars_pos_from_open_sig_expr("open_long_sig", "open_short_sig", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "high_1", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("best_high"),
            col("open_short_pre", "exit_short_pre", "low_1", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("best_low"),
        )
        .with_cols(
            ((col("entry_short") - col("best_low")) / col("entry_short")).alias("pp_raw"),
            ((col("best_high") - col("entry_long")) / col("entry_long")).alias("tt_raw"),
        )
        .with_cols(
            col("pp_raw", 0.1).min(axis=1).alias("pp"),
            col("tt_raw", 0.1).min(axis=1).alias("tt"),
        )
        .with_cols(
            (col("pp") * 10.0).alias("profit_pct_short"),
            (col("tt") * 10.0).alias("profit_pct_long"),
            (col("entry_short") - (col("entry_short") - col("best_low")) * (col("pp") * 10.0)).alias("short_tp_line"),
            (col("entry_long") + (col("best_high") - col("entry_long")) * (col("tt") * 10.0)).alias("long_tp_line"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("low") <= (col("entry_long") * (1.0 - pcnt * 0.01)))
            )
            .fill_null(False)
            .alias("exit_long_sl"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("high") >= (col("entry_short") * (1.0 + pcnt * 0.01)))
            )
            .fill_null(False)
            .alias("exit_short_sl"),
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("best_high") >= (col("entry_long") * (1.0 + trail_start_pct * 0.01)))
                & (col("low") <= col("long_tp_line"))
            )
            .fill_null(False)
            .alias("exit_long_tp"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("best_low") <= (col("entry_short") * (1.0 - trail_start_pct * 0.01)))
                & (col("high") >= col("short_tp_line"))
            )
            .fill_null(False)
            .alias("exit_short_tp"),
        )
        .with_cols(
            col("exit_long_sl", "exit_long_tp").any(axis=1).fill_null(False).alias("exit_long_raw"),
            col("exit_short_sl", "exit_short_tp").any(axis=1).fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c18", "按 C18 源码实现：变种布林+量能放大突破入场，持仓后固定止损并叠加入场后极值驱动的浮动止盈。")

def build_c19_expr() -> Expr:
    n1 = pms("c19_n1", 50, 1000, 50).limit(20)
    n2 = pms("c19_n2", 20, 500, 20).limit(20)
    sloss = pms("c19_sloss", 0.2, 20.0, 0.2).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("bar_no"),
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("close").shift(n1).expanding().alias("close_n1"),
            col("close").shift(n2).expanding().alias("close_n2"),
        )
        .with_cols(
            (col("close") - col("close_n1")).alias("direction1"),
            (col("close") - col("close_n2")).alias("direction2"),
            (col("close") - col("close_1")).abs().alias("abs_diff"),
        )
        .with_cols(
            col("abs_diff").sum().rolling(n1).alias("volatility1"),
            col("abs_diff").sum().rolling(n2).alias("volatility2"),
            col("close").ta.ema(n1).expanding().alias("eman"),
            (
                (
                    col("close").ta.ema(100).expanding()
                    + col("close").ta.ema(120).expanding()
                    + col("close").ta.ema(140).expanding()
                    + col("close").ta.ema(160).expanding()
                )
                * 0.25
            ).alias("rt"),
        )
        .with_cols(
            col(col("volatility1") != 0.0, (col("direction1") / col("volatility1")).abs(), 0.0).if_else().alias("er1"),
            col(col("volatility2") != 0.0, (col("direction2") / col("volatility2")).abs(), 0.0).if_else().alias("er2"),
            col("rt").mean().rolling(n2).alias("sman"),
        )
        .with_cols(
            col('eman', 'sman').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("eman") > col("sman")) & (col("eman_1") <= col("sman_1"))).fill_null(False).alias("buypk"),
            ((col("eman") < col("sman")) & (col("eman_1") >= col("sman_1"))).fill_null(False).alias("sellpk"),
            ((col("eman") > col("sman")) & (col("er1") > col("er2")) & (col("direction1") > 0.0)).fill_null(False).alias("buyk"),
            ((col("eman") < col("sman")) & (col("er1") > col("er2")) & (col("direction1") < 0.0)).fill_null(False).alias("sellk"),
        )
        .with_cols(
            col("buypk").shift(1).expanding().fill_null(False).add_suffix("1"),
            col("sellpk").shift(1).expanding().fill_null(False).add_suffix("1"),
            col("buyk").shift(1).expanding().fill_null(False).add_suffix("1"),
            col("sellk").shift(1).expanding().fill_null(False).add_suffix("1"),
        )
        .with_cols(
            (
                (col("bar_no") > n1)
                & col("buypk_1", "buyk_1").any(axis=1)
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("bar_no") > n1)
                & col("sellpk_1", "sellk_1").any(axis=1)
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "high_1", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("best_high"),
            col("open_short_pre", "exit_short_pre", "low_1", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("best_low"),
        )
        .with_cols(
            (col("close") >= (col("entry_short") * (1.0 + sloss * 0.01))).fill_null(False).alias("buys"),
            (col("close") <= (col("entry_long") * (1.0 - sloss * 0.01))).fill_null(False).alias("sells"),
            (
                (col("close") > (col("entry_long") * (1.0 + sloss * 0.01)))
                & (col("close") < (col("best_high") * (1.0 - sloss * 0.01)))
            )
            .fill_null(False)
            .alias("buyy"),
            (
                (col("close") < (col("entry_short") * (1.0 - sloss * 0.01)))
                & (col("close") > (col("best_low") * (1.0 + sloss * 0.01)))
            )
            .fill_null(False)
            .alias("selly"),
        )
        .with_cols(
            (col("sells", "selly").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_long_src"),
            (col("buys", "buyy").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_short_src"),
        )
        .with_cols(
            col("exit_long_src").shift(1).expanding().fill_null(False).alias("exit_long_raw"),
            col("exit_short_src").shift(1).expanding().fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c19", "按 C19 源码实现：EMA/平滑均线交叉与效率比共振入场，固定比例止损与利润回撤止盈并行离场。")

def build_c20_expr() -> Expr:
    x = pms("c20_x", 1.0, 20.0, 1.0).limit(20)
    trailing_stop_rate = pms("c20_trailing_stop_rate", 0.2, 10.0, 0.2).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("datetime").dt.date().alias("day"),
            col("close", "high", "low").shift(1).expanding().add_suffix("1"),
            col("close").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            col("day").shift(1).expanding().add_suffix("1"),
            col(
                col("open").first_value().alias("open_d0"),
                col("high").max().alias("day_high_to_now"),
                col("low").min().alias("day_low_to_now"),
            ).expanding().over(col("day")),
        )
        .with_cols(
            (col("day") != col("day_1")).fill_null(True).alias("is_new_day"),
            col('day_high_to_now', 'day_low_to_now').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("is_new_day", "close_1", col.null).if_else().alias("prev_close_seed"),
            col("is_new_day", "day_high_to_now_1", col.null).if_else().alias("prev_high_seed"),
            col("is_new_day", "day_low_to_now_1", col.null).if_else().alias("prev_low_seed"),
        )
        .with_cols(
            col("prev_close_seed").ffill().expanding().alias("close_d1"),
            col("prev_high_seed").ffill().expanding().alias("high_d1"),
            col("prev_low_seed").ffill().expanding().alias("low_d1"),
        )
        .with_cols(
            col(col("close_d1") - col("low_d1"), col("high_d1") - col("close_d1")).max(axis=1).alias("band0"),
        )
        .with_cols(
            col(col("open_d0") * 0.008, col("band0")).max(axis=1).alias("band"),
        )
        .with_cols(
            (col("open_d0") + 0.1 * x * col("band")).alias("tmp1"),
            (col("open_d0") - 0.1 * x * col("band")).alias("tmp2"),
        )
        .with_cols(
            ((col("close_1") >= col("tmp1")) & (col("close_2") < col("tmp1"))).fill_null(False).alias("open_long_raw"),
            ((col("close_1") <= col("tmp2")) & (col("close_2") > col("tmp2"))).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "close_1", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("best_long_close"),
            col("open_short_pre", "exit_short_pre", "close_1", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("best_short_close"),
        )
        .with_cols(
            (col("best_long_close") - col("open") * trailing_stop_rate * 0.01).alias("long_trail_line"),
            (col("best_short_close") + col("open") * trailing_stop_rate * 0.01).alias("short_trail_line"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("close_1") <= col("tmp2"))).fill_null(False).alias("exit_long_base"),
            ((col("pos_pre") == -1.0) & (col("close_1") >= col("tmp1"))).fill_null(False).alias("exit_short_base"),
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("low") < col("long_trail_line"))
            )
            .fill_null(False)
            .alias("exit_long_ts"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("high") > col("short_trail_line"))
            )
            .fill_null(False)
            .alias("exit_short_ts"),
        )
        .with_cols(
            col("exit_long_base", "exit_long_ts").any(axis=1).fill_null(False).alias("exit_long_raw"),
            col("exit_short_base", "exit_short_ts").any(axis=1).fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c20", "按 C20 源码实现：以前一交易日区间构造 TMP1/TMP2 交叉入场，叠加对侧线离场与入场后收盘极值跟踪止损。")

def build_c21_expr() -> Expr:
    n = pms("c21_n", 20, 600, 20).limit(20)
    m = pms("c21_m", 1, 20, 1).limit(20)
    x = pms("c21_x", 0.2, 20.0, 0.2).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("bar_no"),
            col("close").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("high", "low", "close").ta.cci(n).expanding().alias("cci_v"),
            col("close").mean().rolling(n * m).alias("macc"),
        )
        .with_cols(
            col("cci_v").shift(1).expanding().alias("cci_1"),
        )
        .with_cols(
            ((col("cci_v") > 100.0) & (col("cci_1") <= 100.0) & (col("close") > col("macc"))).fill_null(False).alias("buyk"),
            ((col("cci_v") < -100.0) & (col("cci_1") >= -100.0) & (col("close") < col("macc"))).fill_null(False).alias("sellk"),
        )
        .with_cols(
            col("buyk").shift(1).expanding().fill_null(False).alias("open_long_raw"),
            col("sellk").shift(1).expanding().fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
        )
        .with_cols(
            (
                (col("bar_no") > (n * m))
                & (col("close") <= (col("entry_long") * (1.0 - x * 0.01)))
            )
            .fill_null(False)
            .alias("sells"),
            (
                (col("bar_no") > (n * m))
                & (col("close") >= (col("entry_short") * (1.0 + x * 0.01)))
            )
            .fill_null(False)
            .alias("buys"),
            (
                (col("bar_no") > (n * m))
                & (col("cci_v") < 0.0)
                & (col("close") > (col("entry_long") * (1.0 + x * 0.01)))
            )
            .fill_null(False)
            .alias("selly"),
            (
                (col("bar_no") > (n * m))
                & (col("cci_v") > 0.0)
                & (col("close") < (col("entry_short") * (1.0 - x * 0.01)))
            )
            .fill_null(False)
            .alias("buyy"),
        )
        .with_cols(
            (col("sells", "selly").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_long_src"),
            (col("buys", "buyy").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_short_src"),
        )
        .with_cols(
            col("exit_long_src").shift(1).expanding().fill_null(False).alias("exit_long_raw"),
            col("exit_short_src").shift(1).expanding().fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c21", "按 C21 源码实现：CCI 上下阈值交叉配合长均线过滤入场，固定止损与“达到盈利后 CCI 回落”联合离场。")

def build_c22_expr() -> Expr:
    length1 = pms("c22_length1", 50, 800, 50).limit(20)
    length2 = pms("c22_length2", 10, 300, 10).limit(20)
    n = pms("c22_n", 0.5, 8.0, 0.5).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            ((col("high") + col("low") + col("close")) / 3.0).alias("tp"),
            col("close").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("tp").mean().rolling(length1).alias("midline"),
            col("high", "low", "close").ta.atr(length2).expanding().alias("atr"),
        )
        .with_cols(
            (col("midline") + n * col("atr")).alias("upband"),
            (col("midline") - n * col("atr")).alias("downband"),
        )
        .with_cols(
            col('upband', 'downband', 'midline').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("close", "upband", "downband").stra.bollin_sig(),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            col("open_long_sig").alias("open_long_pre"),
            col("open_short_sig").alias("open_short_pre"),
        )
        .with_cols(
            _bars_pos_from_open_sig_expr("open_long_sig", "open_short_sig", "bars_pos"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("bars_pos") > 0) & (col("close_1") <= col("midline_1")))
            .fill_null(False)
            .alias("exit_long_raw"),
            ((col("pos_pre") == -1.0) & (col("bars_pos") > 0) & (col("close_1") >= col("midline_1")))
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c22", "按 C22 源码实现：中轨+ATR 通道突破入场，持仓后仅以回到中轨触发离场。")

def build_c23_expr() -> Expr:
    n = pms("c23_n", 50, 1000, 50).limit(20)
    sloss = pms("c23_sloss", 0.2, 20.0, 0.2).limit(20)
    length = pms("c23_length", 3, 80, 3).limit(20)
    slow_length = pms("c23_slow_length", 2, 30, 2).limit(20)
    smooth_length = pms("c23_smooth_length", 2, 30, 2).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("bar_no"),
        )
        .with_cols(
            col("close").ta.ema(n).expanding().alias("mac"),
            col("high").max().rolling(length).alias("highest_v"),
            col("low").min().rolling(length).alias("lowest_v"),
        )
        .with_cols(
            (col("highest_v") - col("lowest_v")).alias("hl_v"),
            (col("close") - col("lowest_v")).alias("cl_v"),
        )
        .with_cols(
            col("hl_v", "cl_v").sum().rolling(slow_length).alias("sum_hl", "sum_cl"),
        )
        .with_cols(
            col(col("sum_hl") != 0.0, col("sum_cl") / col("sum_hl") * 100.0, 0.0).if_else().alias("k_value"),
        )
        .with_cols(
            col("k_value").mean().rolling(smooth_length).alias("d_value"),
        )
        .with_cols(
            ((col("close") > col("mac")) & (col("k_value") < col("d_value"))).fill_null(False).alias("buyk"),
            ((col("close") < col("mac")) & (col("k_value") > col("d_value"))).fill_null(False).alias("sellk"),
        )
        .with_cols(
            (
                (col("bar_no") > n)
                & col("buyk").shift(1).expanding().fill_null(False)
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("bar_no") > n)
                & col("sellk").shift(1).expanding().fill_null(False)
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
        )
        .with_cols(
            (
                (col("close") <= (col("entry_long") * (1.0 - sloss * 0.01)))
            )
            .fill_null(False)
            .alias("sells"),
            (
                (col("close") >= (col("entry_short") * (1.0 + sloss * 0.01)))
            )
            .fill_null(False)
            .alias("buys"),
            (
                (col("close") >= (col("entry_long") * (1.0 + sloss * 0.01)))
                & (col("close") < col("mac"))
            )
            .fill_null(False)
            .alias("selly"),
            (
                (col("close") <= (col("entry_short") * (1.0 - sloss * 0.01)))
                & (col("close") > col("mac"))
            )
            .fill_null(False)
            .alias("buyy"),
        )
        .with_cols(
            (col("sells", "selly").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_long_src"),
            (col("buys", "buyy").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_short_src"),
        )
        .with_cols(
            col("exit_long_src").shift(1).expanding().fill_null(False).alias("exit_long_raw"),
            col("exit_short_src").shift(1).expanding().fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c23", "按 C23 源码实现：EMA 趋势+K/D 关系入场，固定止损与达到盈利后的趋势反向离场并行。")

def build_c24_expr() -> Expr:
    sloss = pms("c24_sloss", 0.2, 20.0, 0.2).limit(20)
    n1 = pms("c24_n1", 5, 200, 5).limit(20)
    n2 = pms("c24_n2", 50, 1000, 50).limit(20)
    length = pms("c24_length", 3, 80, 3).limit(20)
    oversold = pms("c24_oversold", 5.0, 50.0, 5.0).limit(20)
    overbought = pms("c24_overbought", 50.0, 95.0, 50.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("bar_no"),
        )
        .with_cols(
            col("close").ta.rsi(length).expanding().alias("rsi_v"),
            col("close").ta.ema(n1).expanding().alias("ma1"),
            col("close").ta.ema(n2).expanding().alias("ma2"),
        )
        .with_cols(
            col("rsi_v").shift(1).expanding().alias("rsi_1"),
        )
        .with_cols(
            (
                (col("bar_no") > n2)
                & (col("ma1") > col("ma2"))
                & (col("close") > col("ma1", "ma2").max(axis=1))
                & (col("rsi_v") > overbought)
                & (col("rsi_1") <= overbought)
            )
            .fill_null(False)
            .alias("buyk"),
            (
                (col("bar_no") > n2)
                & (col("ma1") < col("ma2"))
                & (col("close") < col("ma1", "ma2").min(axis=1))
                & (col("rsi_v") < oversold)
                & (col("rsi_1") >= oversold)
            )
            .fill_null(False)
            .alias("sellk"),
        )
        .with_cols(
            col("buyk").shift(1).expanding().fill_null(False).alias("open_long_raw"),
            col("sellk").shift(1).expanding().fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
        )
        .with_cols(
            (col("close") >= (col("entry_short") * (1.0 + sloss * 0.01))).fill_null(False).alias("buys"),
            (col("close") <= (col("entry_long") * (1.0 - sloss * 0.01))).fill_null(False).alias("sells"),
            (
                (col("close") <= (col("entry_short") * (1.0 - sloss * 0.01)))
                & (col("ma1") > col("ma2"))
            )
            .fill_null(False)
            .alias("buyy"),
            (
                (col("close") >= (col("entry_long") * (1.0 + sloss * 0.01)))
                & (col("ma1") < col("ma2"))
            )
            .fill_null(False)
            .alias("selly"),
        )
        .with_cols(
            (col("sells", "selly").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_long_src"),
            (col("buys", "buyy").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_short_src"),
        )
        .with_cols(
            col("exit_long_src").shift(1).expanding().fill_null(False).alias("exit_long_raw"),
            col("exit_short_src").shift(1).expanding().fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c24", "按 C24 源码实现：双 EMA 趋势过滤下 RSI 阈值交叉入场，固定止损与趋势反向条件联合离场。")

def build_c25_expr() -> Expr:
    n_open = pms("c25_n_open", 3, 80, 3).limit(20)
    n_exit = pms("c25_n_exit", 5, 120, 5).limit(20)
    frac = pms("c25_frac", 0.001, 0.05, 0.001).limit(20)
    e = _full_kline_expr().select("high", "low", "close").stra.c25(
        n_open=n_open,
        n_exit=n_exit,
        frac=frac,
    )
    return _with_meta(e, "c25", "C25 原生实现：基于原策略参数 n_open/n_exit/frac 计算信号。")

def build_c26_expr() -> Expr:
    n1 = pms("c26_n1", 20, 400, 20).limit(20)
    n2 = pms("c26_n2", 80, 1000, 80).limit(20)
    sloss = pms("c26_sloss", 0.2, 20.0, 0.2).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("bar_no"),
        )
        .with_cols(
            col("high").max().rolling(n1).alias("hh_n1"),
            col("low").min().rolling(n1).alias("ll_n1"),
            col("high").max().rolling(n2).alias("hh_n2"),
            col("low").min().rolling(n2).alias("ll_n2"),
        )
        .with_cols(
            ((col("hh_n1") + col("ll_n1")) * 0.5).alias("line1"),
            ((col("hh_n2") + col("ll_n2")) * 0.5).alias("line2"),
        )
        .with_cols(
            col('line1', 'line2').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("bar_no") > n2)
                & (col("line1_1") < col("line2_1"))
                & (col("line1") >= col("line2"))
            )
            .fill_null(False)
            .alias("buypk"),
            (
                (col("bar_no") > n2)
                & (col("line1_1") > col("line2_1"))
                & (col("line1") <= col("line2"))
            )
            .fill_null(False)
            .alias("sellpk"),
            ((col("bar_no") > n2) & (col("high") >= col("hh_n1"))).fill_null(False).alias("buyk"),
            ((col("bar_no") > n2) & (col("low") <= col("ll_n1"))).fill_null(False).alias("sellk"),
        )
        .with_cols(
            col(
                col("buypk").shift(1).expanding().fill_null(False),
                col("buyk").shift(1).expanding().fill_null(False),
            ).any(axis=1).fill_null(False).alias("open_long_raw"),
            col(
                col("sellpk").shift(1).expanding().fill_null(False),
                col("sellk").shift(1).expanding().fill_null(False),
            ).any(axis=1).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            ((col("close") < (col("entry_long") * (1.0 - sloss * 0.01))) & (col("bars_pos") > 1)).fill_null(False).alias("sells"),
            ((col("close") > (col("entry_short") * (1.0 + sloss * 0.01))) & (col("bars_pos") > 1)).fill_null(False).alias("buys"),
        )
        .with_cols(
            col("sells").shift(1).expanding().fill_null(False).alias("exit_long_raw"),
            col("buys").shift(1).expanding().fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c26", "按 C26 源码实现：长短周期中线交叉与 N1 极值突破联合入场，固定比例止损离场。")

def build_c27_expr() -> Expr:
    dmi_n = pms("c27_dmi_n", 5, 60, 5).limit(20)
    nc = pms("c27_nc", 20, 400, 20).limit(20)
    nhl = pms("c27_nhl", 10, 200, 10).limit(20)
    sloss = pms("c27_sloss", 0.2, 20.0, 0.2).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("bar_no"),
            col('high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("high", "low", "close").ta.adx(dmi_n).expanding().alias("adx"),
            col("high", "low", "close").ta.adxr(dmi_n).expanding().alias("adxr"),
            col("high", "low", "close").ta.plus_di(dmi_n).expanding().alias("pdi"),
            col("high", "low", "close").ta.minus_di(dmi_n).expanding().alias("mdi"),
            col("close").mean().rolling(nc).alias("mac"),
            col("high").max().rolling(nhl).alias("hh_nhl"),
            col("low").min().rolling(nhl).alias("ll_nhl"),
        )
        .with_cols(
            (
                (col("high") >= col("hh_nhl"))
                & (col("adx") > col("adxr"))
                & (col("pdi") > col("mdi"))
                & (col("pdi") > 20.0)
            )
            .fill_null(False)
            .alias("buypk"),
            (
                (col("low") <= col("ll_nhl"))
                & (col("adx") > col("adxr"))
                & (col("pdi") < col("mdi"))
                & (col("mdi") > 20.0)
            )
            .fill_null(False)
            .alias("sellpk"),
        )
        .with_cols(
            ((col("bar_no") > nc) & col("buypk").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_long_raw"),
            ((col("bar_no") > nc) & col("sellpk").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(_four_signals_from_pos_expr("pos_pre"))
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "high_1", "entry_long").stra.t02_track_best_since_entry(False).expanding().alias("best_high"),
            col("open_short_pre", "exit_short_pre", "low_1", "entry_short").stra.t02_track_best_since_entry(True).expanding().alias("best_low"),
        )
        .with_cols(
            (col("close") >= (col("entry_short") * (1.0 + sloss * 0.01))).fill_null(False).alias("buys"),
            (col("close") <= (col("entry_long") * (1.0 - sloss * 0.01))).fill_null(False).alias("sells"),
            (
                (col("close") <= (col("entry_short") * (1.0 - sloss * 0.01)))
                & (col("close") >= (col("best_low") * (1.0 + sloss * 0.01)))
                & (col("close") > col("mac"))
            )
            .fill_null(False)
            .alias("buyy"),
            (
                (col("close") >= (col("entry_long") * (1.0 + sloss * 0.01)))
                & (col("close") <= (col("best_high") * (1.0 - sloss * 0.01)))
                & (col("close") < col("mac"))
            )
            .fill_null(False)
            .alias("selly"),
        )
        .with_cols(
            (col("sells", "selly").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_long_src"),
            (col("buys", "buyy").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_short_src"),
        )
        .with_cols(
            col("exit_long_src").shift(1).expanding().fill_null(False).alias("exit_long_raw"),
            col("exit_short_src").shift(1).expanding().fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c27", "按 C27 源码实现：DMI 强趋势+NHL 突破入场，固定止损与盈利回撤+均线方向过滤离场。")

def build_c28_expr() -> Expr:
    mtm_n = pms("c28_mtm_n", 20, 300, 20).limit(20)
    n = pms("c28_n", 20, 400, 20).limit(20)
    m = pms("c28_m", 1.0, 10.0, 1.0).limit(20)
    sloss = pms("c28_sloss", 0.2, 20.0, 0.2).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("bar_no"),
        )
        .with_cols(
            col("close").shift(mtm_n).expanding().alias("close_mtm_n"),
        )
        .with_cols(
            (col("close") - col("close_mtm_n")).alias("mtm"),
            col("close").mean().rolling(n).alias("mac"),
        )
        .with_cols(
            col("mtm").mean().rolling(mtm_n).alias("mtmma"),
            col("mac").mean().rolling(n).alias("macma"),
        )
        .with_cols(
            ((col("bar_no") > n) & (col("mtm") > col("mtmma")) & (col("mac") > col("macma"))).fill_null(False).alias("buyk"),
            ((col("bar_no") > n) & (col("mtm") < col("mtmma")) & (col("mac") < col("macma"))).fill_null(False).alias("sellk"),
        )
        .with_cols(
            col("buyk", "sellk").shift(1).expanding().fill_null(False).alias("open_long_raw", "open_short_raw"),
        )
        .with_cols(_pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"))
        .with_cols(_four_signals_from_pos_expr("pos_pre"))
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            (col("close") >= (col("entry_short") * (1.0 + sloss * 0.01))).fill_null(False).alias("buys"),
            (col("close") <= (col("entry_long") * (1.0 - sloss * 0.01))).fill_null(False).alias("sells"),
            (
                (col("close") <= (col("entry_short") * (1.0 - sloss * 0.01 * m)))
                & (col("mac") > col("macma"))
            )
            .fill_null(False)
            .alias("buyy"),
            (
                (col("close") >= (col("entry_long") * (1.0 + sloss * 0.01 * m)))
                & (col("mac") < col("macma"))
            )
            .fill_null(False)
            .alias("selly"),
        )
        .with_cols(
            (col("sells", "selly").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_long_src"),
            (col("buys", "buyy").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_short_src"),
        )
        .with_cols(
            col("exit_long_src").shift(1).expanding().fill_null(False).alias("exit_long_raw"),
            col("exit_short_src").shift(1).expanding().fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(_pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"))
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c28", "按 C28 源码实现：MTM 与双层均线一致性入场，固定止损与达到盈利后的均线反转离场。")

def build_c29_expr() -> Expr:
    eff1 = pms("c29_eff_ratio_length1", 5, 80, 5).limit(20)
    eff2 = pms("c29_eff_ratio_length2", 10, 160, 10).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").ta.kama(eff1).expanding().alias("ama1"),
            col("close").ta.kama(eff2).expanding().alias("ama2"),
        )
        .with_cols(
            col('ama1', 'ama2').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("ama1_1") < col("ama2_1")) & (col("ama2") < col("ama1"))).fill_null(False).alias("buypk"),
            ((col("ama1_1") > col("ama2_1")) & (col("ama2") > col("ama1"))).fill_null(False).alias("sellpk"),
        )
        .with_cols(
            col("buypk").shift(1).expanding().fill_null(False).alias("open_long_raw"),
            col("sellpk").shift(1).expanding().fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "open_short_raw", "open_short_raw", "open_long_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c29", "按 C29 源码实现：双自适应均线（AMA）交叉反手。")

def build_c30_expr() -> Expr:
    n = pms("c30_n", 20, 600, 20).limit(20)
    m = pms("c30_m", 10, 200, 10).limit(20)
    sloss = pms("c30_sloss", 0.2, 20.0, 0.2).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("bar_no"),
        )
        .with_cols(
            col("close").shift(m).expanding().alias("close_m"),
        )
        .with_cols(
            col("high").max().rolling(n).alias("hh"),
            col("low").min().rolling(n).alias("ll"),
            (col("close") / col("close_m")).alias("rc"),
        )
        .with_cols(
            col("rc").mean().rolling(m).alias("arc"),
            (col("high") >= col("hh")).fill_null(False).alias("up"),
            (col("low") <= col("ll")).fill_null(False).alias("down"),
        )
        .with_cols(
            ((col("high") >= col("hh")) & (col("arc") > 1.0)).fill_null(False).alias("buypk"),
            ((col("low") <= col("ll")) & (col("arc") < 1.0)).fill_null(False).alias("sellpk"),
        )
        .with_cols(
            ((col("bar_no") > n) & col("buypk").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_long_raw"),
            ((col("bar_no") > n) & col("sellpk").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(_pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"))
        .with_cols(_four_signals_from_pos_expr("pos_pre"))
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            ((col("close") >= (col("entry_short") * (1.0 + sloss * 0.01)))).fill_null(False).alias("buys"),
            ((col("close") <= (col("entry_long") * (1.0 - sloss * 0.01)))).fill_null(False).alias("sells"),
            (col("down") & (col("close") > (col("entry_long") * (1.0 + sloss * 0.01)))).fill_null(False).alias("selly"),
            (col("up") & (col("close") < (col("entry_short") * (1.0 - sloss * 0.01)))).fill_null(False).alias("buyy"),
        )
        .with_cols(
            (col("sells", "selly").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_long_src"),
            (col("buys", "buyy").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_short_src"),
        )
        .with_cols(
            col("exit_long_src").shift(1).expanding().fill_null(False).alias("exit_long_raw"),
            col("exit_short_src").shift(1).expanding().fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(_pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"))
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c30", "按 C30 源码实现：N 周期极值突破并结合 ROC 均值过滤入场，固定止损与极值反转止盈离场。")

def build_c31_expr() -> Expr:
    trade_begin = pms("c31_trade_begin_hhmm", 0, 2359, 916).limit(20)
    last_trade = pms("c31_last_trade_hhmm", 0, 2359, 1512).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            (col("datetime").dt.hour() * 100 + col("datetime").dt.minute()).alias("hhmm"),
            col("high", "low", "close").ta.trange().expanding().alias("tr"),
            col("close").std().rolling(26).alias("a9"),
            col('high', 'low', 'close').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("tr").sum().rolling(16).alias("a10"),
            (col("high") - col("high_1")).alias("a11"),
            (col("low_1") - col("low")).alias("a12"),
            ((col("high") + col("low") + col("close")) / 3.0).alias("a5"),
        )
        .with_cols(
            col((col("a11") > 0.0) & (col("a11") > col("a12")), col("a11"), 0.0).if_else().alias("plus_dm"),
            col((col("a12") > 0.0) & (col("a12") > col("a11")), col("a12"), 0.0).if_else().alias("minus_dm"),
            col("a5", "open").shift(1).expanding().add_suffix("1"),
            col("open").shift(2).expanding().add_suffix("2"),
            col("open").shift(3).expanding().add_suffix("3"),
            col('high', 'low').shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            col("plus_dm", "minus_dm").sum().rolling(16).alias("a13", "a14"),
            (col("high") - col("a5")).alias("a6"),
            (col("a5") - col("low")).alias("a7"),
            col(
                "open_1",
                "high_1",
                "low_1",
                "close_1",
                "open_2",
                "high_2",
                "low_2",
                col("close").shift(2).expanding(),
            ).mean(axis=1).alias("a1"),
            col("open_2", "open_1", "open_3", "open").mean(axis=1).alias("a2"),
        )
        .with_cols(
            (col("a13") * 100.0 / col("a10")).alias("a15"),
            (col("a14") * 100.0 / col("a10")).alias("a16"),
        )
        .with_cols(
            ((col("a16") - col("a15")).abs() / (col("a16") + col("a15")) * 100.0).alias("dx_like"),
        )
        .with_cols(
            col("dx_like").mean().rolling(10).alias("a17"),
            col('a6', 'a7').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("a17").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("hhmm") >= trade_begin)
                & (col("hhmm") <= last_trade)
                & (col("a17_1") > 11.9)
                & (col("a6_1") < col("a7_1"))
                & (col("high") > col("high_1"))
                & (col("a1") > col("a2"))
                & (col("a9") >= 1.58)
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("hhmm") >= trade_begin)
                & (col("hhmm") <= last_trade)
                & (col("a17_1") > 11.9)
                & (col("a6_1") > col("a7_1"))
                & (col("low") < col("low_1"))
                & (col("a1") < col("a2"))
                & (col("a9") >= 1.58)
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(_pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"))
        .with_cols(_four_signals_from_pos_expr("pos_pre"))
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "high_1", "entry_long").stra.t02_track_best_since_entry(False).expanding().alias("best_high"),
            col("open_short_pre", "exit_short_pre", "low_1", "entry_short").stra.t02_track_best_since_entry(True).expanding().alias("best_low"),
        )
        .with_cols(
            col(col("entry_long") * 0.9927, col("best_high") * 0.9899).max(axis=1).alias("long_stop"),
            col(col("entry_short") * 1.0073, col("best_low") * 1.0101).min(axis=1).alias("short_stop"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("bars_pos") > 0) & (col("low") <= col("long_stop"))).fill_null(False).alias("exit_long_raw"),
            ((col("pos_pre") == -1.0) & (col("bars_pos") > 0) & (col("high") >= col("short_stop"))).fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(_pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"))
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c31", "按 C31 源码核心实现：多维形态评分入场，持仓后按入场后极值构造动态保护线离场。")

def build_c32_expr() -> Expr:
    n = pms("c32_n", 5, 80, 5).limit(20)
    m = pms("c32_m", 0.5, 10.0, 0.5).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").ta.ema(n).expanding().alias("ema_c"),
            col("open").ta.ema(n).expanding().alias("ema_o"),
            col("high", "low", "close").ta.atr(n).expanding().alias("atr_n"),
            col("close").count().expanding().alias("bar_no"),
        )
        .with_cols(
            (col("ema_c") - col("ema_o")).alias("c_o"),
            (col("atr_n") * 0.1 * m).alias("band"),
        )
        .with_cols(
            col("c_o").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("c_o") > 0.0) & (col("c_o_1") <= 0.0)).fill_null(False).alias("b"),
            ((col("c_o") < 0.0) & (col("c_o_1") >= 0.0)).fill_null(False).alias("s"),
        )
        .with_cols(
            col("b", col("high") + col("band"), col.null).if_else().alias("price_bpk_seed"),
            col("b", col("low") - col("band"), col.null).if_else().alias("price_sp_seed"),
            col("s", col("low") - col("band"), col.null).if_else().alias("price_spk_seed"),
            col("s", col("high") + col("band"), col.null).if_else().alias("price_bp_seed"),
        )
        .with_cols(
            col("price_bpk_seed").ffill().expanding().alias("price_bpk"),
            col("price_sp_seed").ffill().expanding().alias("price_sp"),
            col("price_spk_seed").ffill().expanding().alias("price_spk"),
            col("price_bp_seed").ffill().expanding().alias("price_bp"),
        )
        .with_cols(
            ((col("c_o") > 0.0) & (col("close") >= col("price_bpk"))).fill_null(False).alias("buypk"),
            ((col("c_o") < 0.0) & (col("close") <= col("price_spk"))).fill_null(False).alias("sellpk"),
            col("s").fill_null(False).alias("sellp"),
            col("b").fill_null(False).alias("buyp"),
            (col("close") <= col("price_sp")).fill_null(False).alias("sells"),
            (col("close") >= col("price_bp")).fill_null(False).alias("buys"),
        )
        .with_cols(
            ((col("bar_no") > n) & col("buypk").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_long_raw"),
            ((col("bar_no") > n) & col("sellpk").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(_pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"))
        .with_cols(_four_signals_from_pos_expr("pos_pre"))
        .with_cols(
            _bars_pos_from_open_sig_expr("open_long_sig", "open_short_sig", "bars_pos"),
        )
        .with_cols(
            (col("sells", "sellp").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_long_src"),
            (col("buys", "buyp").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_short_src"),
        )
        .with_cols(
            col("exit_long_src").shift(1).expanding().fill_null(False).alias("exit_long_raw"),
            col("exit_short_src").shift(1).expanding().fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(_pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"))
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c32", "按 C32 源码实现：C/O 指数均线差与 ATR 偏移线共同定义突破与保护价，信号反转/保护价触发离场。")

def build_c33_expr() -> Expr:
    n1 = pms("c33_n1", 10, 200, 10).limit(20)
    n2 = pms("c33_n2", 20, 300, 20).limit(20)
    n3 = pms("c33_n3", 40, 500, 40).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("bar_no"),
            col("close").ta.ema(n1).expanding().alias("ema_n1"),
            col("close").ta.ema(n1 * 2).expanding().alias("ema_n1_2"),
            col("close").ta.ema(n1 * 4).expanding().alias("ema_n1_4"),
            col("close").ta.ema(n2).expanding().alias("ema_n2"),
            col("close").ta.ema(n2 * 2).expanding().alias("ema_n2_2"),
            col("close").ta.ema(n2 * 4).expanding().alias("ema_n2_4"),
            col("close").ta.ema(n3).expanding().alias("ema_n3"),
            col("close").ta.ema(n3 * 2).expanding().alias("ema_n3_2"),
            col("close").ta.ema(n3 * 4).expanding().alias("ema_n3_4"),
        )
        .with_cols(
            ((col("ema_n1") + col("ema_n1_2") + col("ema_n1_4")) / 3.0).alias("pubu1"),
            ((col("ema_n2") + col("ema_n2_2") + col("ema_n2_4")) / 3.0).alias("pubu2"),
            ((col("ema_n3") + col("ema_n3_2") + col("ema_n3_4")) / 3.0).alias("pubu3"),
        )
        .with_cols(
            ((col("close") > col("pubu1")) & (col("pubu1") > col("pubu2")) & (col("pubu2") > col("pubu3"))).fill_null(False).alias("buypk"),
            ((col("close") < col("pubu1")) & (col("pubu1") < col("pubu2")) & (col("pubu2") < col("pubu3"))).fill_null(False).alias("sellpk"),
            (col("close") < col("pubu3")).fill_null(False).alias("sellp"),
            (col("close") > col("pubu3")).fill_null(False).alias("buyp"),
        )
        .with_cols(
            ((col("bar_no") > n3) & col("buypk").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_long_raw"),
            ((col("bar_no") > n3) & col("sellpk").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(_pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"))
        .with_cols(_four_signals_from_pos_expr("pos_pre"))
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            ((col("close") < col("pubu2")) & (col("pubu1") < col("pubu2")) & (col("close") > col("entry_long"))).fill_null(False).alias("selly"),
            ((col("close") > col("pubu2")) & (col("pubu1") > col("pubu2")) & (col("close") < col("entry_short"))).fill_null(False).alias("buyy"),
        )
        .with_cols(
            (col("selly", "sellp").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_long_src"),
            (col("buyy", "buyp").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_short_src"),
        )
        .with_cols(
            col("exit_long_src").shift(1).expanding().fill_null(False).alias("exit_long_raw"),
            col("exit_short_src").shift(1).expanding().fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(_pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"))
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c33", "按 C33 源码实现：三层瀑布均线多空排列入场，短级趋势转弱与主趋势线破位共同离场。")

def build_c34_expr() -> Expr:
    n1 = pms("c34_n1", 1, 10, 1).limit(20)
    n2 = pms("c34_n2", 1, 20, 1).limit(20)
    n4_u64 = (col.lit(n2) + col.lit(n1) + col.lit(n2)).to(pl.UInt64)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("bar_no"),
            ((col("high") + col("low")) * 0.5).alias("hl2"),
            col('high', 'low').shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            col("hl2").mean().rolling((n2 + n1 + n2), (n2 + n1 + n2)).alias("y"),
            col("hl2").mean().rolling((n1 + n2), (n1 + n2)).alias("r"),
            col("hl2").mean().rolling(n2).alias("g"),
            col("high").max().rolling(5).alias("hh5"),
            col("low").min().rolling(5).alias("ll5"),
        )
        .with_cols(
            (col("high_2") == col("hh5")).fill_null(False).alias("top_fractal_cond"),
            (col("low_2") == col("ll5")).fill_null(False).alias("bottom_fractal_cond"),
            col("y", "r", "g").max(axis=1).alias("max_yrg"),
            col("y", "r", "g").min(axis=1).alias("min_yrg"),
        )
        .with_cols(
            col("top_fractal_cond", "high_2", col.null).if_else().alias("top_seed"),
            col("bottom_fractal_cond", "low_2", col.null).if_else().alias("bottom_seed"),
        )
        .with_cols(
            col("top_seed").ffill().expanding().alias("top_fractal"),
            col("bottom_seed").ffill().expanding().alias("bottom_fractal"),
        )
        .with_cols(
            ((col("close") >= col("top_fractal")) & (col("top_fractal") > col("max_yrg"))).fill_null(False).alias("buypk"),
            ((col("close") <= col("bottom_fractal")) & (col("bottom_fractal") < col("min_yrg"))).fill_null(False).alias("sellpk"),
            (col("close") < col("y")).fill_null(False).alias("sellp"),
            (col("close") > col("y")).fill_null(False).alias("buyp"),
        )
        .with_cols(
            ((col("bar_no") > n4_u64) & col("buypk").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_long_raw"),
            ((col("bar_no") > n4_u64) & col("sellpk").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(_pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"))
        .with_cols(_four_signals_from_pos_expr("pos_pre"))
        .with_cols(
            _bars_pos_from_open_sig_expr("open_long_sig", "open_short_sig", "bars_pos"),
        )
        .with_cols(
            ((col("sellp")) & (col("bars_pos") > 1)).fill_null(False).alias("exit_long_src"),
            ((col("buyp")) & (col("bars_pos") > 1)).fill_null(False).alias("exit_short_src"),
        )
        .with_cols(
            col("exit_long_src").shift(1).expanding().fill_null(False).alias("exit_long_raw"),
            col("exit_short_src").shift(1).expanding().fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(_pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"))
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c34", "按 C34 源码实现：三线结构与分形突破入场，跌破/上破主线离场。")

def build_c35_expr() -> Expr:
    n = pms("c35_n", 20, 300, 20).limit(20)
    m = pms("c35_m", 0.5, 10.0, 0.5).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col.lit(m).alias("m"),
            col("close").count().expanding().alias("bar_no"),
            col('high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("close").mean().rolling(n).alias("man"),
            col("high").max().rolling(n).alias("hh_n"),
            col("low").min().rolling(n).alias("ll_n"),
            ((col("high") - col("close")) * col("volume")).alias("s_k1_num"),
            ((col("close") - col("low")) * col("volume")).alias("b_k1_num"),
            ((col("high") - col("low")) * col("volume")).alias("hl_v"),
            (col(col("close") > col("open"), col("close") - col("open"), 0.0).if_else() * col("volume")).alias("co_v"),
            (col(col("close") < col("open"), col("open") - col("close"), 0.0).if_else() * col("volume")).alias("oc_v"),
            ((col("close") - col("open")).abs() * col("volume")).alias("abs_co_v"),
            (col("high") - col("low")).alias("hl"),
        )
        .with_cols(
            col("s_k1_num", "b_k1_num", "hl_v", "co_v", "oc_v", "abs_co_v")
            .sum()
            .rolling(n)
            .alias("s_k1_sum", "b_k1_sum", "hl_v_sum", "co_v_sum", "oc_v_sum", "abs_co_v_sum"),
            col("hl").mean().rolling(n).alias("hl_avg"),
        )
        .with_cols(
            (col("close") > col("man")).fill_null(False).alias("b_ma"),
            (col("close") < col("man")).fill_null(False).alias("s_ma"),
            (col("b_k1_sum") > 0.5 * col("hl_v_sum")).fill_null(False).alias("b_k1"),
            (col("s_k1_sum") > 0.5 * col("hl_v_sum")).fill_null(False).alias("s_k1"),
            (col("co_v_sum") > 0.5 * col("abs_co_v_sum")).fill_null(False).alias("b_k2"),
            (col("oc_v_sum") > 0.5 * col("abs_co_v_sum")).fill_null(False).alias("s_k2"),
            (col("m") * col("hl_avg")).alias("stoploss"),
        )
        .with_cols(
            (
                col("b_k1")
                & col("b_k2")
                & col("b_ma")
                & (col("high") >= col("hh_n"))
            )
            .fill_null(False)
            .alias("buypk"),
            (
                col("s_k1")
                & col("s_k2")
                & col("s_ma")
                & (col("low") <= col("ll_n"))
            )
            .fill_null(False)
            .alias("sellpk"),
        )
        .with_cols(
            col("buypk").shift(1).expanding().fill_null(False).alias("open_long_raw"),
            col("sellpk").shift(1).expanding().fill_null(False).alias("open_short_raw"),
        )
        .with_cols(_pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"))
        .with_cols(_four_signals_from_pos_expr("pos_pre"))
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "high_1", "entry_long").stra.t02_track_best_since_entry(False).expanding().alias("best_high"),
            col("open_short_pre", "exit_short_pre", "low_1", "entry_short").stra.t02_track_best_since_entry(True).expanding().alias("best_low"),
        )
        .with_cols(
            (col("close") < (col("entry_long") - col("stoploss"))).fill_null(False).alias("sells"),
            (col("close") > (col("entry_short") + col("stoploss"))).fill_null(False).alias("buys"),
            (col("s_ma") & (col("best_high") > (col("entry_long") + col("stoploss")))).fill_null(False).alias("selly"),
            (col("b_ma") & (col("best_low") < (col("entry_short") - col("stoploss")))).fill_null(False).alias("buyy"),
        )
        .with_cols(
            (col("sells", "selly").any(axis=1) & (col("bars_pos") > 0)).fill_null(False).alias("exit_long_src"),
            (col("buys", "buyy").any(axis=1) & (col("bars_pos") > 0)).fill_null(False).alias("exit_short_src"),
        )
        .with_cols(
            col("exit_long_src").shift(1).expanding().fill_null(False).alias("exit_long_raw"),
            col("exit_short_src").shift(1).expanding().fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(_pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"))
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c35", "按 C35 源码实现：量价分布+均线方向+N 周期极值突破入场，固定幅度止损与趋势反转止盈离场。")

def build_c36_expr() -> Expr:
    param1 = pms("c36_param1", 20, 600, 20).limit(20)
    param2 = pms("c36_param2", 200, 2000, 200).limit(20)
    param3 = pms("c36_param3", 0.2, 10.0, 0.2).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col.lit(param3).alias("p3"),
            col("close").count().expanding().alias("bar_no"),
            col('high', 'low', 'close').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("close").mean().rolling(param2).alias("var2"),
        )
        .with_cols(
            col("var2").mean().rolling(param1).alias("var3"),
            col((col("high") + col("low")) <= (col("high_1") + col("low_1")), 0.0, col((col("high") - col("high_1")).abs(), (col("low") - col("low_1")).abs()).max(axis=1)).if_else()
            .alias("var4"),
            col((col("high") + col("low")) >= (col("high_1") + col("low_1")), 0.0, col((col("high") - col("high_1")).abs(), (col("low") - col("low_1")).abs()).max(axis=1)).if_else()
            .alias("var5"),
        )
        .with_cols(
            col("var4", "var5").sum().rolling(param1).alias("sum4", "sum5"),
        )
        .with_cols(
            col((col("sum4") + col("sum5")) != 0.0, col("sum4") / (col("sum4") + col("sum5")), 0.0).if_else().alias("var6"),
            col((col("sum4") + col("sum5")) != 0.0, col("sum5") / (col("sum4") + col("sum5")), 0.0).if_else().alias("var7"),
        )
        .with_cols(
            (col("var6") - col("var7")).alias("var8"),
        )
        .with_cols(
            col("var8").mean().rolling((param1 * 2), (param1 * 2)).alias("var9"),
        )
        .with_cols(
            col("var9").mean().rolling(param1).alias("var10"),
        )
        .with_cols(
            (
                (col("bar_no") > param2)
                & (col("close") > col("var2"))
                & (col("var2") > col("var3"))
                & (col("var8") > 0.0)
                & (col("var9") > col("var10"))
            )
            .fill_null(False)
            .alias("buyk"),
            (
                (col("bar_no") > param2)
                & (col("close") < col("var2"))
                & (col("var2") < col("var3"))
                & (col("var8") < 0.0)
                & (col("var9") < col("var10"))
            )
            .fill_null(False)
            .alias("sellk"),
        )
        .with_cols(
            col("buyk").shift(1).expanding().fill_null(False).alias("open_long_raw"),
            col("sellk").shift(1).expanding().fill_null(False).alias("open_short_raw"),
        )
        .with_cols(_pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"))
        .with_cols(_four_signals_from_pos_expr("pos_pre"))
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            (col("close") < (col("entry_long") * (1.0 - col("p3") * 0.01))).fill_null(False).alias("sells"),
            (col("close") > (col("entry_short") * (1.0 + col("p3") * 0.01))).fill_null(False).alias("buys"),
            ((col("close") < col("var2")) & (col("close") > (col("entry_long") * (1.0 + col("p3") * 0.01)))).fill_null(False).alias("selly"),
            ((col("close") > col("var2")) & (col("close") < (col("entry_short") * (1.0 - col("p3") * 0.01)))).fill_null(False).alias("buyy"),
        )
        .with_cols(
            (col("sells", "selly").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_long_src"),
            (col("buys", "buyy").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_short_src"),
        )
        .with_cols(
            col("exit_long_src").shift(1).expanding().fill_null(False).alias("exit_long_raw"),
            col("exit_short_src").shift(1).expanding().fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(_pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"))
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c36", "按 C36 源码实现：双均线方向+DDI 结构入场，固定止损与盈利后趋势反转止盈离场。")

def build_c37_expr() -> Expr:
    param1 = pms("c37_param1", 2, 20, 5).limit(20)
    param2 = pms("c37_param2", 10, 100, 40).limit(20)
    param3 = pms("c37_param3", 30, 300, 150).limit(20)
    n_tr = pms("c37_n_tr", 10, 60, 26).limit(20)
    short_macd = pms("c37_short_macd", 4, 30, 12).limit(20)
    long_macd = pms("c37_long_macd", 10, 80, 26).limit(20)
    m_macd = pms("c37_m_macd", 3, 30, 9).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("bar_no"),
            col("close").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("high", "low", "close").ta.trange().expanding().alias("tr"),
            col("close").ta.ema(short_macd * param1).expanding().alias("ema_s_1"),
            col("close").ta.ema(long_macd * param1).expanding().alias("ema_l_1"),
            col("close").ta.ema(short_macd * param2).expanding().alias("ema_s_2"),
            col("close").ta.ema(long_macd * param2).expanding().alias("ema_l_2"),
            col("high").max().rolling(param3).alias("hh_p3"),
            col("low").min().rolling(param3).alias("ll_p3"),
        )
        .with_cols(
            col("tr").mean().rolling(n_tr).alias("atr_v"),
            (col("ema_s_1") - col("ema_l_1")).alias("var4"),
            (col("ema_s_2") - col("ema_l_2")).alias("var7"),
            ((col("hh_p3") + col("ll_p3")) * 0.5).alias("var12"),
        )
        .with_cols(
            col("var4").ta.ema(m_macd * param1).expanding().alias("var5"),
            col("var7").ta.ema(m_macd * param2).expanding().alias("var8"),
        )
        .with_cols(
            (col("var4") - col("var5")).alias("var6"),
            (col("var7") - col("var8")).alias("var9"),
            col('close', 'hh_p3', 'll_p3').shift(1).expanding().alias('close_1b', 'hh_p3_1', 'll_p3_1'),
        )
        .with_cols(
            ((col("close") > col("hh_p3")) & (col("close_1b") <= col("hh_p3_1"))).fill_null(False).alias("bool1"),
            ((col("close") < col("ll_p3")) & (col("close_1b") >= col("ll_p3_1"))).fill_null(False).alias("bool2"),
        )
        .with_cols(
            ((col("bar_no") > param3) & col("bool1").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_long_raw"),
            ((col("bar_no") > param3) & col("bool2").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(_pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"))
        .with_cols(_four_signals_from_pos_expr("pos_pre"))
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "var12", "stop_long_now", "stop_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "stop_long_now").stra.t02_track_entry_price().expanding().alias("stop_long"),
            col("open_short_pre", "exit_short_pre", "stop_short_now").stra.t02_track_entry_price().expanding().alias("stop_short"),
        )
        .with_cols(
            ((col("close") <= col("stop_long")) & (col("bars_pos") > 1)).fill_null(False).alias("exit_long_raw"),
            ((col("close") >= col("stop_short")) & (col("bars_pos") > 1)).fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(_pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"))
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c37", "按 C37 源码核心实现：趋势突破触发初次入场，入场时以中线 VAR12 锁定保护位并据此离场。")

def build_c38_expr() -> Expr:
    param1 = pms("c38_param1", 20, 400, 20).limit(20)
    param2 = pms("c38_param2", 2, 50, 2).limit(20)
    param3 = pms("c38_param3", 0.2, 20.0, 0.2).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("bar_no"),
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("high") + col("low") + col("close")) / 3.0).alias("var2"),
            ((col("close") > col("close_1")) & (col("high") > col("high_1")) & (col("close") > col("open"))).fill_null(False).alias("bool1"),
            ((col("close") < col("close_1")) & (col("low") < col("low_1")) & (col("close") < col("open"))).fill_null(False).alias("bool2"),
        )
        .with_cols(
            col("var2").mean().rolling(param1).alias("var3"),
        )
        .with_cols(
            col("var3").ta.ema(param2).expanding().alias("var4"),
        )
        .with_cols(
            (
                (col("bar_no") > param1)
                & (col("close") > col("var3"))
                & col("bool1")
                & (col("var3") > col("var4"))
            )
            .fill_null(False)
            .alias("buypk"),
            (
                (col("bar_no") > param1)
                & (col("close") < col("var3"))
                & col("bool2")
                & (col("var3") < col("var4"))
            )
            .fill_null(False)
            .alias("sellpk"),
        )
        .with_cols(
            col("buypk").shift(1).expanding().fill_null(False).alias("open_long_raw"),
            col("sellpk").shift(1).expanding().fill_null(False).alias("open_short_raw"),
        )
        .with_cols(_pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"))
        .with_cols(_four_signals_from_pos_expr("pos_pre"))
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            (col("close") < (col("entry_long") * (1.0 - param3 * 0.01))).fill_null(False).alias("sells"),
            (col("close") > (col("entry_short") * (1.0 + param3 * 0.01))).fill_null(False).alias("buys"),
        )
        .with_cols(
            (col("sells") & (col("bars_pos") > 1)).fill_null(False).alias("exit_long_src"),
            (col("buys") & (col("bars_pos") > 1)).fill_null(False).alias("exit_short_src"),
        )
        .with_cols(
            col("exit_long_src").shift(1).expanding().fill_null(False).alias("exit_long_raw"),
            col("exit_short_src").shift(1).expanding().fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(_pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"))
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c38", "按 C38 源码实现：双均线趋势+K 线形态突破入场，固定比例止损离场。")

def build_c39_expr() -> Expr:
    stoploss = pms("c39_stoploss", 0.2, 20.0, 0.2).limit(20)
    n = pms("c39_n", 10, 200, 10).limit(20)
    n_tmp = pms("c39_n_tmp", 0.5, 10.0, 0.5).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("bar_no"),
            col("close").shift(1).expanding().add_suffix("1"),
            col("close").shift(29).expanding().add_suffix("29"),
        )
        .with_cols(
            col("close").mean().rolling(n).alias("mac"),
            col("close").std().rolling(n).alias("tmp"),
            col("high").max().rolling(30).alias("hh_30"),
            col("low").min().rolling(30).alias("ll_30"),
            col("high").max().rolling(9).alias("hh_9"),
            col("low").min().rolling(9).alias("ll_9"),
        )
        .with_cols(
            (col("mac") + n_tmp * col("tmp")).alias("top"),
            (col("mac") - n_tmp * col("tmp")).alias("bottom"),
            col((col("hh_30") - col("ll_30")) != 0.0, (col("close") - col("close_29")).abs() / (col("hh_30") - col("ll_30")) * 100.0, 0.0).if_else()
            .alias("cmi"),
            col((col("hh_9") - col("ll_9")) != 0.0, (col("close") - col("ll_9")) / (col("hh_9") - col("ll_9")) * 100.0, 0.0).if_else()
            .alias("rsv"),
        )
        .with_cols(
            col("rsv").mean().rolling(3).alias("k"),
        )
        .with_cols(
            col("k").mean().rolling(3).alias("d"),
            col("k").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("d").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("k") > col("d")) & (col("d") < 30.0)).fill_null(False).alias("bkd"),
            ((col("k") < col("d")) & (col("d") > 70.0)).fill_null(False).alias("skd"),
            (col("close") > col("mac")).fill_null(False).alias("bboll"),
            (col("close") < col("mac")).fill_null(False).alias("sboll"),
        )
        .with_cols(
            ((col("cmi") < 20.0) & col("bkd")).fill_null(False).alias("buypk1"),
            ((col("cmi") < 20.0) & col("skd")).fill_null(False).alias("sellpk1"),
            ((col("cmi") >= 20.0) & (col("close") > col("top"))).fill_null(False).alias("buypk2"),
            ((col("cmi") >= 20.0) & (col("close") < col("bottom"))).fill_null(False).alias("sellpk2"),
        )
        .with_cols(
            ((col("bar_no") > n) & col("buypk1").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_long_1"),
            ((col("bar_no") > n) & col("buypk2").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_long_2"),
            ((col("bar_no") > n) & col("sellpk1").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_short_1"),
            ((col("bar_no") > n) & col("sellpk2").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_short_2"),
        )
        .with_cols(
            col("open_long_1", "open_long_2").any(axis=1).fill_null(False).alias("open_long_raw"),
            col("open_short_1", "open_short_2").any(axis=1).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(_pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"))
        .with_cols(_four_signals_from_pos_expr("pos_pre"))
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
            col("open_long_pre", col("cmi").shift(1).expanding(), col.null).if_else().alias("cmi_long_now"),
            col("open_short_pre", col("cmi").shift(1).expanding(), col.null).if_else().alias("cmi_short_now"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now").stra.t02_track_entry_price().expanding().alias("entry_short"),
            col("open_long_pre", "exit_long_pre", "cmi_long_now").stra.t02_track_entry_price().expanding().alias("cmi_bk"),
            col("open_short_pre", "exit_short_pre", "cmi_short_now").stra.t02_track_entry_price().expanding().alias("cmi_sk"),
        )
        .with_cols(
            (
                (col("cmi_bk") < 20.0)
                & (col("close") > (col("entry_long") * (1.0 + 0.03 * stoploss)))
                & (col("k") < col("d"))
            )
            .fill_null(False)
            .alias("selly1"),
            (
                (col("cmi_bk") >= 20.0)
                & (col("close") > (col("entry_long") * (1.0 + 0.03 * stoploss)))
                & col("sboll")
            )
            .fill_null(False)
            .alias("selly2"),
            (
                (col("cmi_bk") >= 20.0)
                & (col("close") < (col("entry_long") * (1.0 - 0.01 * stoploss)))
                & col("sboll")
            )
            .fill_null(False)
            .alias("sells2"),
            (
                (col("cmi_sk") < 20.0)
                & (col("close") < (col("entry_short") * (1.0 - 0.03 * stoploss)))
                & (col("k") > col("d"))
            )
            .fill_null(False)
            .alias("buyy1"),
            (
                (col("cmi_sk") >= 20.0)
                & (col("close") < (col("entry_short") * (1.0 - 0.03 * stoploss)))
                & col("bboll")
            )
            .fill_null(False)
            .alias("buyy2"),
            (
                (col("cmi_sk") >= 20.0)
                & (col("close") > (col("entry_short") * (1.0 + 0.01 * stoploss)))
                & col("bboll")
            )
            .fill_null(False)
            .alias("buys2"),
        )
        .with_cols(
            (col("selly1", "selly2", "sells2").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_long_src"),
            (col("buyy1", "buyy2", "buys2").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_short_src"),
        )
        .with_cols(
            col("exit_long_src").shift(1).expanding().fill_null(False).alias("exit_long_raw"),
            col("exit_short_src").shift(1).expanding().fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(_pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"))
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c39", "按 C39 源码核心实现：CMI 模式切换下的 KD/布林突破入场，分模式止盈止损离场。")

def build_c41_expr() -> Expr:
    length = pms("c41_length", 20, 300, 20).limit(20)
    matr = pms("c41_matr", 1.0, 50.0, 1.0).limit(20)
    m_hl = pms("c41_m_hl", 2, 100, 2).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col.lit(matr).alias("matr"),
            col("close").count().expanding().alias("bar_no"),
            col("high", "low", "close").ta.trange().expanding().alias("tr"),
        )
        .with_cols(
            col("high").max().rolling(length).alias("hh"),
            col("low").min().rolling(length).alias("ll"),
            col("tr").mean().rolling(length).alias("atr"),
        )
        .with_cols(
            ((col("hh") + col("ll")) * 0.5).alias("midhl"),
        )
        .with_cols(
            ((col("high") >= col("hh")) & (col("close") > col("midhl"))).fill_null(False).alias("buypk"),
            ((col("low") <= col("ll")) & (col("close") < col("midhl"))).fill_null(False).alias("sellpk"),
        )
        .with_cols(
            ((col("bar_no") > length) & col("buypk").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_long_raw"),
            ((col("bar_no") > length) & col("sellpk").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(_pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"))
        .with_cols(_four_signals_from_pos_expr("pos_pre"))
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
            col("open_long_pre", "atr", col.null).if_else().alias("atr_long_now"),
            col("open_short_pre", "atr", col.null).if_else().alias("atr_short_now"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now").stra.t02_track_entry_price().expanding().alias("entry_short"),
            col("open_long_pre", "exit_long_pre", "atr_long_now").stra.t02_track_entry_price().expanding().alias("atr_long_entry"),
            col("open_short_pre", "exit_short_pre", "atr_short_now").stra.t02_track_entry_price().expanding().alias("atr_short_entry"),
        )
        .with_cols(
            col("low").min().rolling(m_hl).alias("low_mhl"),
            col("high").max().rolling(m_hl).alias("high_mhl"),
            ((col("bars_pos") + 1).cast(pl.Float64)).alias("n_entry"),
        )
        .with_cols(
            (col("low_mhl") + col("n_entry") * 0.05 * col("atr")).alias("price_selly"),
            (col("high_mhl") - col("n_entry") * 0.05 * col("atr")).alias("price_buyy"),
        )
        .with_cols(
            (
                (col("close") <= (col("entry_long") - col("matr") * col("atr_long_entry")))
            )
            .fill_null(False)
            .alias("sells"),
            (
                (col("close") >= (col("entry_short") + col("matr") * col("atr_short_entry")))
            )
            .fill_null(False)
            .alias("buys"),
            (
                (col("close") > (col("entry_long") + col("matr") * col("atr_long_entry")))
                & (col("close") <= col("price_selly"))
            )
            .fill_null(False)
            .alias("selly"),
            (
                (col("close") < (col("entry_short") - col("matr") * col("atr_short_entry")))
                & (col("close") >= col("price_buyy"))
            )
            .fill_null(False)
            .alias("buyy"),
        )
        .with_cols(
            (col("sells", "selly").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_long_src"),
            (col("buys", "buyy").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_short_src"),
        )
        .with_cols(
            col("exit_long_src").shift(1).expanding().fill_null(False).alias("exit_long_raw"),
            col("exit_short_src").shift(1).expanding().fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(_pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"))
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c41", "按 C41 源码核心实现：高低轨突破入场，入场即设 ATR 止损，并叠加基于 M_HL 的动态止盈线。")

def build_c42_expr() -> Expr:
    length_mac = pms("c42_length_mac", 50, 600, 50).limit(20)
    length_emac = pms("c42_length_emac", 2, 80, 2).limit(20)
    highkd = pms("c42_highkd", 50.0, 95.0, 50.0).limit(20)
    stoploss = pms("c42_stoploss", 0.2, 20.0, 0.2).limit(20)
    darkback = pms("c42_darkback", 0.5, 20.0, 0.5).limit(20)
    length = pms("c42_kd_length", 3, 80, 3).limit(20)
    slow_length = pms("c42_kd_slow_length", 2, 30, 2).limit(20)
    smooth_length = pms("c42_kd_smooth_length", 2, 30, 2).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("bar_no"),
            col('high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("close").mean().rolling(length_mac).alias("mac"),
            col("close").ta.ema(length_emac).expanding().alias("emac"),
        )
        .with_cols(
            (col("open") - col("emac")).abs().alias("bias"),
        )
        .with_cols(
            col("bias").mean().expanding().alias("mabias"),
            col("high").max().rolling(length).alias("highest_v"),
            col("low").min().rolling(length).alias("lowest_v"),
        )
        .with_cols(
            (col("highest_v") - col("lowest_v")).alias("hl_v"),
            (col("close") - col("lowest_v")).alias("cl_v"),
        )
        .with_cols(
            col("hl_v", "cl_v").sum().rolling(slow_length).alias("sum_hl", "sum_cl"),
        )
        .with_cols(
            col(col("sum_hl") != 0.0, col("sum_cl") / col("sum_hl") * 100.0, 0.0).if_else().alias("k_value"),
        )
        .with_cols(
            col("k_value").mean().rolling(smooth_length).alias("d_value"),
        )
        .with_cols(
            (
                (col("close") > col("mac"))
                & (col("bias") < 2.0 * col("mabias"))
                & (col("k_value") > col("d_value"))
                & (col("d_value") > highkd)
            )
            .fill_null(False)
            .alias("buypk"),
            (
                (col("close") < col("mac"))
                & (col("bias") < 2.0 * col("mabias"))
                & (col("k_value") < col("d_value"))
                & (col("d_value") < (100.0 - highkd))
            )
            .fill_null(False)
            .alias("sellpk"),
        )
        .with_cols(
            ((col("bar_no") > length_mac) & col("buypk").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_long_raw"),
            ((col("bar_no") > length_mac) & col("sellpk").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(_pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"))
        .with_cols(_four_signals_from_pos_expr("pos_pre"))
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "high_1", "entry_long").stra.t02_track_best_since_entry(False).expanding().alias("bkhigh"),
            col("open_short_pre", "exit_short_pre", "low_1", "entry_short").stra.t02_track_best_since_entry(True).expanding().alias("sklow"),
        )
        .with_cols(
            (col("close") <= (col("entry_long") * (1.0 - 0.01 * stoploss))).fill_null(False).alias("sells"),
            (col("close") >= (col("entry_short") * (1.0 + 0.01 * stoploss))).fill_null(False).alias("buys"),
            (col("close") <= (col("bkhigh") * (1.0 - 0.01 * darkback))).fill_null(False).alias("selly"),
            (col("close") >= (col("sklow") * (1.0 + 0.01 * darkback))).fill_null(False).alias("buyy"),
        )
        .with_cols(
            (col("sells", "selly").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_long_src"),
            (col("buys", "buyy").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_short_src"),
        )
        .with_cols(
            col("exit_long_src").shift(1).expanding().fill_null(False).alias("exit_long_raw"),
            col("exit_short_src").shift(1).expanding().fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(_pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"))
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c42", "按 C42 源码实现：MA 方向+BIA S 过滤+KD 条件入场，固定止损与高低回撤止盈离场。")

def build_c44_expr() -> Expr:
    n_day = pms("c44_n_day", 2, 40, 10).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("datetime").dt.date().alias("day"),
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
            col("close").count().expanding().alias("bar_no"),
        )
        .with_cols(
            col("day").shift(1).expanding().add_suffix("1"),
            col(
                col("datetime").count().alias("nn"),
                col("open").first_value().alias("o_today"),
                col("high").max().alias("h_today"),
                col("low").min().alias("l_today"),
            ).expanding().over(col("day")),
        )
        .with_cols(
            (col("day") != col("day_1")).fill_null(True).alias("is_new_day"),
            col('h_today', 'l_today').shift(1).expanding().add_suffix("1"),
            (col("nn") * col.lit(n_day).to(pl.UInt32).cast(pl.UInt64)).cast(pl.UInt32).alias("length_u"),
        )
        .with_cols(
            col("is_new_day", "close_1", col.null).if_else().alias("c_yesterday_seed"),
            col("is_new_day", "h_today_1", col.null).if_else().alias("h_yesterday_seed"),
            col("is_new_day", "l_today_1", col.null).if_else().alias("l_yesterday_seed"),
        )
        .with_cols(
            col("c_yesterday_seed").ffill().expanding().alias("c_yesterday"),
            col("h_yesterday_seed").ffill().expanding().alias("h_yesterday"),
            col("l_yesterday_seed").ffill().expanding().alias("l_yesterday"),
        )
        .with_cols(
            (col("h_yesterday") - col("l_yesterday")).alias("y_range"),
            col((col("h_yesterday") - col("c_yesterday")).abs(), (col("l_yesterday") - col("c_yesterday")).abs()).min(axis=1).alias("orb"),
        )
        .with_cols(
            col("y_range").mean().rolling_dynamic("length_u", window_max=16384, min_samples=1).alias("distance"),
        )
        .with_cols(
            col("orb", col("distance") * 0.1).max(axis=1).alias("band"),
            col("close").mean().rolling_dynamic("length_u", window_max=16384, min_samples=1).alias("mac"),
            col("high").max().rolling_dynamic("length_u", window_max=16384, min_samples=1).alias("ma_hh"),
            col("low").min().rolling_dynamic("length_u", window_max=16384, min_samples=1).alias("ma_ll"),
        )
        .with_cols(
            (col("o_today") + col("band")).alias("upband"),
            (col("o_today") - col("band")).alias("downband"),
        )
        .with_cols(
            (
                (col("close") > col("upband"))
                & (col("close") > col("mac"))
                & (col("close") > col("ma_hh"))
            )
            .fill_null(False)
            .alias("buypk"),
            (
                (col("close") < col("downband"))
                & (col("close") < col("mac"))
                & (col("close") < col("ma_ll"))
            )
            .fill_null(False)
            .alias("sellpk"),
        )
        .with_cols(
            col("buypk").shift(1).expanding().fill_null(False).alias("open_long_raw"),
            col("sellpk").shift(1).expanding().fill_null(False).alias("open_short_raw"),
        )
        .with_cols(_pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"))
        .with_cols(_four_signals_from_pos_expr("pos_pre"))
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            ((col("close") < col("mac")) & (col("close") > col("entry_long"))).fill_null(False).alias("selly"),
            ((col("close") > col("mac")) & (col("close") < col("entry_short"))).fill_null(False).alias("buyy"),
        )
        .with_cols(
            (col("selly") & (col("bars_pos") > 1)).fill_null(False).alias("exit_long_src"),
            (col("buyy") & (col("bars_pos") > 1)).fill_null(False).alias("exit_short_src"),
        )
        .with_cols(
            col("exit_long_src").shift(1).expanding().fill_null(False).alias("exit_long_raw"),
            col("exit_short_src").shift(1).expanding().fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(_pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"))
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c44", "按 C44 源码主逻辑实现：基于昨日区间构造 ORB 轨道并叠加均线过滤入场，均线回归离场。")

def build_c45_expr() -> Expr:
    n = pms("c45_n", 8, 8, 8).limit(20)
    m = pms("c45_m", 5, 5, 5).limit(20)
    score_terms = []
    for i in range(1, 9):
        ma_short = col("close").mean().rolling(i)
        ma_long = col("close").mean().rolling(i * 5)
        score_terms.append(col(ma_short > ma_long, 1.0, -1.0).if_else())
    score_expr = score_terms[0]
    for term in score_terms[1:]:
        score_expr = score_expr + term
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("bar_no"),
            score_expr.alias("score"),
        )
        .with_cols(
            (col("score") > 4.0).fill_null(False).alias("buypk"),
            (col("score") < 4.0).fill_null(False).alias("sellpk"),
            (col("score") <= 0.0).fill_null(False).alias("sellp"),
            (col("score") >= 0.0).fill_null(False).alias("buyp"),
        )
        .with_cols(
            (col("buypk").shift(1).expanding().fill_null(False) & (col("bar_no") > 40)).fill_null(False).alias("open_long_raw"),
            (col("sellpk").shift(1).expanding().fill_null(False) & (col("bar_no") > 40)).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(_pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"))
        .with_cols(_four_signals_from_pos_expr("pos_pre"))
        .with_cols(_bars_pos_from_open_sig_expr("open_long_sig", "open_short_sig", "bars_pos"))
        .with_cols(
            (col("sellp").shift(1).expanding().fill_null(False) & (col("bars_pos") > 1)).fill_null(False).alias("exit_long_raw"),
            (col("buyp").shift(1).expanding().fill_null(False) & (col("bars_pos") > 1)).fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(_pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"))
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c45", "按 C45 源码主逻辑实现：多组均线相对强弱投票形成 Score，阈值触发入场与离场。")

def build_c46_expr() -> Expr:
    p1 = pms("c46_param1", 5, 100, 5).limit(20)
    p2 = pms("c46_param2", 10, 200, 10).limit(20)
    p3 = pms("c46_param3", 20, 300, 20).limit(20)
    p4 = pms("c46_param4", 40, 500, 40).limit(20)
    p5 = pms("c46_param5", 60, 700, 60).limit(20)
    p6 = pms("c46_param6", 100, 1000, 100).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("bar_no"),
            col("close").ta.ema(p1).expanding().alias("var2"),
            col("close").ta.ema(p2).expanding().alias("var3"),
            col("close").ta.ema(p3).expanding().alias("var4"),
            col("close").ta.ema(p4).expanding().alias("var5"),
            col("close").ta.ema(p5).expanding().alias("var6"),
            col("close").ta.ema(p6).expanding().alias("var7"),
        )
        .with_cols(
            col("var5", "var6", "var7").max(axis=1).alias("var8"),
            col("var5", "var6", "var7").min(axis=1).alias("var9"),
            col('var4', 'var5').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("var2") >= col("var3")) & (col("var3") >= col("var4"))).fill_null(False).alias("buy_bool1"),
            ((col("var2") <= col("var3")) & (col("var3") <= col("var4"))).fill_null(False).alias("sell_bool1"),
            ((col("var4") > col("var5")) & (col("var4_1") <= col("var5_1"))).fill_null(False).alias("buy_bool2"),
            ((col("var4") < col("var5")) & (col("var4_1") >= col("var5_1"))).fill_null(False).alias("sell_bool2"),
            (col("var4") > col("var5")).fill_null(False).alias("buy_bool3"),
            (col("var4") < col("var5")).fill_null(False).alias("sell_bool3"),
        )
        .with_cols(
            (
                col("buy_bool1")
                & (col("var4") > col("var8"))
                & col("buy_bool2", "buy_bool3").any(axis=1)
            )
            .fill_null(False)
            .alias("buypk"),
            (
                col("sell_bool1")
                & (col("var4") < col("var9"))
                & col("sell_bool2", "sell_bool3").any(axis=1)
            )
            .fill_null(False)
            .alias("sellpk"),
            col("sell_bool1").fill_null(False).alias("sellp"),
            col("buy_bool1").fill_null(False).alias("buyp"),
        )
        .with_cols(
            ((col("bar_no") > p6) & col("buypk").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_long_raw"),
            ((col("bar_no") > p6) & col("sellpk").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(_pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"))
        .with_cols(_four_signals_from_pos_expr("pos_pre"))
        .with_cols(_bars_pos_from_open_sig_expr("open_long_sig", "open_short_sig", "bars_pos"))
        .with_cols(
            (col("sellp").shift(1).expanding().fill_null(False) & (col("bars_pos") > 1)).fill_null(False).alias("exit_long_raw"),
            (col("buyp").shift(1).expanding().fill_null(False) & (col("bars_pos") > 1)).fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(_pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"))
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c46", "按 C46 源码主逻辑实现：多层 EMA 结构共振入场，短中层结构反转离场。")

def build_c47_expr() -> Expr:
    length1 = pms("c47_length1", 2, 60, 2).limit(20)
    length2 = pms("c47_length2", 5, 120, 5).limit(20)
    m_hl = pms("c47_m_hl", 2, 80, 2).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("bar_no"),
            col("high", "low", "close").ta.trange().expanding().alias("tr"),
        )
        .with_cols(
            col("tr").mean().rolling(length2).alias("atr"),
            col("close").mean().rolling(length1).alias("ma1"),
            col("close").mean().rolling(length2).alias("ma2"),
            col("high").max().rolling(length1).alias("hh_l1"),
            col("low").min().rolling(length1).alias("ll_l1"),
        )
        .with_cols(
            ((col("high") >= col("hh_l1")) & (col("ma1") > col("ma2"))).fill_null(False).alias("buypk"),
            ((col("low") <= col("ll_l1")) & (col("ma1") < col("ma2"))).fill_null(False).alias("sellpk"),
        )
        .with_cols(
            ((col("bar_no") > length2) & col("buypk").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_long_raw"),
            ((col("bar_no") > length2) & col("sellpk").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(_pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"))
        .with_cols(_four_signals_from_pos_expr("pos_pre"))
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            *_entry_now_exprs("open_long_pre", "open_short_pre", "open", "entry_long_now", "entry_short_now", "bars_pos"),
            col("open_long_pre", "atr", col.null).if_else().alias("atr_long_now"),
            col("open_short_pre", "atr", col.null).if_else().alias("atr_short_now"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "entry_long_now").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "entry_short_now").stra.t02_track_entry_price().expanding().alias("entry_short"),
            col("open_long_pre", "exit_long_pre", "atr_long_now").stra.t02_track_entry_price().expanding().alias("atr_buyk"),
            col("open_short_pre", "exit_short_pre", "atr_short_now").stra.t02_track_entry_price().expanding().alias("atr_sellk"),
        )
        .with_cols(
            ((col("bars_pos") + 1).cast(pl.Float64)).alias("n_entry"),
            col("low").min().rolling(m_hl).alias("low_mhl"),
            col("high").max().rolling(m_hl).alias("high_mhl"),
        )
        .with_cols(
            (col("entry_long") - 2.0 * col("atr_buyk")).alias("price_sells"),
            (col("entry_short") + 2.0 * col("atr_sellk")).alias("price_buys"),
            (col("low_mhl") + col("n_entry") * 0.05 * col("atr")).alias("price_selly"),
            (col("high_mhl") - col("n_entry") * 0.05 * col("atr")).alias("price_buyy"),
        )
        .with_cols(
            (col("close") <= col("price_sells")).fill_null(False).alias("sells"),
            (col("close") >= col("price_buys")).fill_null(False).alias("buys"),
            (
                (col("close") > (col("entry_long") + col("atr_buyk")))
                & (col("close") <= col("price_selly"))
            )
            .fill_null(False)
            .alias("selly"),
            (
                (col("close") < (col("entry_short") - col("atr_sellk")))
                & (col("close") >= col("price_buyy"))
            )
            .fill_null(False)
            .alias("buyy"),
        )
        .with_cols(
            (col("sells", "selly").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_long_src"),
            (col("buys", "buyy").any(axis=1) & (col("bars_pos") > 1)).fill_null(False).alias("exit_short_src"),
        )
        .with_cols(
            col("exit_long_src").shift(1).expanding().fill_null(False).alias("exit_long_raw"),
            col("exit_short_src").shift(1).expanding().fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(_pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"))
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c47", "按 C47 源码核心实现：突破+均线方向入场，ATR 固定止损与 M_HL 动态回撤止盈离场。")

def build_c48_expr() -> Expr:
    n1 = pms("c48_n1", 1, 80, 1).limit(20)
    n2 = pms("c48_n2", 2, 160, 2).limit(20)
    mas = pms("c48_mas", 5, 600, 5).limit(20)
    trailing_stop_rate = pms("c48_trailing_stop_rate", 0.5, 100.0, 0.5).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col.lit(trailing_stop_rate).alias("trailing_stop_rate"),
            col("close").ta.ema(n1).expanding().alias("var0"),
            col("close").ta.ema(n2).expanding().alias("var2"),
            col("close").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("close_1").mean().rolling(mas).alias("ma_long"),
            col('var0', 'var2', 'close_1').shift(1).expanding().alias('var0_1', 'var2_1', 'close_2'),
        )
        .with_cols(
            ((col("var0") > col("var2")) & (col("var0_1") <= col("var2_1"))).fill_null(False).alias("cross_up"),
            ((col("var0") < col("var2")) & (col("var0_1") >= col("var2_1"))).fill_null(False).alias("cross_dn"),
        )
        .with_cols(
            col("cross_dn", "cross_up").count_last().expanding().alias("bars_from_dn", "bars_from_up"),
        )
        .with_cols(
            (col("bars_from_dn") + 1).cast(pl.UInt32).alias("win_up_u"),
            (col("bars_from_up") + 1).cast(pl.UInt32).alias("win_dn_u"),
        )
        .with_cols(
            col("high").max().rolling_dynamic("win_up_u", window_max=8192, min_samples=1).alias("var3_now"),
            col("low").min().rolling_dynamic("win_dn_u", window_max=8192, min_samples=1).alias("var4_now"),
        )
        .with_cols(
            col(col("cross_up") & (col("bars_from_dn") > 0), col("var3_now"), col.null).if_else().alias("var3_seed"),
            col(col("cross_dn") & (col("bars_from_up") > 0), col("var4_now"), col.null).if_else().alias("var4_seed"),
        )
        .with_cols(
            col("var3_seed").ffill().expanding().alias("var3"),
            col("var4_seed").ffill().expanding().alias("var4"),
        )
        .with_cols(
            col('var3', 'var4').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("var0_1") > col("var2_1"))
                & (col("close_1") > col("var3"))
                & (col("close_2") <= col("var3_1"))
                & (col("close_1") > col("ma_long"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("var2_1") > col("var0_1"))
                & (col("close_1") < col("var4"))
                & (col("close_2") >= col("var4_1"))
                & (col("close_1") < col("ma_long"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
            col("close").shift(1).expanding().alias("close_1b"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
            col("open_long_sig", "open_short_sig").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "close_1b", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("higher_after_entry"),
            col("open_short_pre", "exit_short_pre", "close_1b", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("lower_after_entry"),
        )
        .with_cols(
            (col("higher_after_entry") - col("open") * col("trailing_stop_rate") * 0.01).alias("stop_long"),
            (col("lower_after_entry") + col("open") * col("trailing_stop_rate") * 0.01).alias("stop_short"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & (col("low") < col("stop_long"))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & (col("high") > col("stop_short"))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "c48",
        "按 C48 源码实现：双 EMA 交叉后以交叉区间高低突破入场，叠加 MA240 方向过滤，并按入场后收盘极值回撤止损离场。",
    )

def build_c49_expr() -> Expr:
    x = pms("c49_x", 1.0, 20.0, 1.0).limit(20)
    m = pms("c49_m", 5, 120, 5).limit(20)
    tz = pms("c49_tz", 0.5, 20.0, 0.5).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("datetime").dt.date().alias("day"),
            col(
                col("datetime").count().alias("bars_in_day"),
                col("open").first_value().alias("open_d0"),
                col("high").max().alias("day_high"),
                col("low").min().alias("day_low"),
                col("close").last_value().alias("day_close"),
            ).expanding().over(col("datetime").dt.date()),
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("close_1").mean().rolling(240).alias("ma60"),
            col("day_high", "day_low", "day_close").shift(1).expanding().add_suffix("1"),
            col("day_high", "day_low", "day_close").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            col("day_high_1").max().rolling(m).alias("hh1"),
            col("day_low_1").min().rolling(m).alias("ll1"),
            col("day_close_1").mean().rolling(m).alias("cc1"),
            col("open_d0").shift(1).expanding().alias("nop"),
        )
        .with_cols(
            ((col("hh1") + col("ll1") + col("cc1")) / 3.0).alias("pivott"),
        )
        .with_cols(
            (2.0 * col("pivott") - col("ll1")).alias("r1"),
            (2.0 * col("pivott") - col("hh1")).alias("s1"),
            ((col("day_high_2") + col("day_low_2") + col("day_close_2")) / 3.0).alias("pivottd"),
            (col(col("day_close_1") - col("day_low_1"), col("day_high_1") - col("day_close_1")).max(axis=1)).alias("band"),
        )
        .with_cols(
            (col("pivott") + (col("r1") - col("s1"))).alias("r2"),
            (col("pivott") - (col("r1") - col("s1"))).alias("s2"),
            col(col("open_d0") * 0.008, col("band")).max(axis=1).alias("band_fix"),
            (col.lit(m).to(pl.Float64) * x * 0.1).alias("mpras"),
        )
        .with_cols(
            (col("hh1") + 2.0 * (col("pivott") - col("ll1"))).alias("r3"),
            (col("ll1") - 2.0 * (col("hh1") - col("pivott"))).alias("s3"),
        )
        .with_cols(
            ((col("pivott") + col("s1")) * 0.5).alias("sm1"),
            ((col("s1") + col("s2")) * 0.5).alias("sm2"),
            ((col("s2") + col("s3")) * 0.5).alias("sm3"),
            ((col("pivott") + col("r1")) * 0.5).alias("rm1"),
            ((col("r1") + col("r2")) * 0.5).alias("rm2"),
            ((col("r2") + col("r3")) * 0.5).alias("rm3"),
            (col("open_d0") + 0.1 * col("mpras") * col("band_fix")).alias("tmp1"),
            (col("open_d0") - 0.1 * col("mpras") * col("band_fix")).alias("tmp2"),
        )
        .with_cols(
            ((col("nop") > col("pivott")) & (col("nop") < col("rm1"))).fill_null(False).alias("cond1"),
            ((col("nop") > col("sm2")) & (col("nop") < col("sm1"))).fill_null(False).alias("cond2"),
            ((col("nop") > col("sm3")) & (col("nop") < col("sm2"))).fill_null(False).alias("cond3"),
            ((col("nop") > col("rm1")) & (col("nop") < col("rm2"))).fill_null(False).alias("cond4"),
            ((col("nop") > col("rm2")) & (col("nop") < col("rm3"))).fill_null(False).alias("cond5"),
            ((col("nop") < col("pivott")) & (col("nop") > col("sm1"))).fill_null(False).alias("cond6"),
            ((col("nop") > col("rm3")) | (col("nop") < col("sm3"))).fill_null(False).alias("cond7"),
        )
        .with_cols(
            _if_chain_expr(
                [
                    (col("cond1"), col("rm1")),
                    (col("cond2"), col("sm1")),
                    (col("cond3"), col("sm2")),
                    (col("cond4"), col("rm2")),
                    (col("cond5"), col("rm3")),
                    (col("cond6"), col("pivott")),
                    (col("cond7"), col("tmp1")),
                ],
                default_value=col.null,
                alias="sy",
            ),
            _if_chain_expr(
                [
                    (col("cond1"), col("pivott")),
                    (col("cond2"), col("sm2")),
                    (col("cond3"), col("sm3")),
                    (col("cond4"), col("rm1")),
                    (col("cond5"), col("rm2")),
                    (col("cond6"), col("sm1")),
                    (col("cond7"), col("tmp2")),
                ],
                default_value=col.null,
                alias="xy",
            ),
        )
        .with_cols(
            col("sy", "xy").shift(1).expanding().add_suffix("1"),
            col("close").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            (((col("close") > col("sy")) & (col("close_1") <= col("sy_1"))) | (col("close") > col("sy")))
            .fill_null(False)
            .alias("t1"),
            (((col("close") < col("xy")) & (col("close_1") >= col("xy_1"))) | (col("close") < col("xy")))
            .fill_null(False)
            .alias("t2"),
        )
        .with_cols(
            col("t1", "t2").shift(1).expanding().fill_null(False).add_suffix("1"),
        )
        .with_cols(
            (
                col("t1_1")
                & (~col("t2_1"))
                & (col("bars_in_day") > 1)
                & (col("close_1") > col("ma60"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                col("t2_1")
                & (~col("t1_1"))
                & (col("bars_in_day") > 1)
                & (col("close_1") < col("ma60"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
            col("close").shift(1).expanding().alias("close_1b"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
            col("open_long_sig", "open_short_sig").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "close_1b", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("higher_after_entry"),
            col("open_short_pre", "exit_short_pre", "close_1b", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("lower_after_entry"),
        )
        .with_cols(
            (col("higher_after_entry") - col("open") * tz * 0.01).alias("stop_long"),
            (col("lower_after_entry") + col("open") * tz * 0.01).alias("stop_short"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & (col("low") < col("stop_long"))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & (col("high") > col("stop_short"))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "c49",
        "按 C49 源码实现：基于开盘价所在 Pivot 分层动态映射 SY/XY 区间，触发突破入场，并按持仓后收盘极值比例回撤离场。",
    )

def build_c50_expr() -> Expr:
    myday = pms("c50_myday", 20, 600, 20).limit(20)
    myday2 = pms("c50_myday2", 20, 1200, 20).limit(20)
    stl = pms("c50_stl", 0.1, 20.0, 0.1).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col.lit(stl).alias("stl"),
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("close").mean().rolling(myday).alias("ma_myday"),
            col("close").mean().rolling(myday2).alias("ma_myday2"),
        )
        .with_cols(
            ((col("close") - col("ma_myday")) / col("ma_myday") * 100.0).alias("myrise"),
            ((col("close") - col("ma_myday2")) / col("ma_myday2") * 100.0).alias("myrise2"),
            col("high_1").max().rolling(20).alias("hecd"),
            col("low_1").min().rolling(20).alias("lecd"),
        )
        .with_cols(
            col("myrise", "myrise2").shift(1).expanding().add_suffix("1"),
            col("myrise").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            ((col("myrise") < 0.0) & (col("myrise_1") >= 0.0)).fill_null(False).alias("pcd1"),
            ((col("myrise") > 0.0) & (col("myrise_1") <= 0.0)).fill_null(False).alias("pcd2"),
            (col("myrise2_1") > 0.0).fill_null(False).alias("dcd2"),
            (col("myrise2_1") < 0.0).fill_null(False).alias("kcd2"),
            (col("close") > col("hecd")).fill_null(False).alias("hecond"),
            (col("close") < col("lecd")).fill_null(False).alias("lecond"),
        )
        .with_cols(
            col("hecond").shift(1).expanding().fill_null(False).add_suffix("1"),
            col("lecond").shift(1).expanding().fill_null(False).add_suffix("1"),
            col("pcd1").shift(1).expanding().fill_null(False).add_suffix("1"),
            col("pcd2").shift(1).expanding().fill_null(False).add_suffix("1"),
        )
        .with_cols(
            col("dcd2", "hecond_1").all(axis=1).fill_null(False).alias("open_long_raw"),
            col("kcd2", "lecond_1").all(axis=1).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
            col("close").shift(1).expanding().alias("close_1b"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
            col("open_long_sig", "open_short_sig").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            (col("close") > col("entry_long")).fill_null(False).alias("pd"),
            (col("close") < col("entry_short")).fill_null(False).alias("pk"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & (
                    col("pcd1_1", "pd").all(axis=1)
                    | (col("low") < (col("entry_long") * (1.0 - 0.01 * col("stl"))))
                )
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & (
                    col("pcd2_1", "pk").all(axis=1)
                    | (col("high") > (col("entry_short") * (1.0 + 0.01 * col("stl"))))
                )
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "c50",
        "按 C50 源码实现：双周期乖离过滤 + 20 周期突破入场，信号反转与固定百分比止损离场。",
    )

def build_c51_expr() -> Expr:
    n1 = pms("c51_n1", 0.5, 5.0, 0.5).limit(20)
    n2 = pms("c51_n2", 0.5, 5.0, 0.5).limit(20)
    length = pms("c51_length", 5, 120, 5).limit(20)
    trailing_stop = pms("c51_trailing_stop", 1.0, 50.0, 1.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
            col("high", "low", "close").ta.atr(length).expanding().alias("atr"),
        )
        .with_cols(
            col("close_1").mean().rolling(600).alias("magg"),
        )
        .with_cols(
            col("atr").shift(1).expanding().add_suffix("1"),
            col("high_1").max().rolling(length).alias("hh_len"),
            col("low_1").min().rolling(length).alias("ll_len"),
        )
        .with_cols(
            (col("close_1") + col("atr_1") * n1).alias("buycon"),
            (col("close_1") - col("atr_1") * n2).alias("sellcon"),
        )
        .with_cols(
            (col("high") > col("hh_len")).fill_null(False).alias("buyct1"),
            (col("low") < col("ll_len")).fill_null(False).alias("sellct1"),
            (col("high") >= col("buycon")).fill_null(False).alias("buyct2"),
            (col("low") <= col("sellcon")).fill_null(False).alias("sellct2"),
        )
        .with_cols(
            col("buyct1", "sellct1").shift(1).expanding().fill_null(False).add_suffix("1"),
        )
        .with_cols(
            (col("buyct1_1") & col("buyct2") & (col("close_1") > col("magg"))).fill_null(False).alias("open_long_raw"),
            (col("sellct1_1") & col("sellct2") & (col("close_1") < col("magg"))).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
            col("close").shift(1).expanding().alias("close_1b"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
            col("open_long_sig", "open_short_sig").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & (col("close_1b") <= (col("entry_long") * (1.0 - trailing_stop * 0.001)))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & (col("close_1b") >= (col("entry_short") * (1.0 + trailing_stop * 0.001)))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c51", "按 C51 源码实现：ATR 阈值突破与 MA600 方向过滤开仓，按入场价千分比止损离场。")

def build_c52_expr() -> Expr:
    alen = pms("c52_alen", 20, 600, 20).limit(20)
    disp = pms("c52_disp", 1, 60, 1).limit(20)
    exbar = pms("c52_exbar", 5, 300, 5).limit(20)
    minpoint = pms("c52_minpoint", 0.1, 20.0, 0.1).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("datetime").dt.date().alias("day"),
            col.lit(exbar).to(pl.UInt32).alias("exbar_u"),
            col("open").first_value().expanding().over(col("datetime").dt.date()).alias("open_d0"),
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("close_1").mean().rolling(600).alias("ma"),
            (col("high") - col("close") + col("open_d0")).alias("up_src"),
            (col("low") - col("close") + col("open_d0")).alias("dn_src"),
            ((col("high") + col("low")) * 0.5).alias("median"),
            (col("high") - col("low")).alias("range"),
        )
        .with_cols(
            col("up_src").max().rolling(alen).alias("upavg"),
            col("dn_src").min().rolling(alen).alias("lowavg"),
            col("median").shift(disp).expanding().mean().rolling(alen).alias("exavg"),
            col("range").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col('upavg', 'lowavg').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("median") > col("high_1")) & (col("range") > col("range_1"))).fill_null(False).alias("rlb"),
            ((col("median") < col("low_1")) & (col("range") > col("range_1"))).fill_null(False).alias("rls"),
        )
        .with_cols(
            col("rlb", "rls").shift(1).expanding().fill_null(False).add_suffix("1"),
        )
        .with_cols(
            ((col("close_1") > col("ma")) & col("rlb_1") & (col("close_1") > col("upavg_1"))).fill_null(False).alias("open_long_raw"),
            ((col("close_1") < col("ma")) & col("rls_1") & (col("close_1") < col("lowavg_1"))).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            _bars_pos_from_open_sig_expr("open_long_sig", "open_short_sig", "bars_pos"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (
                    ((col("bars_pos") <= col("exbar_u")) & (col("low") <= col("exavg")))
                    | ((col("bars_pos") > col("exbar_u")) & (col("low") <= (col("lowavg") - minpoint)))
                )
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (
                    ((col("bars_pos") <= col("exbar_u")) & (col("high") >= col("exavg")))
                    | ((col("bars_pos") > col("exbar_u")) & (col("high") >= (col("upavg") + minpoint)))
                )
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c52", "按 C52 源码实现：OpenD 偏移通道与中点放量过滤入场，持仓按 ExBar 分段使用 ExAvg/LowAvg/UpAvg 离场。")

def build_c53_expr() -> Expr:
    c_l = pms("c53_cl", 10, 300, 10).limit(20)
    c_d = pms("c53_cd", 1, 200, 1).limit(20)
    stl = pms("c53_stl", 0.2, 20.0, 0.2).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
            col.lit(stl).alias("stl"),
        )
        .with_cols(
            col("close_1").mean().rolling(240).alias("mas"),
            col("high").max().rolling(c_l).alias("upper_c"),
            col("low").min().rolling(c_l).alias("lower_c"),
        )
        .with_cols(
            col("upper_c").shift(c_d + 1).expanding().alias("upper_ref"),
            col("lower_c").shift(c_d + 1).expanding().alias("lower_ref"),
        )
        .with_cols(
            (
                (col("close_1") > col("mas"))
                & (col("high_1") < col("upper_ref"))
                & (col("high") >= col("upper_ref"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("close_1") < col("mas"))
                & (col("low_1") > col("lower_ref"))
                & (col("low") <= col("lower_ref"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
            col("open_long_sig", "open_short_sig").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & (col("low") < (col("entry_long") * (1.0 - col("stl") * 0.01)))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & (col("high") > (col("entry_short") * (1.0 + col("stl") * 0.01)))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c53", "按 C53 源码实现：延迟通道突破配合 MA240 方向过滤开仓，并按入场价固定百分比止损离场。")

def build_c54_expr() -> Expr:
    ol = pms("c54_ol", 5, 400, 5).limit(20)
    cl = pms("c54_cl", 5, 400, 5).limit(20)
    atrs = pms("c54_atrs", 1, 30, 1).limit(20)
    trailing_stop_rate = pms("c54_trailing_stop_rate", 0.5, 60.0, 0.5).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("datetime").dt.date().alias("day"),
            col("open").first_value().expanding().over(col("datetime").dt.date()).alias("open_d0"),
            col.lit(trailing_stop_rate).alias("trailing_stop_rate"),
            col("close").ta.ema(cl).expanding().alias("ema_c"),
            col("open").ta.ema(ol).expanding().alias("ema_o"),
            col("high", "low", "close").ta.trange().expanding().alias("tr"),
        )
        .with_cols(
            (col("ema_c") - col("ema_o")).alias("histogram"),
            col("tr").shift(1).expanding().mean().rolling(atrs).alias("atrmax"),
        )
        .with_cols(
            col("histogram").shift(1).expanding().alias("hist_1"),
            col("histogram").shift(2).expanding().alias("hist_2"),
        )
        .with_cols(
            ((col("hist_1") > 0.0) & (col("hist_2") <= 0.0)).fill_null(False).alias("con1"),
            ((col("hist_1") < 0.0) & (col("hist_2") >= 0.0)).fill_null(False).alias("con2"),
        )
        .with_cols(
            col("con1", col("open_d0") + col("atrmax") * 0.5, col.null).if_else().alias("buy_price_seed"),
            col("con2", col("open_d0") - col("atrmax") * 0.5, col.null).if_else().alias("sell_price_seed"),
        )
        .with_cols(
            col("buy_price_seed").ffill().expanding().alias("buy_price"),
            col("sell_price_seed").ffill().expanding().alias("sell_price"),
            col("close").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("histogram").shift(2).expanding() > 0.0) & (col("close_1") >= col("buy_price"))).fill_null(False).alias("open_long_raw"),
            ((col("histogram").shift(2).expanding() < 0.0) & (col("close_1") <= col("sell_price"))).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
            col('high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
            col("open_long_sig", "open_short_sig").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "high_1", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("higher_after_entry"),
            col("open_short_pre", "exit_short_pre", "low_1", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("lower_after_entry"),
        )
        .with_cols(
            (col("higher_after_entry") - col("open") * col("trailing_stop_rate") * 0.01).alias("stop_long"),
            (col("lower_after_entry") + col("open") * col("trailing_stop_rate") * 0.01).alias("stop_short"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & (col("low") < col("stop_long"))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & (col("high") > col("stop_short"))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c54", "按 C54 源码实现：开收 EMA 差值穿零触发方向，OpenD±0.5*ATR 触发入场，持仓后按高/低有利极值回撤离场。")

def build_c55_expr() -> Expr:
    length = pms("c55_length", 20, 500, 20).limit(20)
    st = pms("c55_st", 0.2, 5.0, 0.2).limit(20)
    tr = pms("c55_tr", 1.0, 300.0, 1.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col.lit(length).to(pl.UInt32).alias("length_u"),
            col.lit(st).alias("st"),
            col.lit(tr).alias("tr"),
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("close_1").ta.ema(360).expanding().alias("ma240"),
            ((col("high") + col("low") + col("close")) / 3.0).alias("price"),
            col("high", "low", "close").ta.trange().expanding().alias("trange"),
        )
        .with_cols(
            col("price").mean().rolling(length).alias("avg_val"),
            col("trange").mean().rolling(length).alias("avg_range"),
            col("price").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("avg_val") + col("avg_range") * col("st")).alias("ku"),
            (col("avg_val") - col("avg_range") * col("st")).alias("kl"),
        )
        .with_cols(
            ((col("ku") - col("kl")) * 0.5).alias("cr"),
            col('ku', 'kl').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("price") > col("ku")) & (col("price_1") <= col("ku_1"))).fill_null(False).alias("buycon"),
            ((col("price") < col("kl")) & (col("price_1") >= col("kl_1"))).fill_null(False).alias("sellcon"),
        )
        .with_cols(
            col(col("buycon").shift(1).expanding().fill_null(False), col("high_1") + col("cr") * 0.85, col.null).if_else().alias("hh_seed"),
            col(col("sellcon").shift(1).expanding().fill_null(False), col("low_1") - col("cr") * 0.85, col.null).if_else().alias("ll_seed"),
        )
        .with_cols(
            col("hh_seed").ffill().expanding().alias("hh"),
            col("ll_seed").ffill().expanding().alias("ll"),
        )
        .with_cols(
            ((col("price_1") > col("ku_1")) & (col("high") >= col("hh")) & (col("close_1") > col("ma240"))).fill_null(False).alias("open_long_raw"),
            ((col("price_1") < col("kl_1")) & (col("low") <= col("ll")) & (col("close_1") < col("ma240"))).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
            col("open_long_sig", "open_short_sig").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "low_1", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("lower_after_entry"),
            col("open_short_pre", "exit_short_pre", "high_1", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("higher_after_entry"),
        )
        .with_cols(
            (col("lower_after_entry") - col("open") * col("tr") * 0.001).alias("stop_long"),
            (col("higher_after_entry") + col("open") * col("tr") * 0.001).alias("stop_short"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & (col("low") <= col("stop_long"))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & (col("high") >= col("stop_short"))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c55", "按 C55 源码实现：均价波动通道二次确认入场并叠加 MA360 过滤，持仓按有利低/高点偏移线离场。")

def build_c56_expr() -> Expr:
    boll_len = pms("c56_bollinger_lengths", 60, 1200, 60).limit(20)
    xs = pms("c56_xs", 0.1, 1.0, 0.1).limit(20)
    len_xs = (boll_len * xs).floor()
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("close").mean().rolling(boll_len).alias("ma240"),
            col("close").mean().rolling(len_xs).alias("ma1"),
            col("high_1").max().rolling(boll_len).alias("upband"),
            col("low_1").min().rolling(boll_len).alias("dnband"),
        )
        .with_cols(
            (col("close") - col("close").shift(boll_len).expanding()).alias("roccalc"),
            col('upband', 'dnband', 'ma240', 'ma1').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("close") > col("upband")) & (col("close_1") <= col("upband_1"))).fill_null(False).alias("opdband"),
            ((col("close") < col("dnband")) & (col("close_1") >= col("dnband_1"))).fill_null(False).alias("opkband"),
        )
        .with_cols(
            (
                (col("roccalc").shift(1).expanding() > 0.0)
                & col("opdband").shift(1).expanding().fill_null(False)
                & (col("close_1") > col("ma240_1"))
                & (col("ma1_1") > col("ma240_1"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("roccalc").shift(1).expanding() < 0.0)
                & col("opkband").shift(1).expanding().fill_null(False)
                & (col("close_1") < col("ma240_1"))
                & (col("ma1_1") < col("ma240_1"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            col("open_long_sig", "open_short_sig").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "open").stra.t02_track_entry_price().expanding().alias("dkp"),
            col("open_short_pre", "exit_short_pre", "open").stra.t02_track_entry_price().expanding().alias("kkp"),
            col(
                col("pos_pre") == 0.0,
                len_xs,
                len_xs + 1.0,
            ).if_else()
            .cast(pl.UInt32)
            .alias("liq_days_u"),
        )
        .with_cols(
            col("low_1").min().rolling_dynamic("liq_days_u", window_max=16384, min_samples=1).alias("dliq"),
            col("high_1").max().rolling_dynamic("liq_days_u", window_max=16384, min_samples=1).alias("kliq"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & (col("low") <= col("dliq").shift(1).expanding())
                & (col("low") >= col("dkp"))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & (col("high") >= col("kliq").shift(1).expanding())
                & (col("high") <= col("kkp"))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c56", "按 C56 源码实现：长周期突破+趋势方向过滤入场，持仓按动态 liqDays 通道并受入场价约束离场。")

def build_c57_expr() -> Expr:
    setup_len = pms("c57_setup_len", 10, 300, 10).limit(20)
    atr_pcnt = pms("c57_atr_pcnt", 0.5, 10.0, 0.5).limit(20)
    tr = pms("c57_tr", 1.0, 300.0, 1.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("datetime").dt.date().alias("day"),
            col("datetime").count().expanding().alias("bar_no"),
            col("datetime").count().expanding().over(col("datetime").dt.date()).alias("bars_in_day"),
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("close_1").mean().rolling(360).alias("ma360"),
            (col("close") - col("close").shift(setup_len).expanding()).alias("momvalue"),
            col(col("bars_in_day").cast(pl.UInt32), setup_len).max(axis=1).alias("avgleng_u"),
            col((col("high") - col("low")).abs(), (col("high") - col("close_1")).abs(), (col("low") - col("close_1")).abs()).max(axis=1).alias("true_range"),
        )
        .with_cols(
            (col("volume") * col("momvalue")).alias("vm_raw"),
        )
        .with_cols(
            col("vm_raw").mean().rolling_dynamic("avgleng_u", window_max=16384, min_samples=1).alias("vwm"),
            col("true_range").mean().rolling_dynamic("avgleng_u", window_max=16384, min_samples=1).alias("aatr"),
        )
        .with_cols(
            col("vwm").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("vwm") > 0.0) & (col("vwm_1") <= 0.0)).fill_null(False).alias("bullsetup"),
            ((col("vwm") < 0.0) & (col("vwm_1") >= 0.0)).fill_null(False).alias("bearsetup"),
        )
        .with_cols(
            col("bullsetup", "bearsetup").shift(1).count_last().expanding().alias("lsetup", "ssetup"),
            col("bullsetup", "high", col.null).if_else().alias("leprice_seed"),
            col("bearsetup", "low", col.null).if_else().alias("seprice_seed"),
        )
        .with_cols(
            col("leprice_seed").ffill().expanding().alias("leprice"),
            col("seprice_seed").ffill().expanding().alias("seprice"),
        )
        .with_cols(
            col('leprice', 'seprice').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("bar_no") > col("avgleng_u").cast(pl.UInt64))
                & (col("leprice_1") > 0.0)
                & (col("close_1") >= (col("leprice_1") + atr_pcnt * col("aatr").shift(1).expanding()))
                & (col("lsetup").shift(1).expanding() <= setup_len)
                & (col("lsetup") >= 1)
                & (col("close_1") > col("ma360"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("bar_no") > col("avgleng_u").cast(pl.UInt64))
                & (col("seprice_1") > 0.0)
                & (col("close_1") <= (col("seprice_1") - atr_pcnt * col("aatr").shift(1).expanding()))
                & (col("ssetup").shift(1).expanding() <= setup_len)
                & (col("ssetup") >= 1)
                & (col("close_1") < col("ma360"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
            col("open_long_sig", "open_short_sig").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "low_1", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("lower_after_entry"),
            col("open_short_pre", "exit_short_pre", "high_1", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("higher_after_entry"),
        )
        .with_cols(
            (col("lower_after_entry") - col("open") * tr * 0.001).alias("stop_long"),
            (col("higher_after_entry") + col("open") * tr * 0.001).alias("stop_short"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & (col("low") <= col("stop_long"))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & (col("high") >= col("stop_short"))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c57", "按 C57 源码实现：VWM 零轴切换设定触发价并以 ATR 突破入场，持仓按有利低/高点偏移线离场。")

def build_c58_expr() -> Expr:
    k1 = pms("c58_k1", 1.0, 20.0, 1.0).limit(20)
    k2 = pms("c58_k2", 1.0, 20.0, 1.0).limit(20)
    m = pms("c58_m", 20, 600, 20).limit(20)
    m_half = (m / 2).floor()
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("datetime").dt.date().alias("day"),
            col.lit(k1).alias("k1"),
            col.lit(k2).alias("k2"),
            col("open").first_value().expanding().over(col("datetime").dt.date()).alias("open_d0"),
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("high").max().rolling(m).alias("hh"),
            col("close").max().rolling(m).alias("hc"),
            col("low").min().rolling(m).alias("ll"),
            col("close").min().rolling(m).alias("lc"),
            col("close_1").ta.ema(m).expanding().alias("long_ma"),
        )
        .with_cols(
            col(col("hh") - col("hc"), col("ll") - col("lc")).max(axis=1).alias("range"),
        )
        .with_cols(
            (col("open_d0") + col("k1") * col("range").shift(1).expanding()).alias("aupband"),
            (col("open_d0") - col("k2") * col("range").shift(1).expanding()).alias("adnband"),
            (col("open_d0") + col("k2") * col("range").shift(1).expanding()).alias("bupband"),
            (col("open_d0") - col("k1") * col("range").shift(1).expanding()).alias("bdnband"),
            col("high_1").max().rolling(m_half).alias("exit_hi_band"),
            col("low_1").min().rolling(m_half).alias("exit_lo_band"),
        )
        .with_cols(
            col(col("close_1") >= col("long_ma"), col("aupband"), col("bupband")).if_else().alias("upband"),
            col(col("close_1") >= col("long_ma"), col("adnband"), col("bdnband")).if_else().alias("dnband"),
        )
        .with_cols(
            ((col("high") >= col("upband").shift(1).expanding()) & (col("upband").shift(1).expanding() > 0.0)).fill_null(False).alias("open_long_raw"),
            ((col("low") <= col("dnband").shift(1).expanding()) & (col("dnband").shift(1).expanding() > 0.0)).fill_null(False).alias("open_short_raw"),
            (col("low") <= col("exit_lo_band").shift(1).expanding()).fill_null(False).alias("exit_long_band"),
            (col("high") >= col("exit_hi_band").shift(1).expanding()).fill_null(False).alias("exit_short_band"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_band", "open_short_raw", "exit_short_band", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c58", "按 C58 源码实现：以 OpenD 与区间 Range 生成双区入场带，按趋势区间切换后突破入场，并用 M/2 通道反向突破离场。")

def build_c59_expr() -> Expr:
    m = pms("c59_m", 20, 600, 250).limit(20)
    s = pms("c59_s", 1, 10, 2).limit(20)
    tr = pms("c59_tr", 1.0, 20.0, 5.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col.lit(tr).alias("tr"),
            (((col("high") + col("low") + col("close")) / 3.0)).alias("hlc3"),
            col('high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("hlc3").ta.ema(m).expanding().alias("majj"),
            col("hlc3").ta.ema(m).expanding().alias("mamin"),
        )
        .with_cols(
            col("majj").ta.ema(s).expanding().alias("mama"),
            col("mamin").ta.ema(m).expanding().alias("mamamin"),
        )
        .with_cols(
            (col("mama") - col("mamamin")).alias("dlh"),
        )
        .with_cols(
            col("dlh").ta.ema(s).expanding().alias("madlh"),
        )
        .with_cols(
            (
                (col("dlh") > col("madlh"))
                & (col("dlh") > 0.0)
                & (col("majj") > col("mama"))
                & (col("mamin") > col("mamamin"))
                & (col("mama") > col("mamamin"))
            )
            .fill_null(False)
            .alias("dk"),
            (
                (col("dlh") < col("madlh"))
                & (col("dlh") < 0.0)
                & (col("majj") < col("mama"))
                & (col("mamin") < col("mamamin"))
                & (col("mama") < col("mamamin"))
            )
            .fill_null(False)
            .alias("kk"),
            (col("majj") < col("mama")).fill_null(False).alias("pd"),
            (col("majj") > col("mama")).fill_null(False).alias("pk"),
        )
        .with_cols(
            col("dk").shift(1).expanding().fill_null(False).alias("open_long_raw"),
            col("kk").shift(1).expanding().fill_null(False).alias("open_short_raw"),
            col("pd").shift(1).expanding().fill_null(False).alias("exit_long_flip"),
            col("pk").shift(1).expanding().fill_null(False).alias("exit_short_flip"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
            col("open_long_sig", "open_short_sig").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "low_1", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("lower_after_entry"),
            col("open_short_pre", "exit_short_pre", "high_1", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("higher_after_entry"),
        )
        .with_cols(
            (col("lower_after_entry") - col("open") * col("tr") * 0.01).alias("stop_long"),
            (col("higher_after_entry") + col("open") * col("tr") * 0.01).alias("stop_short"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & (col("low") <= col("stop_long"))
            )
            .fill_null(False)
            .alias("exit_long_stop"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & (col("high") >= col("stop_short"))
            )
            .fill_null(False)
            .alias("exit_short_stop"),
        )
        .with_cols(
            col(
                (col("exit_long_flip") & (col("pos_pre") == 1.0) & (col("bars_long") > 0)).fill_null(False),
                col("exit_long_stop"),
            ).any(axis=1).fill_null(False).alias("exit_long_raw"),
            col(
                (col("exit_short_flip") & (col("pos_pre") == -1.0) & (col("bars_short") > 0)).fill_null(False),
                col("exit_short_stop"),
            ).any(axis=1).fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c59", "按 C59 源码实现：双层平滑趋势共振入场，趋势反向与有利极值回撤共同触发离场。")

def build_c60_expr() -> Expr:
    length = pms("c60_length", 20, 1200, 20).limit(20)
    smooth_len = pms("c60_smooth_length", 5, 300, 5).limit(20)
    matr = pms("c60_matr", 1.0, 50.0, 1.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col.lit(length).to(pl.UInt32).alias("length_u"),
            col.lit(matr).alias("matr"),
            col("high", "low", "close").ta.trange().expanding().alias("tr"),
            col('high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("tr").mean().rolling(length).alias("atr"),
            col("close").ta.ema(length).expanding().alias("ema1"),
        )
        .with_cols(
            col("ema1").ta.ema(length).expanding().alias("ema2"),
        )
        .with_cols(
            col("ema2").ta.ema(length).expanding().alias("tmp_value"),
        )
        .with_cols(
            col("tmp_value").shift(1).expanding().alias("tmp_1"),
        )
        .with_cols(
            ((col("tmp_value") - col("tmp_1")) / col("tmp_1") * 100.0).alias("trix"),
        )
        .with_cols(
            col("trix").mean().rolling(smooth_len).alias("avg_trix"),
            col("trix").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("avg_trix").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("trix") > col("avg_trix")) & (col("trix_1") <= col("avg_trix_1"))).fill_null(False).alias("cond1"),
            ((col("trix") < col("avg_trix")) & (col("trix_1") >= col("avg_trix_1"))).fill_null(False).alias("cond2"),
        )
        .with_cols(
            col("cond1", "cond2").shift(1).expanding().fill_null(False).alias("open_long_raw", "open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
            col("open_long_sig", "open_short_sig").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
            col("open_long_pre", col("open") - col("matr") * col("atr"), col.null).if_else().alias("myent_buy_now"),
            col("open_short_pre", col("open") + col("matr") * col("atr"), col.null).if_else().alias("myent_sell_now"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "myent_buy_now").stra.t02_track_entry_price().expanding().alias("myent_buy"),
            col("open_short_pre", "exit_short_pre", "myent_sell_now").stra.t02_track_entry_price().expanding().alias("myent_sell"),
            col(
                col("pos_pre") == 0.0,
                col("length_u"),
                col((col("length_u").cast(pl.Int64) - 1).clip(lower_bound=1), smooth_len).max(axis=1),
            ).if_else()
            .cast(pl.UInt32)
            .alias("liq_days_u"),
        )
        .with_cols(
            (col("close") < col("myent_buy")).fill_null(False).alias("sells"),
            (col("close") > col("myent_sell")).fill_null(False).alias("buys"),
            col("low_1").min().rolling_dynamic("liq_days_u", window_max=16384, min_samples=1).alias("dliq"),
            col("high_1").max().rolling_dynamic("liq_days_u", window_max=16384, min_samples=1).alias("kliq"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 1)
                & (
                    col("sells").shift(1).expanding().fill_null(False)
                    | ((col("low") <= col("dliq").shift(1).expanding()) & (col("low") >= col("entry_long")))
                )
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 1)
                & (
                    col("buys").shift(1).expanding().fill_null(False)
                    | ((col("high") >= col("kliq").shift(1).expanding()) & (col("high") <= col("entry_short")))
                )
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c60", "按 C60 源码实现：TRIX 与平滑线交叉入场，入场 ATR 常数止损与动态 liqDays 通道共同离场。")

def build_c61_expr() -> Expr:
    m = pms("c61_m", 20, 400, 200).limit(20)
    s1 = pms("c61_s1", 0.2, 5.0, 1.0).limit(20)
    s2 = pms("c61_s2", 1.0, 15.0, 7.5).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("datetime").dt.date().alias("day"),
            col.lit(m).to(pl.UInt32).alias("m_u"),
            col.lit(s1).alias("s1"),
            col.lit(s2).alias("s2"),
            col("open").first_value().expanding().over(col("datetime").dt.date()).alias("open_d0"),
            col("high", "low", "close").ta.atr(m).expanding().alias("atr"),
            col("close").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("day").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("day") == col("day_1")).fill_null(False).alias("bartoday"),
            (col("open_d0") + col("s1") * col("atr").shift(1).expanding()).alias("dk_src"),
            (col("open_d0") - col("s1") * col("atr").shift(1).expanding()).alias("dd_src"),
            (col("open_d0") + col("s2") * col("atr").shift(1).expanding()).alias("kk_src"),
            (col("open_d0") - col("s2") * col("atr").shift(1).expanding()).alias("kd_src"),
        )
        .with_cols(
            col("dk_src").max().rolling(m).alias("dk"),
            col("dd_src").min().rolling(m).alias("dd"),
            col("kk_src").max().rolling(m).alias("kk"),
            col("kd_src").min().rolling(m).alias("kd"),
        )
        .with_cols(
            ((col("high") >= col("dk").shift(1).expanding()) & col("bartoday")).fill_null(False).alias("open_long_raw"),
            ((col("low") <= col("dd").shift(1).expanding()) & col("bartoday")).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
            col("open_long_sig", "open_short_sig").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & col("bartoday")
                & (col("close_1") > col("entry_long"))
                & (col("low") <= col("kd").shift(1).expanding())
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & col("bartoday")
                & (col("close_1") < col("entry_short"))
                & (col("high") >= col("kk").shift(1).expanding())
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c61", "按 C61 源码实现：基于日开盘±ATR双通道突破入场，并按更宽通道与盈亏方向约束平仓。")

def build_c62_expr() -> Expr:
    range_len = pms("c62_range_len", 20, 400, 20).limit(20)
    atrs = pms("c62_atrs", 10, 300, 10).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col.lit(atrs).alias("atrs"),
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
            ((col("high") + col("low")) * 0.5).alias("mid"),
            col("high", "low", "close").ta.trange().expanding().alias("tr"),
            col("high").count().expanding().alias("bar_no"),
        )
        .with_cols(
            col("close_1").mean().rolling(atrs).alias("ma1"),
            col("close_1").mean().rolling(range_len).alias("ma2"),
            col("high_1").max().rolling(range_len).alias("range_h"),
            col("low_1").min().rolling(range_len).alias("range_l"),
            col("tr").mean().rolling(range_len).alias("atrma"),
        )
        .with_cols(
            col("atrma").shift(range_len).expanding().alias("atrma_ref"),
            (col("close") > col("range_h")).fill_null(False).alias("cond2"),
            (col("close") < col("range_l")).fill_null(False).alias("cond3"),
            (col("mid") > col("high_1")).fill_null(False).alias("mid_up"),
            (col("mid") < col("low_1")).fill_null(False).alias("mid_dn"),
        )
        .with_cols(
            (col("tr") > col("atrma_ref")).fill_null(False).alias("cond1"),
            (col("cond2", "mid_up").all(axis=1).fill_null(False)).alias("cond2_ok"),
            (col("cond3", "mid_dn").all(axis=1).fill_null(False)).alias("cond3_ok"),
        )
        .with_cols(
            (
                col("cond1").shift(1).expanding().fill_null(False)
                & col("cond2_ok").shift(1).expanding().fill_null(False)
                & (col("ma1") > col("ma2"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                col("cond1").shift(1).expanding().fill_null(False)
                & col("cond3_ok").shift(1).expanding().fill_null(False)
                & (col("ma1") < col("ma2"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            col("open_long_sig", "open_short_sig").count_last().expanding().alias("bars_long", "bars_short"),
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_tmp"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "range_l").stra.t02_track_entry_price().expanding().alias("long_risk"),
            col("open_short_pre", "exit_short_pre", "range_h").stra.t02_track_entry_price().expanding().alias("short_risk"),
            col("open_long_pre", "exit_long_pre", "high_1", "open")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("long_high"),
            col("open_short_pre", "exit_short_pre", "low_1", "open")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("short_low"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & (
                    col("cond3_ok").shift(1).expanding().fill_null(False)
                    | (col("low") <= col("long_risk"))
                    | (col("low") <= (col("long_high").shift(1).expanding() - col("atrs") * col("atrma").shift(1).expanding()))
                )
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & (
                    col("cond2_ok").shift(1).expanding().fill_null(False)
                    | (col("high") >= col("short_risk"))
                    | (col("high") >= (col("short_low").shift(1).expanding() + col("atrs") * col("atrma").shift(1).expanding()))
                )
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c62", "按 C62 源码实现：区间突破+ATR 波动过滤+均线方向入场，并以反向信号、初始风险位和 ATR 回撤线离场。")

def build_c63_expr() -> Expr:
    n = pms("c63_n", 2, 60, 10).limit(20)
    k1 = pms("c63_k1", 0.1, 5.0, 1.0).limit(20)
    trs = pms("c63_trs", 1.0, 30.0, 8.0).limit(20)
    z = pms("c63_z", 0.5, 10.0, 3.0).limit(20)
    exit_on_close = pms("c63_exit_on_close_mins", 900, 1500, 1450).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            (col("datetime").dt.hour() * 100 + col("datetime").dt.minute()).alias("hhmm"),
            col("datetime").dt.date().alias("day"),
            col(
                col("datetime").count().alias("bars_in_day"),
                col("open").first_value().alias("open_d0"),
            ).expanding().over(col("datetime").dt.date()),
            col('high', 'low', 'close').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col(col("bars_in_day") <= n, col("high"), col.null).if_else().alias("high_seed"),
            col(col("bars_in_day") <= n, col("close"), col.null).if_else().alias("close_h_seed"),
            col(col("bars_in_day") <= n, col("low"), col.null).if_else().alias("low_seed"),
            col(col("bars_in_day") <= n, col("close"), col.null).if_else().alias("close_l_seed"),
        )
        .with_cols(col(
            col("high_seed").max().alias("hh"),
            col("close_h_seed").max().alias("hc"),
            col("low_seed").min().alias("ll"),
            col("close_l_seed").min().alias("lc"),
        ).expanding().over("day"))
        .with_cols(
            col((col("hh") - col("lc")) >= (col("hc") - col("ll")), col("hh") - col("lc"), col("hc") - col("ll")).if_else()
            .alias("range_v"),
        )
        .with_cols(
            (k1 * 0.001 * col("range_v")).alias("trig"),
            (col("open_d0", "hh").max(axis=1) + k1 * 0.001 * col("range_v")).alias("buyposition"),
            (col("open_d0", "ll").min(axis=1) - k1 * 0.001 * col("range_v")).alias("sellposition"),
        )
        .with_cols(
            (
                (col("bars_in_day") > n)
                & (col("hhmm") < exit_on_close)
                & (col("high") >= col("buyposition"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("bars_in_day") > n)
                & (col("hhmm") < exit_on_close)
                & (col("low") <= col("sellposition"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            col("open_long_sig", "open_short_sig").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "low_1", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("lower_after_entry"),
            col("open_short_pre", "exit_short_pre", "high_1", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("higher_after_entry"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & (
                    (col("close_1") < (col("entry_long") * (1.0 - 0.001 * z)))
                    | ((col("lower_after_entry") - col("open") * trs * 0.001) >= col("entry_long"))
                    & (col("low") <= (col("lower_after_entry") - col("open") * trs * 0.001))
                )
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & (
                    (col("close_1") > (col("entry_short") * (1.0 + 0.001 * z)))
                    | ((col("higher_after_entry") + col("open") * trs * 0.001) <= col("entry_short"))
                    & (col("high") >= (col("higher_after_entry") + col("open") * trs * 0.001))
                )
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            ((col("hhmm") >= exit_on_close) & (col("pos_pre") == 1.0)).fill_null(False).alias("force_exit_long"),
            ((col("hhmm") >= exit_on_close) & (col("pos_pre") == -1.0)).fill_null(False).alias("force_exit_short"),
        )
        .with_cols(
            col("exit_long_raw", "force_exit_long").any(axis=1).fill_null(False).alias("exit_long_all"),
            col("exit_short_raw", "force_exit_short").any(axis=1).fill_null(False).alias("exit_short_all"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_all", "open_short_raw", "exit_short_all", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c63", "按 C63 源码实现：日内前 N 根区间构造突破位入场，叠加固定止损与动态回撤，并在收盘前强平。")

def build_c64_expr() -> Expr:
    trs = pms("c64_trs", 0.5, 20.0, 0.5).limit(20)
    eff_len = pms("c64_eff_ratio_length", 2, 30, 2).limit(20)
    fast_len = pms("c64_fast_avg_length", 2, 20, 2).limit(20)
    slow_len = pms("c64_slow_avg_length", 5, 80, 5).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col.lit(trs).alias("trs"),
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
            col("close").count().expanding().alias("bar_no"),
        )
        .with_cols(
            (col("close") - col("close").shift(eff_len).expanding()).alias("direction"),
            (col("close") - col("close_1")).abs().alias("abs_move"),
        )
        .with_cols(
            col("abs_move").sum().rolling(eff_len).alias("volatility"),
        )
        .with_cols(
            col(col("volatility").abs() <= 1e-12, 0.0, col("direction") / col("volatility")).if_else().alias("effratio"),
            (2.0 / (fast_len + 1.0)).alias("fastest"),
            (2.0 / (col.lit(slow_len).to(pl.UInt32).cast(pl.Float64) + 1.0)).alias("slowest"),
        )
        .with_cols(
            ((col("effratio") * (col("fastest") - col("slowest")) + col("slowest")) ** 2).alias("scaled"),
            col("close").ta.ema(eff_len).expanding().alias("ama_base"),
        )
        .with_cols(
            (col("ama_base") + col("scaled") * 0.01 * (col("close") - col("ama_base"))).alias("ama"),
        )
        .with_cols(
            col("ama").mean().rolling(eff_len).alias("ama2"),
        )
        .with_cols(
            col("ama2").mean().rolling(eff_len).alias("ama3"),
            col("ama2").shift(1).expanding().add_suffix("1"),
            col("high_1").max().rolling(slow_len).alias("bkgo2"),
            col("low_1").min().rolling(slow_len).alias("skgo2"),
        )
        .with_cols(
            col("ama3").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("ama2") > col("ama3")) & (col("ama2_1") <= col("ama3_1"))).fill_null(False).alias("bkgo"),
            ((col("ama2") < col("ama3")) & (col("ama2_1") >= col("ama3_1"))).fill_null(False).alias("skgo"),
        )
        .with_cols(
            col(col("bkgo").shift(1).expanding().fill_null(False), col("bkgo2"), col.null).if_else().alias("bkcond_seed"),
            col(col("skgo").shift(1).expanding().fill_null(False), col("skgo2"), col.null).if_else().alias("skcond_seed"),
            col("bkgo", "skgo").count_last().expanding().alias("bars_since_bk", "bars_since_sk"),
        )
        .with_cols(
            col("bkcond_seed").ffill().expanding().alias("bkcond"),
            col("skcond_seed").ffill().expanding().alias("skcond"),
            col(col("bars_since_bk") < col("bars_since_sk"), 1.0, -1.0).if_else().alias("x_state"),
        )
        .with_cols(
            (
                (col("x_state") == 1.0)
                & (col("bkcond") > 0.0)
                & (col("high") >= col("bkcond"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("x_state") == -1.0)
                & (col("skcond") > 0.0)
                & (col("low") <= col("skcond"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
            col("open_long_sig", "open_short_sig").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "low_1", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("lower_after_entry"),
            col("open_short_pre", "exit_short_pre", "high_1", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("higher_after_entry"),
        )
        .with_cols(
            (col("lower_after_entry") - col("open") * col("trs") * 0.001).alias("stop_long"),
            (col("higher_after_entry") + col("open") * col("trs") * 0.001).alias("stop_short"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & (col("low") <= col("stop_long"))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & (col("high") >= col("stop_short"))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c64", "按 C64 源码实现：自适应均线双重平滑交叉确定触发价，突破入场并以有利极值回撤线离场。")

def build_c65_expr() -> Expr:
    turn_len = pms("c65_turn_into_length", 10, 400, 100).limit(20)
    pct = pms("c65_percent_of_range", 0.05, 1.5, 0.4).limit(20)
    trs = pms("c65_trs", 0.2, 10.0, 1.2).limit(20)
    enter_hhmm = pms("c65_enter_hhmm", 900, 1500, 940).limit(20)
    last_hhmm = pms("c65_entry_last_hhmm", 900, 1500, 1430).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            (col("datetime").dt.hour() * 100 + col("datetime").dt.minute()).alias("hhmm"),
            col("datetime").dt.date().alias("day"),
            col(
                col("open").first_value().alias("price"),
                col("open").first_value().alias("open_d0"),
                col("high").max().alias("day_high"),
                col("low").min().alias("day_low"),
                col("datetime").count().alias("bars_in_day"),
            ).expanding().over(col("datetime").dt.date()),
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col('day_high', 'day_low').shift(1).expanding().alias('preday_high', 'preday_low'),
            col("close_1").ta.ema(turn_len).expanding().alias("long_mid"),
        )
        .with_cols(
            (col("preday_high") - col("preday_low")).alias("preday_hl"),
        )
        .with_cols(
            (col("open_d0") + col("preday_hl") * pct).alias("upline"),
            (col("open_d0") - col("preday_hl") * pct).alias("dnline"),
        )
        .with_cols(
            col("upline", "dnline").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("hhmm") > enter_hhmm)
                & (col("hhmm") < last_hhmm)
                & (col("high") >= col("upline_1"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("hhmm") > enter_hhmm)
                & (col("hhmm") < last_hhmm)
                & (col("low") <= col("dnline_1"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
            col("open_long_sig", "open_short_sig").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "high_1", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("higher_after_long"),
            col("open_short_pre", "exit_short_pre", "low_1", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("lower_after_short"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & (
                    (col("low") <= (col("higher_after_long") * (1.0 - trs * 0.01)))
                    | ((col("low") <= col("price")) & (col("low") <= col("long_mid")))
                    | (col("low") <= col("upline", "price").min(axis=1))
                )
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & (
                    (col("high") >= (col("lower_after_short") * (1.0 + trs * 0.01)))
                    | ((col("high") >= col("price")) & (col("high") >= col("long_mid")))
                    | (col("high") >= col("dnline", "price").max(axis=1))
                )
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c65", "按 C65 源码实现：昨日日振幅映射开盘突破入场，叠加趋势中枢与多重价格阈值组合离场。")

def build_c66_expr() -> Expr:
    length = pms("c66_length", 10, 200, 10).limit(20)
    slow_len = pms("c66_slow_length", 2, 60, 2).limit(20)
    m = pms("c66_m", 5, 100, 5).limit(20)
    ts = pms("c66_ts", 1.0, 100.0, 1.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("close_1").mean().rolling(60).alias("ma1"),
        )
        .with_cols(
            col("high").max().rolling(length).alias("highest_v"),
            col("low").min().rolling(length).alias("lowest_v"),
        )
        .with_cols(
            (col("highest_v") - col("lowest_v")).alias("hl_v"),
            (col("close") - col("lowest_v")).alias("cl_v"),
        )
        .with_cols(
            col("hl_v", "cl_v").sum().rolling(slow_len).alias("sum_hl", "sum_cl"),
        )
        .with_cols(
            col(col("sum_hl").abs() <= 1e-12, 0.0, col("sum_cl") / col("sum_hl") * 100.0).if_else().alias("k_value"),
        )
        .with_cols(
            col("k_value").mean().rolling(slow_len).alias("d_value"),
        )
        .with_cols(
            col("k_value", "d_value").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("k_value") > col("d_value")) & (col("k_value_1") <= col("d_value_1"))).fill_null(False).alias("cross_up"),
            ((col("k_value") < col("d_value")) & (col("k_value_1") >= col("d_value_1"))).fill_null(False).alias("cross_dn"),
        )
        .with_cols(
            col("cross_up", "cross_dn").shift(1).count_last().expanding().alias("since_up", "since_dn"),
        )
        .with_cols(
            col(
                (col("since_dn").cast(pl.Int64) - col("since_up").cast(pl.Int64)).cast(pl.Int32),
                m.cast(pl.Int32),
            ).max(axis=1)
            .cast(pl.UInt32)
            .alias("len_hd_u"),
            col(
                (col("since_up").cast(pl.Int64) - col("since_dn").cast(pl.Int64)).cast(pl.Int32),
                m.cast(pl.Int32),
            ).max(axis=1)
            .cast(pl.UInt32)
            .alias("len_ld_u"),
            col("cross_up", "cross_dn").shift(2).count_last().expanding().alias("since_up2", "since_dn2"),
        )
        .with_cols(
            col("high_1").max().rolling_dynamic("len_hd_u", window_max=8192, min_samples=1).alias("highup"),
            col("low_1").min().rolling_dynamic("len_ld_u", window_max=8192, min_samples=1).alias("lowdown"),
        )
        .with_cols(
            col(col("cross_up").shift(1).expanding().fill_null(False), col("highup"), col.null).if_else().alias("hd_seed"),
            col(col("cross_dn").shift(1).expanding().fill_null(False), col("lowdown"), col.null).if_else().alias("ld_seed"),
        )
        .with_cols(
            col("hd_seed").ffill().expanding().alias("hd"),
            col("ld_seed").ffill().expanding().alias("ld"),
            col(col("since_up2") < col("since_dn2"), 1.0, -1.0).if_else().alias("k_state"),
        )
        .with_cols(
            (
                (col("k_state") > 0.0)
                & (col("high") >= col("hd").shift(1).expanding())
                & (col("hd").shift(1).expanding() > 0.0)
                & (col("close_1") > col("ma1"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("k_state") < 0.0)
                & (col("low") <= col("ld").shift(1).expanding())
                & (col("ld").shift(1).expanding() > 0.0)
                & (col("close_1") < col("ma1"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            col("open_long_sig", "open_short_sig").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "low_1", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("lower_after_entry"),
            col("open_short_pre", "exit_short_pre", "high_1", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("higher_after_entry"),
        )
        .with_cols(
            (col("lower_after_entry") - col("open") * ts * 0.001).alias("stop_long"),
            (col("higher_after_entry") + col("open") * ts * 0.001).alias("stop_short"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & (col("low") <= col("stop_long"))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & (col("high") >= col("stop_short"))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c66", "按 C66 源码实现：KD 交叉区间构造突破价入场，持仓按有利极值偏移止损离场。")

def build_c67_expr() -> Expr:
    length = pms("c67_length", 10, 200, 10).limit(20)
    n1 = pms("c67_n1", 1.0, 20.0, 1.0).limit(20)
    n2 = pms("c67_n2", 1.0, 20.0, 1.0).limit(20)
    ts = pms("c67_ts", 1.0, 20.0, 1.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("datetime").dt.date().alias("day"),
            col("open").first_value().expanding().over(col("datetime").dt.date()).alias("open_d0"),
            col("high", "low", "close").ta.atr(length).expanding().alias("atrvar"),
            col('high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("open_d0") + n1 * col("atrvar").shift(1).expanding()).alias("dupband"),
            (col("open_d0") - n2 * col("atrvar").shift(1).expanding()).alias("ddnband"),
            col("close").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("close") > col("dupband")) & (col("close_1") <= col("dupband").shift(1).expanding())).fill_null(False).alias("dkcond"),
            ((col("close") < col("ddnband")) & (col("close_1") >= col("ddnband").shift(1).expanding())).fill_null(False).alias("kkcond"),
        )
        .with_cols(
            col("dkcond").shift(1).expanding().fill_null(False).alias("open_long_raw"),
            col("kkcond").shift(1).expanding().fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
            col("open_long_sig", "open_short_sig").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "low_1", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("lower_after_entry"),
            col("open_short_pre", "exit_short_pre", "high_1", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("higher_after_entry"),
        )
        .with_cols(
            (col("lower_after_entry") - col("open") * ts * 0.001).alias("stop_long"),
            (col("higher_after_entry") + col("open") * ts * 0.001).alias("stop_short"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & (col("low") <= col("stop_long"))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & (col("high") >= col("stop_short"))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c67", "按 C67 源码实现：日开盘±ATR 通道突破入场，持仓后按有利极值偏移线离场。")

def build_c68_expr() -> Expr:
    length = pms("c68_length", 2, 60, 11).limit(20)
    ts = pms("c68_ts", 1.0, 20.0, 6.0).limit(20)
    exit_hhmm = pms("c68_exit_on_close_hhmm", 900, 1500, 1450).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            (col("datetime").dt.hour() * 100 + col("datetime").dt.minute()).alias("hhmm"),
            col("datetime").dt.date().alias("day"),
            col("datetime").count().expanding().over(col("datetime").dt.date()).alias("bars_in_day"),
            col('high', 'low', 'close').shift(1).expanding().add_suffix("1"),
            col.lit(length).to(pl.UInt32).alias("length_u"),
            col.lit(ts).alias("ts"),
            col.lit(exit_hhmm).alias("exit_hhmm"),
        )
        .with_cols(
            (col("bars_in_day").cast(pl.Int64) - 1).clip(lower_bound=0).alias("i_d"),
            (col("high_1") - col("close_1")).alias("pdr_up"),
            (col("close_1") - col("low_1")).alias("pdr_dn"),
            col(
                col("high_1") - col("low_1"),
                col("high").shift(2).expanding() - col("low").shift(2).expanding(),
                col("high").shift(3).expanding() - col("low").shift(3).expanding(),
                col("high").shift(4).expanding() - col("low").shift(4).expanding(),
            ).mean(axis=1).alias("temp"),
        )
        .with_cols(
            col("pdr_up", "temp").min(axis=1).ta.ema(4).expanding().alias("dgsv"),
            col("pdr_dn", "temp").min(axis=1).ta.ema(4).expanding().alias("kgsv"),
            col('high', 'low').shift(5).expanding().add_suffix("5"),
        )
        .with_cols(
            col("high_5").max().rolling(length).alias("hh5"),
            col("low_5").min().rolling(length).alias("ll5"),
        )
        .with_cols(
            (col("hh5") + col("dgsv", col("dgsv").shift(1).expanding()).max(axis=1)).alias("line1"),
            (col("ll5") - col("kgsv", col("kgsv").shift(1).expanding()).max(axis=1)).alias("line2"),
        )
        .with_cols(
            (
                (col("high") >= col("line1").shift(1).expanding())
                & (col("hhmm") < col("exit_hhmm"))
                & (col("hhmm") > 931)
                & (col("i_d") > 5)
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("low") <= col("line2").shift(1).expanding())
                & (col("hhmm") < col("exit_hhmm"))
                & (col("hhmm") > 931)
                & (col("i_d") > 5)
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            col("open_long_sig", "open_short_sig").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "low_1", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("lower_after_entry"),
            col("open_short_pre", "exit_short_pre", "high_1", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("higher_after_entry"),
        )
        .with_cols(
            (col("lower_after_entry") - col("open") * col("ts") * 0.001).alias("stop_long"),
            (col("higher_after_entry") + col("open") * col("ts") * 0.001).alias("stop_short"),
            (col("hhmm") >= col("exit_hhmm")).fill_null(False).alias("force_flat"),
        )
        .with_cols(
            (
                (
                    (col("pos_pre") == 1.0)
                    & (col("bars_long") > 0)
                    & (col("low") <= col("stop_long"))
                )
                | ((col("pos_pre") == 1.0) & col("force_flat"))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (
                    (col("pos_pre") == -1.0)
                    & (col("bars_short") > 0)
                    & (col("high") >= col("stop_short"))
                )
                | ((col("pos_pre") == -1.0) & col("force_flat"))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c68", "按 C68 源码实现：日内 Line1/Line2 突破入场，持仓按有利极值偏移线离场并在收盘前强平。")

def build_c69_expr() -> Expr:
    lenth = pms("c69_lenth", 20, 500, 20).limit(20)
    trailing = pms("c69_trailing_stop_rate", 1.0, 120.0, 1.0).limit(20)
    x = pms("c69_x", 1.0, 20.0, 1.0).limit(20)
    lx = (lenth * x * 0.1).floor()
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("datetime").dt.date().alias("day"),
            col.lit(trailing).alias("trailing"),
            col.lit(x).alias("x"),
            col("open").first_value().expanding().over(col("datetime").dt.date()).alias("open_d0"),
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("close").mean().rolling(240).alias("ma"),
            (col("high") - col("low")).alias("rc"),
            ((col("high") + col("low")) * 0.5).alias("midprice"),
            col("high_1").max().rolling(lenth).alias("hh_l"),
            col("low_1").min().rolling(lenth).alias("ll_l"),
        )
        .with_cols(
            (col("hh_l") - col("close_1") + col("open_d0")).alias("sy_src"),
            (col("ll_l") - col("close_1") + col("open_d0")).alias("xy_src"),
            col("rc").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("sy_src").max().rolling(lenth).alias("sy"),
            col("xy_src").min().rolling(lenth).alias("xy"),
            col("sy_src").max().rolling(lx).alias("psy"),
            col("xy_src").min().rolling(lx).alias("pxy"),
            ((col("midprice") > col("high_1")) & (col("rc") > col("rc_1"))).fill_null(False).alias("upband"),
            ((col("midprice") < col("low_1")) & (col("rc") < col("rc_1"))).fill_null(False).alias("downband"),
        )
        .with_cols(
            (
                (col("close_1") > col("ma").shift(1).expanding())
                & col("upband").shift(1).expanding().fill_null(False)
                & (col("close_1") > col("sy").shift(1).expanding())
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("close_1") < col("ma").shift(1).expanding())
                & col("downband").shift(1).expanding().fill_null(False)
                & (col("close_1") < col("xy").shift(1).expanding())
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            col("open_long_sig", "open_short_sig").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "low_1", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("lower_after_entry"),
            col("open_short_pre", "exit_short_pre", "high_1", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("higher_after_entry"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & (col("close_1") < col("pxy").shift(1).expanding())
                & col("downband").shift(1).expanding().fill_null(False)
            )
            .fill_null(False)
            .alias("exit_long_tech"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & (col("close_1") > col("psy"))
                & col("upband").shift(1).expanding().fill_null(False)
            )
            .fill_null(False)
            .alias("exit_short_tech"),
            (col("lower_after_entry") - col("open") * col("trailing") * 0.001).alias("stop_long"),
            (col("higher_after_entry") + col("open") * col("trailing") * 0.001).alias("stop_short"),
        )
        .with_cols(
            (
                col("exit_long_tech")
                | (
                    (col("pos_pre") == 1.0)
                    & (col("bars_long") > 0)
                    & (col("low") <= col("stop_long"))
                )
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                col("exit_short_tech")
                | (
                    (col("pos_pre") == -1.0)
                    & (col("bars_short") > 0)
                    & (col("high") >= col("stop_short"))
                )
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c69", "按 C69 源码实现：区间突破+长期均线过滤入场，技术反向与回撤止损组合离场。")

def build_c70_expr() -> Expr:
    m = pms("c70_m", 5, 500, 5).limit(20)
    trs = pms("c70_trs", 0.2, 20.0, 0.2).limit(20)
    ls = pms("c70_ls", 0.5, 40.0, 0.5).limit(20)
    length = pms("c70_length", 2, 60, 2).limit(20)
    slow_len = pms("c70_slow_length", 1, 20, 1).limit(20)
    smooth_len = pms("c70_smooth_length", 1, 20, 1).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("high").max().rolling(length).alias("highest_v"),
            col("low").min().rolling(length).alias("lowest_v"),
        )
        .with_cols(
            (col("highest_v") - col("lowest_v")).alias("hl_v"),
            (col("close") - col("lowest_v")).alias("cl_v"),
            col("close").ta.ema(length).expanding().alias("ma1"),
            col("close").ta.ema(m).expanding().alias("ma2"),
        )
        .with_cols(
            col("hl_v", "cl_v").sum().rolling(slow_len).alias("sum_hl", "sum_cl"),
            col('ma1', 'ma2').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col(col("sum_hl").abs() <= 1e-12, 0.0, col("sum_cl") / col("sum_hl") * 100.0).if_else().alias("k_value"),
            ((col("ma1") > col("ma2")) & (col("ma1_1") <= col("ma2_1"))).fill_null(False).alias("dkcond1"),
            ((col("ma1") < col("ma2")) & (col("ma1_1") >= col("ma2_1"))).fill_null(False).alias("kkcond1"),
        )
        .with_cols(
            col("k_value").mean().rolling(smooth_len).alias("d_value"),
        )
        .with_cols(
            col("k_value", "d_value").shift(1).expanding().add_suffix("1"),
            col("dkcond1", "kkcond1").count_last().expanding().alias("since_dk1", "since_kk1"),
        )
        .with_cols(
            ((col("k_value") > col("d_value")) & (col("k_value_1") <= col("d_value_1")) & (col("k_value") > 0.0)).fill_null(False).alias("dkcond2"),
            ((col("k_value") < col("d_value")) & (col("k_value_1") >= col("d_value_1")) & (col("k_value") > 0.0)).fill_null(False).alias("kkcond2"),
            col(col("since_dk1") < col("since_kk1"), 1.0, -1.0).if_else().alias("kg_state"),
        )
        .with_cols(
            (
                (col("kg_state") == 1.0)
                & col("dkcond2").shift(1).expanding().fill_null(False)
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("kg_state") == -1.0)
                & col("kkcond2").shift(1).expanding().fill_null(False)
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            col("open_long_sig", "open_short_sig").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            (col("entry_long") * (1.0 + ls * 0.01)).alias("dk_ls"),
            (col("entry_long") * (1.0 - trs * 0.01)).alias("dk_stop"),
            (col("entry_short") * (1.0 - ls * 0.01)).alias("kk_ls"),
            (col("entry_short") * (1.0 + trs * 0.01)).alias("kk_stop"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & ((col("high") >= col("dk_ls")) | (col("low") <= col("dk_stop")))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & ((col("low") <= col("kk_ls")) | (col("high") >= col("kk_stop")))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c70", "按 C70 源码实现：长短均线定向后用 KD 交叉触发入场，并按 LS 目标位与 TRS 止损位离场。")

def build_c71_expr() -> Expr:
    boll_len = pms("c71_boll_len", 5, 200, 5).limit(20)
    rng = pms("c71_range", 0.05, 1.0, 0.05).limit(20)
    trs = pms("c71_trs", 1.0, 20.0, 1.0).limit(20)
    exit_hhmm = pms("c71_exit_hhmm", 900, 1500, 900).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            (col("datetime").dt.hour() * 100 + col("datetime").dt.minute()).alias("hhmm"),
            col("datetime").dt.date().alias("day"),
            col(
                col("open").first_value().alias("opd"),
                col("high").max().alias("day_high"),
                col("low").min().alias("day_low"),
            ).expanding().over(col("datetime").dt.date()),
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col('day_high', 'day_low').shift(1).expanding().alias('preday_high', 'preday_low'),
            (col("close") - col("close").shift(2).expanding()).alias("direction"),
            (col("close") - col("close").shift(2).expanding()).abs().alias("abs_move"),
        )
        .with_cols(
            col("abs_move").sum().rolling(boll_len).alias("volatility"),
        )
        .with_cols(
            col(col("volatility").abs() <= 1e-12, 0.0, col("direction") / col("volatility") * 100.0).if_else().alias("effratio"),
            (col("preday_high") - col("preday_low")).alias("preday_hl"),
        )
        .with_cols(
            (col("opd") + col("preday_hl") * rng).alias("upband"),
            (col("opd") - col("preday_hl") * rng).alias("dnband"),
        )
        .with_cols(
            ((col("close") > col("upband")) & (col("close_1") <= col("upband").shift(1).expanding()) & (col("effratio") > 0.0)).fill_null(False).alias("cond1"),
            ((col("close") < col("dnband")) & (col("close_1") >= col("dnband").shift(1).expanding()) & (col("effratio") < 0.0)).fill_null(False).alias("cond2"),
        )
        .with_cols(
            col("cond1").shift(1).expanding().fill_null(False).alias("open_long_raw"),
            col("cond2").shift(1).expanding().fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            col("open_long_sig", "open_short_sig").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "low_1", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("lower_after_entry"),
            col("open_short_pre", "exit_short_pre", "high_1", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("higher_after_entry"),
        )
        .with_cols(
            (col("lower_after_entry") - col("open") * trs * 0.001).alias("stop_long"),
            (col("higher_after_entry") + col("open") * trs * 0.001).alias("stop_short"),
            (col("hhmm") >= exit_hhmm).fill_null(False).alias("force_flat"),
        )
        .with_cols(
            (
                (
                    (col("pos_pre") == 1.0)
                    & (col("bars_long") > 0)
                    & (col("low") <= col("stop_long"))
                )
                | ((col("pos_pre") == 1.0) & col("force_flat"))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (
                    (col("pos_pre") == -1.0)
                    & (col("bars_short") > 0)
                    & (col("high") >= col("stop_short"))
                )
                | ((col("pos_pre") == -1.0) & col("force_flat"))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c71", "按 C71 源码实现：昨日区间映射开盘上下轨并结合效率系数入场，回撤止损与收盘强平离场。")

def build_c72_expr() -> Expr:
    n1 = pms("c72_n1", 5, 300, 5).limit(20)
    n2 = pms("c72_n2", 10, 1000, 10).limit(20)
    y = pms("c72_y", 0.5, 20.0, 0.5).limit(20)
    trs = pms("c72_trs", 5.0, 200.0, 5.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").mean().rolling(n1).alias("nma1"),
            col("close").mean().rolling(n2).alias("nma2"),
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("nma1") - col("nma1").shift(n1).expanding()) / col.lit(n1).to(pl.Float64)).alias("k1"),
            ((col("nma2") - col("nma2").shift(n2).expanding()) / col.lit(n2).to(pl.Float64)).alias("k2"),
            col("close").shift(30).expanding().add_suffix("30"),
        )
        .with_cols(
            (col("k1") > col("k2")).fill_null(False).alias("cond1"),
            (col("k1") < col("k2")).fill_null(False).alias("cond2"),
            ((col("nma1") > col("nma2")) & (col("nma2").shift(1).expanding() > col("nma1").shift(1).expanding())).fill_null(False).alias("cond3"),
            ((col("nma1") < col("nma2")) & (col("nma2").shift(1).expanding() < col("nma1").shift(1).expanding())).fill_null(False).alias("cond4"),
            (col("close") > col("close_30")).fill_null(False).alias("hh"),
            (col("close") < col("close_30")).fill_null(False).alias("ll"),
            ((col("high_1") > col("high").shift(2).expanding()) & (col("high_1") > col("high").shift(3).expanding())).fill_null(False).alias("hup"),
            ((col("low_1") < col("low").shift(2).expanding()) & (col("low_1") < col("low").shift(3).expanding())).fill_null(False).alias("ldn"),
        )
        .with_cols(
            col("cond1", "cond2").shift(1).count_last().expanding().alias("since_cond1", "since_cond2"),
            col("cond3").shift(1).expanding().to(pl.UInt32).sum().expanding().alias("upbreak_cnt"),
            col("cond4").shift(1).expanding().to(pl.UInt32).sum().expanding().alias("downbreak_cnt"),
        )
        .with_cols(
            col(col("since_cond1") < col("since_cond2"), 1.0, -1.0).if_else().alias("kg"),
            (col("upbreak_cnt") > 0).fill_null(False).alias("upbreak"),
            (col("downbreak_cnt") > 0).fill_null(False).alias("downbreak"),
        )
        .with_cols(
            (col("hup") & col("upbreak") & (col("kg") == 1.0) & col("hh").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_long_raw"),
            (col("ldn") & col("downbreak") & (col("kg") == -1.0) & col("ll").shift(1).expanding().fill_null(False)).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
            *_false_exit_pre_exprs("exit_long_pre", "exit_short_pre"),
        )
        .with_cols(
            col("open_long_sig", "open_short_sig").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "exit_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "exit_long_pre", "low_1", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("lower_after_entry"),
            col("open_short_pre", "exit_short_pre", "high_1", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("higher_after_entry"),
        )
        .with_cols(
            col(1.0 - 0.1 * (col(col("pos_pre") != 0.0, col("bars_long") + col("bars_short"), 0.0).if_else()), 0.5).max(axis=1).alias("liqka"),
        )
        .with_cols(
            (col("lower_after_entry") - (col("open") * trs * 0.001) * col("liqka")).alias("dliq"),
            (col("higher_after_entry") + (col("open") * trs * 0.001) * col("liqka")).alias("kliq"),
            col('low', 'high').shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & (col("low_1") <= col("dliq").shift(1).expanding())
                & (col("low_2") >= col("dliq").shift(2).expanding())
                & (col("dliq").shift(1).expanding() > 0.0)
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & (col("high_1") >= col("kliq").shift(1).expanding())
                & (col("high_2") <= col("kliq").shift(2).expanding())
                & (col("kliq").shift(1).expanding() > 0.0)
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(e, "c72", "按 C72 源码实现：双均线斜率与结构状态共振入场，持仓按自适应 liQKA 吊灯线反向穿越离场。")

def build_c73_expr() -> Expr:
    length = pms("c73_length", 10, 300, 10).limit(20)
    avg_length = pms("c73_avg_length", 20, 600, 20).limit(20)
    rng_pct = pms("c73_rng_pct", 20.0, 500.0, 20.0).limit(20)
    trs = pms("c73_trs", 10.0, 300.0, 10.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col.lit(trs).alias("trs"),
            col("close", "high", "low").shift(1).expanding().add_suffix("1"),
            col("high", "low").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            col("high", "low", "close").ta.cci(length).expanding().alias("cci_value"),
            col("high_1").max().rolling(length).alias("range_h"),
            col("low_1").min().rolling(length).alias("range_l"),
        )
        .with_cols(
            col("cci_value").mean().rolling(avg_length).alias("cci_avg"),
            (col("range_h") - col("range_l")).alias("t_range"),
            ((col("high") + col("low")) * 0.5).alias("mid_price"),
        )
        .with_cols(
            col((col("range_h") - col("high_1")) > 0.0, col("range_h") - col("high_1"), 0.0).if_else()
            .alias("h_gap"),
            col((col("low_1") - col("range_l")) > 0.0, col("low_1") - col("range_l"), 0.0).if_else()
            .alias("l_gap"),
            (col("close") > col("range_h")).fill_null(False).alias("long_go"),
            (col("close") < col("range_l")).fill_null(False).alias("short_go"),
        )
        .with_cols(
            col("h_gap", "l_gap").sum().rolling(avg_length).alias("h_gap_sum", "l_gap_sum"),
        )
        .with_cols(
            (col("h_gap_sum") + col("l_gap_sum")).alias("no_trades"),
        )
        .with_cols(
            (col("no_trades") >= (col("t_range") * (rng_pct * 0.01))).fill_null(False).alias("rangedk"),
            (col("mid_price") > col("high_1")).fill_null(False).alias("mid_gt_high1"),
            (col("mid_price") < col("low_1")).fill_null(False).alias("mid_lt_low1"),
            col('long_go', 'short_go').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                col("rangedk")
                & col("long_go_1").fill_null(False)
                & col("mid_gt_high1")
                & (col("cci_avg") > 0.0)
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                col("rangedk")
                & col("short_go_1").fill_null(False)
                & col("mid_lt_low1")
                & (col("cci_avg") < 0.0)
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "open_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            _bars_pos_from_open_sig_expr("open_long_sig", "open_short_sig", "bars_pos"),
            col("open_long_pre", "open_short_pre", "low", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("lower_after_entry"),
            col("open_short_pre", "open_long_pre", "high", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("higher_after_entry"),
        )
        .with_cols(
            col(col("pos_pre") == 0.0, 1.0, col(
                    1.0 - col("bars_pos").cast(pl.Float64) * 0.1,
                    0.5,
                ).max(axis=1)).if_else()
            .alias("liqka"),
        )
        .with_cols(
            (col("lower_after_entry") - (col("open") * col("trs") * 0.001) * col("liqka")).alias("dliq"),
            (col("higher_after_entry") + (col("open") * col("trs") * 0.001) * col("liqka")).alias("kliq"),
        )
        .with_cols(
            col("dliq", "kliq").shift(1).expanding().add_suffix("1"),
            col("dliq", "kliq").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("low_1") <= col("dliq_1"))
                & (col("low_2") >= col("dliq_2"))
                & (col("dliq_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("high_1") >= col("kliq_1"))
                & (col("high_2") <= col("kliq_2"))
                & (col("kliq_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "c73",
        "按 C73 源码实现：CCI 均线方向+区间突破入场，持仓后用 liQKA 自适应吊灯线反向穿越离场。",
    )

def build_c74_expr() -> Expr:
    fast_ma = pms("c74_fast_ma", 3, 60, 3).limit(20)
    slow_ma = pms("c74_slow_ma", 6, 140, 6).limit(20)
    signal_ma = pms("c74_signal_ma", 8, 220, 8).limit(20)
    atr_len = pms("c74_atr_len", 6, 140, 6).limit(20)
    length = pms("c74_length", 3, 60, 3).limit(20)
    eatr_pct = pms("c74_eatr_pct", 0.5, 10.0, 0.5).limit(20)
    xatr_pct = pms("c74_xatr_pct", 0.5, 10.0, 0.5).limit(20)
    ts = pms("c74_ts", 10.0, 300.0, 10.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col.lit(ts).alias("ts"),
            col("datetime").dt.date().alias("day"),
            col("close", "high", "low").shift(1).expanding().add_suffix("1"),
            col("high", "low").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            col("day").shift(1).expanding().add_suffix("1"),
            col("close").ta.ema(fast_ma).expanding().alias("ema_fast"),
            col("close").ta.ema(slow_ma).expanding().alias("ema_slow"),
            col("high", "low", "close").ta.atr(atr_len).expanding().alias("aatr"),
        )
        .with_cols(
            (col("day") != col("day_1")).fill_null(True).alias("is_new_day"),
            (col("ema_fast") - col("ema_slow")).alias("macd_line"),
        )
        .with_cols(
            col("is_new_day", "close_1", col.null).if_else().alias("prev_close_seed"),
        )
        .with_cols(
            col("macd_line").ta.ema(signal_ma).expanding().alias("signal_line"),
            col("prev_close_seed").ffill().expanding().alias("prev_close"),
            col("aatr").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("prev_close").mean().rolling(length).alias("avg_myclose"),
            col("signal_line").shift(1).expanding().alias("signal_1"),
        )
        .with_cols(
            ((col("signal_line") > 0.0) & (col("signal_1") <= 0.0)).fill_null(False).alias("cross_up"),
            ((col("signal_line") < 0.0) & (col("signal_1") >= 0.0)).fill_null(False).alias("cross_dn"),
        )
        .with_cols(
            col("cross_up", "cross_dn").count_last().expanding().alias("since_up", "since_dn"),
        )
        .with_cols(
            col(col("since_up") < col("since_dn"), 1.0, -1.0).if_else().alias("trend_state"),
        )
        .with_cols(
            col("trend_state").shift(1).expanding().add_suffix("1"),
            (col("trend_state") == 1.0).fill_null(False).alias("up_trend"),
            (col("trend_state") == -1.0).fill_null(False).alias("dn_trend"),
        )
        .with_cols(
            (col("up_trend") & (col("trend_state_1") != 1.0)).fill_null(False).alias("buy_setup_start"),
            (col("dn_trend") & (col("trend_state_1") != -1.0)).fill_null(False).alias("sell_setup_start"),
        )
        .with_cols(
            col("sell_setup_start", col("low_1") - eatr_pct * col("aatr_1"), col.null).if_else()
            .alias("lower_seed"),
            col("sell_setup_start", col("high_1") + xatr_pct * col("aatr_1"), col.null).if_else()
            .alias("exit_k_seed"),
            col("buy_setup_start", col("high_1") + eatr_pct * col("aatr_1"), col.null).if_else()
            .alias("upper_seed"),
            col("buy_setup_start", col("low_1") - xatr_pct * col("aatr_1"), col.null).if_else()
            .alias("exit_d_seed"),
        )
        .with_cols(
            col("lower_seed").ffill().expanding().alias("lowerband"),
            col("upper_seed").ffill().expanding().alias("upperband"),
            col("exit_k_seed").ffill().expanding().alias("exitband_k"),
            col("exit_d_seed").ffill().expanding().alias("exitband_d"),
        )
        .with_cols(
            (
                col("up_trend")
                & (col("close_1") > col("avg_myclose"))
                & (col("high") >= col("upperband"))
                & (col("upperband") > 0.0)
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                col("dn_trend")
                & (col("close_1") < col("avg_myclose"))
                & (col("low") <= col("lowerband"))
                & (col("lowerband") > 0.0)
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "open_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            _bars_pos_from_open_sig_expr("open_long_sig", "open_short_sig", "bars_pos"),
            col("open_long_sig", "open_short_sig").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "low", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("lower_after_entry"),
            col("open_short_pre", "open_long_pre", "high", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("higher_after_entry"),
        )
        .with_cols(
            col(col("pos_pre") == 0.0, 1.0, col(
                    1.0 - col("bars_pos").cast(pl.Float64) * 0.1,
                    0.5,
                ).max(axis=1)).if_else()
            .alias("liqka"),
        )
        .with_cols(
            (col("lower_after_entry") - (col("open") * col("ts") * 0.001) * col("liqka")).alias("dliq"),
            (col("higher_after_entry") + (col("open") * col("ts") * 0.001) * col("liqka")).alias("kliq"),
        )
        .with_cols(
            col("dliq", "kliq").shift(1).expanding().add_suffix("1"),
            col("dliq", "kliq").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & (col("low") <= col("exitband_d"))
                & (col("exitband_d") > 0.0)
            )
            .fill_null(False)
            .alias("exit_long_band"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & (col("high") >= col("exitband_k"))
                & (col("exitband_k") > 0.0)
            )
            .fill_null(False)
            .alias("exit_short_band"),
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("low_1") <= col("dliq_1"))
                & (col("low_2") >= col("dliq_2"))
                & (col("dliq_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_long_trail"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("high_1") >= col("kliq_1"))
                & (col("high_2") <= col("kliq_2"))
                & (col("kliq_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_short_trail"),
        )
        .with_cols(
            col("exit_long_band", "exit_long_trail").any(axis=1).fill_null(False).alias("exit_long_raw"),
            col("exit_short_band", "exit_short_trail").any(axis=1).fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "c74",
        "按 C74 源码实现：SignalLine 零轴切换设定 ATR 入/出场触发带，叠加跨日均线过滤与 liQKA 自适应吊灯线离场。",
    )

def build_c75_expr() -> Expr:
    n1 = pms("c75_n1", 10, 200, 75).limit(20)
    n2 = pms("c75_n2", 5, 120, 30).limit(20)
    n3 = pms("c75_n3", 20, 400, 105).limit(20)
    n4 = pms("c75_n4", 40, 600, 135).limit(20)
    s1_period = pms("c75_s1_period", 5, 1200, 269).limit(20)
    s2_period = pms("c75_s2_period", 5, 1200, 209).limit(20)
    s3_period = pms("c75_s3_period", 5, 1200, 59).limit(20)
    ts = pms("c75_ts", 10.0, 300.0, 120.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col.lit(ts).alias("ts"),
            col("datetime").dt.date().alias("day"),
            col("close", "high", "low").shift(1).expanding().add_suffix("1"),
            col("high", "low").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            col("day").shift(1).expanding().add_suffix("1"),
            ((col("high") + col("low")) * 0.5).alias("hl"),
            col("high").max().rolling(5).alias("hh"),
            col("low").min().rolling(5).alias("ll"),
        )
        .with_cols(
            (col("day") != col("day_1")).fill_null(True).alias("is_new_day"),
        )
        .with_cols(
            col("is_new_day", "close_1", col.null).if_else().alias("prev_close_seed"),
            col("hl").shift(n1).expanding().alias("hl_n1"),
            col("hl").shift(n2).expanding().alias("hl_n2"),
            col("hl").shift(n3).expanding().alias("hl_n3"),
        )
        .with_cols(
            col("prev_close_seed").ffill().expanding().alias("prev_close"),
            col("hl_n3").ta.ema(s1_period).expanding().alias("s1"),
            col("hl_n2").ta.ema(s2_period).expanding().alias("s2"),
            col("hl_n1").ta.ema(s3_period).expanding().alias("s3"),
        )
        .with_cols(
            col("prev_close").mean().rolling(10).alias("avg_myclose"),
            col("s1", "s2", "s3").max(axis=1).alias("p_max"),
            col("s1", "s2", "s3").min(axis=1).alias("p_min"),
            (col("high") >= col("s1", "s2", "s3").max(axis=1)).fill_null(False).alias("dkcond"),
            (col("low") <= col("s1", "s2", "s3").min(axis=1)).fill_null(False).alias("kkcond"),
        )
        .with_cols(
            col("dkcond", "kkcond").shift(1).expanding().add_suffix("1"),
            col("hh", "ll").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            (
                (col("close_1") > col("avg_myclose"))
                & col("dkcond_1").fill_null(False)
                & (col("close_1") > col("hh_2"))
                & ((col("p_max") - col("p_min")) > 0.005 * col("p_min"))
            )
            .fill_null(False)
            .alias("long_cond"),
            (
                (col("close_1") < col("avg_myclose"))
                & col("kkcond_1").fill_null(False)
                & (col("close_1") < col("ll_2"))
                & ((col("p_max") - col("p_min")) > 0.005 * col("p_min"))
            )
            .fill_null(False)
            .alias("short_cond"),
        )
        .with_cols(
            col("long_cond", "short_cond").alias("open_long_raw", "open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "open_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            _bars_pos_from_open_sig_expr("open_long_sig", "open_short_sig", "bars_pos"),
            col("open_long_sig", "open_short_sig").count_last().expanding().alias("bars_long", "bars_short"),
            col("open_long_pre", "open_short_pre", "low", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("lower_after_entry"),
            col("open_short_pre", "open_long_pre", "high", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("higher_after_entry"),
            col('short_cond', 'long_cond').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col(col("pos_pre") == 0.0, 1.0, col(
                    1.0 - col("bars_pos").cast(pl.Float64) * 0.1,
                    0.5,
                ).max(axis=1)).if_else()
            .alias("liqka"),
        )
        .with_cols(
            (col("lower_after_entry") - (col("open") * col("ts") * 0.001) * col("liqka")).alias("dliq"),
            (col("higher_after_entry") + (col("open") * col("ts") * 0.001) * col("liqka")).alias("kliq"),
        )
        .with_cols(
            col("dliq", "kliq").shift(1).expanding().add_suffix("1"),
            col("dliq", "kliq").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & col("short_cond_1").fill_null(False)
            )
            .fill_null(False)
            .alias("exit_long_flip"),
            (
                (col("pos_pre") == -1.0)
                & col("long_cond_1").fill_null(False)
            )
            .fill_null(False)
            .alias("exit_short_flip"),
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("low_1") <= col("dliq_1"))
                & (col("low_2") >= col("dliq_2"))
                & (col("dliq_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_long_trail"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("high_1") >= col("kliq_1"))
                & (col("high_2") <= col("kliq_2"))
                & (col("kliq_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_short_trail"),
        )
        .with_cols(
            col("exit_long_flip", "exit_long_trail").any(axis=1).fill_null(False).alias("exit_long_raw"),
            col("exit_short_flip", "exit_short_trail").any(axis=1).fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "c75",
        "按 C75 源码实现：三层 SmoothedAverage 结构突破配合跨日均价过滤入场，反向条件与 liQKA 自适应吊灯线共同离场。",
    )

def build_c76_expr() -> Expr:
    fast_len = pms("c76_fast_length", 1, 120, 1).limit(20)
    slow_len = pms("c76_slow_length", 5, 300, 5).limit(20)
    macd_len = pms("c76_macd_length", 1, 40, 1).limit(20)
    ts = pms("c76_ts", 1.0, 120.0, 1.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col.lit(ts).alias("ts"),
            col("close").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("close").mean().rolling(fast_len).alias("ma1"),
            col("close").mean().rolling(slow_len).alias("ma2"),
            col("close").ta.ema(fast_len).expanding().alias("ema_fast"),
            col("close").ta.ema(slow_len).expanding().alias("ema_slow"),
            col("high", "low", "close").ta.adx(fast_len).expanding().alias("adx_val"),
        )
        .with_cols(
            (col("ema_fast") - col("ema_slow")).alias("macd_value"),
            col('adx_val', 'ma1', 'ma2').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("macd_value").ta.ema(macd_len).expanding().alias("avg_macd"),
        )
        .with_cols(
            col('macd_value', 'avg_macd').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("macd_value") > col("avg_macd")).fill_null(False).alias("dk"),
            (col("macd_value") < col("avg_macd")).fill_null(False).alias("kk"),
            ((col("adx_val") > col("adx_val_1")) & (col("adx_val") > 20.0)).fill_null(False).alias("xadx"),
        )
        .with_cols(
            col('dk', 'kk', 'xadx').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("dk_1") & col("xadx_1") & (col("ma1_1") > col("ma2_1"))).fill_null(False).alias("open_long_raw"),
            (col("kk_1") & col("xadx_1") & (col("ma1_1") < col("ma2_1"))).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "open_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            _bars_pos_from_open_sig_expr("open_long_sig", "open_short_sig", "bars_pos"),
            col("open_long_pre", "open_short_pre", "low", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("lower_after_entry"),
            col("open_short_pre", "open_long_pre", "high", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("higher_after_entry"),
        )
        .with_cols(
            (col("lower_after_entry") - col("open") * col("ts") * 0.001).alias("myprice_long"),
            (col("higher_after_entry") + col("open") * col("ts") * 0.001).alias("myprice_short"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("low") <= col("myprice_long"))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("high") >= col("myprice_short"))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "c76",
        "按 C76 源码实现：MACD 与 ADX 增强共振并用快慢均线过滤入场，持仓后按开仓后有利极值偏移线止损离场。",
    )

def build_q01_expr() -> Expr:
    n_break = pms("q01_break_n", 5, 160, 5).limit(20)
    n_trend = pms("q01_trend_n", 10, 300, 10).limit(20)
    n_exit = pms("q01_exit_n", 3, 120, 3).limit(20)
    stop_pct = pms("q01_stop_pct", 0.5, 20.0, 0.5).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
            col.lit(stop_pct).alias("stop_pct"),
        )
        .with_cols(
            col("close_1").mean().rolling(n_trend).alias("ma_trend"),
            col("high_1").max().rolling(n_break).alias("hh_break"),
            col("low_1").min().rolling(n_break).alias("ll_break"),
            col("high_1").max().rolling(n_exit).alias("hh_exit"),
            col("low_1").min().rolling(n_exit).alias("ll_exit"),
        )
        .with_cols(
            (
                (col("close_1") > col("ma_trend"))
                & (col("high") >= col("hh_break"))
                & (col("hh_break") > 0.0)
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("close_1") < col("ma_trend"))
                & (col("low") <= col("ll_break"))
                & (col("ll_break") > 0.0)
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "open_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            _bars_pos_from_open_sig_expr("open_long_sig", "open_short_sig", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "low", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("lower_after_entry"),
            col("open_short_pre", "open_long_pre", "high", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("higher_after_entry"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (
                    (col("low") <= col("ll_exit"))
                    | (col("low") <= col("entry_long") * (1.0 - col("stop_pct") * 0.01))
                    | (col("low") <= col("lower_after_entry") * (1.0 - col("stop_pct") * 0.01))
                )
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (
                    (col("high") >= col("hh_exit"))
                    | (col("high") >= col("entry_short") * (1.0 + col("stop_pct") * 0.01))
                    | (col("high") >= col("higher_after_entry") * (1.0 + col("stop_pct") * 0.01))
                )
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "q01",
        "按 Q01（Classic_Break）实现主逻辑：趋势均线过滤下的经典区间突破入场，通道反向与入场后回撤止损离场。注：原包仅含 fbk 二进制，按可解释主逻辑落地。",
    )

def build_q02_expr() -> Expr:
    td = pms("q02_td", 0, 20, 0).limit(20)
    ratio = pms("q02_ratio", 0.0, 3.0, 0.0).limit(20)
    min_local_volatili = pms("q02_min_local_volatili", 0.1, 6.0, 0.1).limit(20)
    stoploss = pms("q02_stoploss", 1.0, 80.0, 1.0).limit(20)
    p = pms("q02_p", 1.0, 80.0, 1.0).limit(20)
    p2 = pms("q02_p2", 1.0, 80.0, 1.0).limit(20)
    stop3 = pms("q02_stop3", 1.0, 80.0, 1.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            (
                col("datetime").dt.hour().cast(pl.Float64) * 10000.0
                + col("datetime").dt.minute().cast(pl.Float64) * 100.0
                + col("datetime").dt.second().cast(pl.Float64)
            )
            .cast(pl.Int32)
            .alias("hhmmss"),
            col("datetime").dt.date().alias("day"),
            col('close', 'high', 'low', 'open').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("day").shift(1).expanding().add_suffix("1"),
            col(
                col("open").first_value().alias("open_d0"),
                col("high").max().alias("day_high"),
                col("low").min().alias("day_low"),
                col("datetime").count().alias("bars_in_day"),
            ).expanding().over(col("day")),
        )
        .with_cols(
            (col("day") != col("day_1")).fill_null(True).alias("is_new_day"),
            col("close").mean().rolling(4).alias("ma1"),
            col("close").mean().rolling(10).alias("ma2"),
            col("close").mean().rolling(4).alias("ma3"),
            col("close").mean().rolling(15).alias("ma4"),
            col("close").mean().rolling(52).alias("ma52"),
            col("close").mean().rolling(20).alias("midline"),
            col("close").std().rolling(20).alias("band"),
        )
        .with_cols(
            col("is_new_day", "close_1", col.null).if_else().alias("yclose_seed"),
            (col("midline") + ratio * col("band")).alias("upline"),
            (col("midline") - ratio * col("band")).alias("downline"),
            col("low").min().rolling(9).alias("llv9"),
            col("high").max().rolling(9).alias("hhv9"),
            col("close").ta.ema(22).expanding().alias("ema22"),
            col("close").ta.ema(56).expanding().alias("ema56"),
        )
        .with_cols(
            col("yclose_seed").ffill().expanding().alias("yclose"),
            col((col("hhv9") - col("llv9")).abs() <= 1e-12, 50.0, (col("close") - col("llv9")) / (col("hhv9") - col("llv9")) * 100.0).if_else()
            .alias("rsv"),
            (col("ema22") - col("ema56")).alias("diff"),
            (col("ma2") - col("ma52")).alias("b2"),
            (col("high") > col("upline")).cast(pl.UInt32).alias("high_gt_up"),
        )
        .with_cols(
            col("rsv").ewm(1.0 / 3.0).expanding().alias("k"),
            col("diff").ta.ema(9).expanding().alias("dea"),
            col("b2").shift(2).expanding().add_suffix("2"),
            col("high_gt_up").sum().rolling(2, 1).alias("up_cnt2"),
        )
        .with_cols(
            (col("b2") < col("b2_2")).fill_null(False).alias("kkxd"),
            col("k").ewm(1.0 / 3.0).expanding().alias("d"),
            (2.0 * (col("diff") - col("dea"))).alias("macd"),
            (col("up_cnt2") < 2).fill_null(False).alias("kkxd2"),
            (col("band") >= min_local_volatili).fill_null(False).alias("vol_ok"),
        )
        .with_cols(
            (3.0 * col("k") - 2.0 * col("d")).alias("j"),
            (
                (col("open_d0") < (col("yclose") - 4.0))
                & (col("close") < (col("open") - 13.0))
                & (col("close") > (col("yclose") - 30.0))
            )
            .fill_null(False)
            .alias("gap_short"),
            (
                (col("open_d0") > (col("yclose") + 4.0))
                & (col("close") > (col("open") + 13.0))
                & (col("close") < (col("yclose") + 22.0))
            )
            .fill_null(False)
            .alias("gap_long"),
            col(
                col("low").min().rolling(10).alias("llv10"),
                col("high").max().rolling(5).alias("hhv5"),
                col("low").min().rolling(5).alias("llv5"),
            ).shift(1).expanding().alias("llv10_1", "hhv5_1", "llv5_1"),
            col("macd").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("hhmmss") == 94500)
                & col("gap_long")
            )
            .fill_null(False)
            .alias("open_long_gap"),
            (
                (col("hhmmss") == 94500)
                & col("gap_short")
            )
            .fill_null(False)
            .alias("open_short_gap"),
            (
                col("vol_ok")
                & col("kkxd2")
                & (col("hhmmss") >= 94500)
                & (col("hhmmss") <= 143000)
                & (col("low") < col("downline"))
            )
            .fill_null(False)
            .alias("open_long_base"),
            (
                col("vol_ok")
                & (col("hhmmss") >= 94500)
                & (col("hhmmss") <= 143000)
                & ((col("high") > col("upline")) | ((col("k") < col("d")) & (col("close") < col("llv10_1")) & (col("close") > col("midline"))))
            )
            .fill_null(False)
            .alias("open_short_base"),
        )
        .with_cols(
            col("open_long_gap", "open_long_base").any(axis=1).fill_null(False).alias("open_long_raw"),
            col("open_short_gap", "open_short_base").any(axis=1).fill_null(False).alias("open_short_raw"),
            (col("hhmmss") >= 150000).fill_null(False).alias("force_flat"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            *_open_pre_exprs("open_long_sig", "open_short_sig", "open_long_pre", "open_short_pre"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_long"),
            col("open_short_pre", "open_long_pre", "open").stra.t02_track_entry_price().expanding().alias("entry_short"),
        )
        .with_cols(
            _bars_pos_from_open_sig_expr("open_long_sig", "open_short_sig", "bars_pos"),
            col("open_long_pre", "open_short_pre", "high", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("high_after_entry"),
            col("open_short_pre", "open_long_pre", "low", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("low_after_entry"),
        )
        .with_cols(
            col(
                col("llv5_1"),
                col("high_after_entry") - p,
                col("entry_long") - stop3,
            ).min(axis=1).alias("stop_long_line"),
            col(
                col("hhv5_1"),
                col("low_after_entry") + p2,
                col("entry_short") + stop3,
            ).max(axis=1).alias("stop_short_line"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & col(
                    col("low") <= col("stop_long_line"),
                    (col("close") < col("upline")) & (col("macd") < col("macd_1")),
                    col("close") <= col("entry_long") - stoploss,
                    col("force_flat"),
                ).any(axis=1)
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & col(
                    col("high") >= col("stop_short_line"),
                    (col("close") > col("midline")) & (col("k") > col("d")),
                    col("close") >= col("entry_short") + stoploss,
                    col("force_flat"),
                ).any(axis=1)
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "q02",
        "按 Q02 源码主逻辑实现：日内时间窗+布林/KDJ/MACD 组合开仓，叠加跳空开仓与持仓过程高低/阈值止损离场。",
    )

def build_sf01_expr() -> Expr:
    lenth = pms("sf01_lenth", 20, 600, 20).limit(20)
    trailing_stop_rate = pms("sf01_trailing_stop_rate", 1.0, 200.0, 1.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("datetime").dt.date().alias("day"),
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
            col.lit(trailing_stop_rate).alias("trailing_stop_rate"),
        )
        .with_cols(
            col("day").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("day") != col("day_1")).fill_null(True).alias("is_new_day"),
            (col("high") - col("low")).alias("range"),
        )
        .with_cols(
            col("is_new_day", "open", col.null).if_else().alias("open_d0_seed"),
            col("close_1").mean().rolling(240).alias("ma240"),
            col("range").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("open_d0_seed").ffill().expanding().alias("open_d0"),
            ((col("high") + col("low")) * 0.5).alias("median_price"),
        )
        .with_cols(
            (col("high_1") - col("close_1") + col("open_d0")).alias("up_term"),
            (col("low_1") - col("close_1") + col("open_d0")).alias("dn_term"),
            (col("median_price") > col("high_1")).fill_null(False).alias("up_median"),
            (col("median_price") < col("low_1")).fill_null(False).alias("dn_median"),
        )
        .with_cols(
            col("up_term").max().rolling(lenth).alias("up_avg"),
            col("dn_term").min().rolling(lenth).alias("dn_avg"),
        )
        .with_cols(
            (col("up_median") & (col("range") > col("range_1"))).fill_null(False).alias("upband"),
            (col("dn_median") & (col("range") > col("range_1"))).fill_null(False).alias("downband"),
        )
        .with_cols(
            col('up_avg', 'dn_avg', 'upband', 'downband').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("close_1") > col("ma240"))
                & col("upband_1")
                & (col("close_1") > col("up_avg_1"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("close_1") < col("ma240"))
                & col("downband_1")
                & (col("close_1") < col("dn_avg_1"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            col("pos_pre").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("pos_pre_1") != 1.0)).fill_null(False).alias("open_long_pre"),
            ((col("pos_pre") == -1.0) & (col("pos_pre_1") != -1.0)).fill_null(False).alias("open_short_pre"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "open_long_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
            col("open_long_pre", "open_short_pre").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "low", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("lower_after_entry"),
            col("open_short_pre", "open_long_pre", "high", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("higher_after_entry"),
        )
        .with_cols(
            (col("lower_after_entry") - col("open") * col("trailing_stop_rate") * 0.001).alias("myprice2"),
            (col("higher_after_entry") + col("open") * col("trailing_stop_rate") * 0.001).alias("myprice"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & (col("low") <= col("myprice2"))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & (col("high") >= col("myprice"))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "sf01",
        "按 SF01 源码实现：日开盘偏移区间突破 + MA240 方向过滤开仓，持仓后按 Lower/HigherAfterEntry 与 Open*TrailingStopRate 的动态线止损。",
    )

def build_sf02_expr() -> Expr:
    n = pms("sf02_n", 20, 600, 20).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").shift(1).expanding().add_suffix("1"),
            col("high", "low").ta.aroon(n).expanding(),
        )
        .with_cols(
            (col("aroonup") - col("aroondown")).alias("aroon"),
            col("close_1").ta.ema(n).expanding().alias("ma_long"),
            col('aroonup', 'aroondown').shift(1).expanding().alias('aroon_up_1', 'aroon_dn_1'),
        )
        .with_cols(
            ((col("aroonup") > 70.0) & (col("aroon_up_1") <= 70.0) & (col("aroon") > 0.0))
            .fill_null(False)
            .alias("dcond1"),
            ((col("aroondown") < 30.0) & (col("aroon_dn_1") >= 30.0) & (col("aroon") > 0.0))
            .fill_null(False)
            .alias("dcond2"),
            ((col("aroondown") > 70.0) & (col("aroon_dn_1") <= 70.0) & (col("aroon") < 0.0))
            .fill_null(False)
            .alias("kcond1"),
            ((col("aroonup") < 30.0) & (col("aroon_up_1") >= 30.0) & (col("aroon") < 0.0))
            .fill_null(False)
            .alias("kcond2"),
            ((col("aroon") > 0.0) & (col("aroonup") < 50.0) & (col("aroon_up_1") >= 50.0))
            .fill_null(False)
            .alias("pdcond1"),
            ((col("aroon") < 0.0) & (col("aroondown") < 50.0) & (col("aroon_dn_1") >= 50.0))
            .fill_null(False)
            .alias("pkcond1"),
        )
        .with_cols(
            col('dcond1', 'dcond2', 'kcond1', 'kcond2', 'pdcond1', 'pkcond1').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("close_1") > col("ma_long"))
                & col("dcond1_1", "dcond2_1").any(axis=1)
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("close_1") < col("ma_long"))
                & col("kcond1_1", "kcond2_1").any(axis=1)
            )
            .fill_null(False)
            .alias("open_short_raw"),
            col("pdcond1_1").fill_null(False).alias("exit_long_raw"),
            col("pkcond1_1").fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "sf02",
        "按 SF02 源码实现：AROON 组合交叉判势，叠加长周期均线过滤开仓，并以 AROON 回落阈值触发平仓。",
    )

def build_sf03_expr() -> Expr:
    s1 = pms("sf03_s1", 5, 300, 5).limit(20)
    s2 = pms("sf03_s2", 20, 2400, 20).limit(20)
    st = pms("sf03_st", 0.1, 20.0, 0.1).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
            col("close").count().expanding().alias("bar_no"),
            col.lit(s2).alias("s2"),
        )
        .with_cols(
            col("close_1").mean().rolling(s2).alias("ma1"),
        )
        .with_cols(
            col("ma1").mean().rolling(s1).alias("ma2"),
            col((col("high") + col("low")) <= (col("high_1") + col("low_1")), 0.0, col(
                    (col("high") - col("high_1")).abs(),
                    (col("low") - col("low_1")).abs(),
                ).max(axis=1)).if_else()
            .alias("dbf"),
            col((col("high") + col("low")) >= (col("high_1") + col("low_1")), 0.0, col(
                    (col("high") - col("high_1")).abs(),
                    (col("low") - col("low_1")).abs(),
                ).max(axis=1)).if_else()
            .alias("kbf"),
            col.lit(s1).alias("s1"),
        )
        .with_cols(
            ((col("dbf") + col("s1")) / col("dbf", "kbf", 2.0 * col("s1")).sum(axis=1)).alias("dbl"),
            ((col("kbf") + col("s1")) / col("dbf", "kbf", 2.0 * col("s1")).sum(axis=1)).alias("kbl"),
        )
        .with_cols(
            (col("dbl") - col("kbl")).alias("change"),
        )
        .with_cols(
            col("change").mean().rolling(s1).alias("machange"),
        )
        .with_cols(
            col("machange").ta.ema(s1).expanding().alias("machange2"),
        )
        .with_cols(
            (
                (col("bar_no") > col("s2"))
                & (col("close") > col("ma1"))
                & (col("ma1") > col("ma2"))
                & (col("change") > 0.0)
                & (col("machange") > col("machange2"))
            )
            .fill_null(False)
            .alias("buyk"),
            (
                (col("bar_no") > col("s2"))
                & (col("close") < col("ma1"))
                & (col("ma1") < col("ma2"))
                & (col("change") < 0.0)
                & (col("machange") < col("machange2"))
            )
            .fill_null(False)
            .alias("sellk"),
        )
        .with_cols(
            col('buyk', 'sellk').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("buyk_1").fill_null(False).alias("open_long_raw"),
            col("sellk_1").fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            col('pos_pre', 'ma1').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("pos_pre_1") != 1.0)).fill_null(False).alias("open_long_pre"),
            ((col("pos_pre") == -1.0) & (col("pos_pre_1") != -1.0)).fill_null(False).alias("open_short_pre"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "open_long_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
            col("open_long_pre", "open_short_pre").count_last().expanding().alias("bars_long", "bars_short"),
            col.lit(st).alias("st"),
        )
        .with_cols(
            ((col("close") < col("ma1")) & (col("close") > (col("entry_long") * (1.0 + col("st") * 0.01))))
            .fill_null(False)
            .alias("selly"),
            ((col("close") > col("ma1")) & (col("close") < (col("entry_short") * (1.0 - col("st") * 0.01))))
            .fill_null(False)
            .alias("buyy"),
        )
        .with_cols(
            col('selly', 'buyy').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 1)
                & col("selly_1")
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 1)
                & col("buyy_1")
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "sf03",
        "按 SF03 源码实现：双层均线与波动变化率共振开仓，持仓后以 MA1 与入场收益阈值组合触发止盈平仓。",
    )

def build_sf04_expr() -> Expr:
    length1 = pms("sf04_length1", 2, 240, 20).limit(20)
    length2 = pms("sf04_length2", 2, 360, 180).limit(20)
    x = pms("sf04_x", 0.5, 40.0, 13.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("datetime").dt.date().alias("day"),
            col.lit(x).alias("x"),
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
            col("high").shift(length1).expanding().alias("high_len1"),
            col("low").shift(length1).expanding().alias("low_len1"),
            col("high").shift(length2).expanding().alias("high_len2"),
            col("low").shift(length2).expanding().alias("low_len2"),
        )
        .with_cols(
            col("day").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("day") != col("day_1")).fill_null(True).alias("is_new_day"),
            (length1 <= length2).fill_null(True).alias("len1_is_short"),
            col("close_1").mean().rolling(length1).alias("ma_len1"),
            col("close_1").mean().rolling(length2).alias("ma_len2"),
            col("high", "low", "close").ta.atr(length1).expanding().alias("atr"),
        )
        .with_cols(
            col("is_new_day", "open", col.null).if_else().alias("open_d0_seed"),
            col("len1_is_short", "ma_len1", "ma_len2").if_else().alias("ma1"),
            col("len1_is_short", "ma_len2", "ma_len1").if_else().alias("ma2"),
            col("len1_is_short", "high_len1", "high_len2").if_else().alias("high_l1"),
            col("len1_is_short", "low_len1", "low_len2").if_else().alias("low_l1"),
            col("len1_is_short", "low_len2", "low_len1").if_else().alias("exitlong"),
            col("len1_is_short", "high_len2", "high_len1").if_else().alias("exitshort"),
        )
        .with_cols(
            col("open_d0_seed").ffill().expanding().alias("open_d0"),
        )
        .with_cols(
            (col("open_d0") + col("x") * col("atr")).alias("upperband"),
            (col("open_d0") - col("x") * col("atr")).alias("lowerband"),
        )
        .with_cols(
            col('upperband', 'lowerband').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("high") >= col("upperband_1"))
                & (col("ma1") > col("ma2"))
                & (col("high") >= col("high_l1"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("low") <= col("lowerband_1"))
                & (col("ma1") < col("ma2"))
                & (col("low") <= col("low_l1"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
            col("open", "upperband_1", "high_len1").max(axis=1).alias("entry_long_now"),
            col("open", "lowerband_1", "low_len1").min(axis=1).alias("entry_short_now"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            col("pos_pre").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("pos_pre_1") != 1.0)).fill_null(False).alias("open_long_pre"),
            ((col("pos_pre") == -1.0) & (col("pos_pre_1") != -1.0)).fill_null(False).alias("open_short_pre"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "entry_long_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "open_long_pre", "entry_short_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
            col("open_long_pre", "open_short_pre").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & (col("low") >= col("entry_long"))
                & (col("low") <= col("exitlong"))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & (col("high") <= col("entry_short"))
                & (col("high") >= col("exitshort"))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "sf04",
        "按 SF04 源码实现：OpenD±X*ATR 通道突破并以 MA 双周期同向过滤开仓，持仓后按 L[L2]/H[L2] 与保护价组合触发离场。",
    )

def build_sf05_expr() -> Expr:
    lenth = pms("sf05_lenth", 10, 400, 10).limit(20)
    atrs = pms("sf05_atrs", 1.0, 200.0, 1.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col.lit(atrs).alias("atrs"),
            (((col("high") + col("low") + col("close") * 2.0) / 4.0)).alias("avgp"),
            col("high", "low", "close").ta.atr(lenth).expanding().alias("atr_val"),
        )
        .with_cols(
            ((col("avgp") * 2.0) - col("low")).alias("rs_src"),
            ((col("avgp") * 2.0) - col("high")).alias("st_src"),
        )
        .with_cols(
            col("rs_src").max().rolling(lenth).alias("rs"),
            col("st_src").min().rolling(lenth).alias("st"),
        )
        .with_cols(
            col("rs", "st").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("high") >= col("rs_1")).fill_null(False).alias("open_long_raw"),
            (col("low") <= col("st_1")).fill_null(False).alias("open_short_raw"),
            col("open", "rs_1").max(axis=1).alias("entry_long_now"),
            col("open", "st_1").min(axis=1).alias("entry_short_now"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            col("pos_pre").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("pos_pre_1") != 1.0)).fill_null(False).alias("open_long_pre"),
            ((col("pos_pre") == -1.0) & (col("pos_pre_1") != -1.0)).fill_null(False).alias("open_short_pre"),
        )
        .with_cols(
            (col("entry_long_now") + col("atr_val") * col("atrs")).alias("tp_long_now"),
            (col("entry_short_now") - col("atr_val") * col("atrs")).alias("tp_short_now"),
            col("open_long_pre", "open_short_pre").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "tp_long_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("tp_long"),
            col("open_short_pre", "open_long_pre", "tp_short_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("tp_short"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & (col("high") >= col("tp_long"))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & (col("low") <= col("tp_short"))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "sf05",
        "按 SF05 源码实现：加权价格构建支撑/阻力突破入场，持仓后使用“入场时 ATR*ATRs 固定目标位”止盈离场。",
    )

def build_sf06_expr() -> Expr:
    fast = pms("sf06_fast", 5, 240, 95).limit(20)
    rl = pms("sf06_rl", 1.0, 10.0, 3.0).limit(20)
    rs = pms("sf06_rs", 1.0, 10.0, 4.0).limit(20)
    trs = pms("sf06_trs", 1.0, 100.0, 30.0).limit(20)
    slow_l = (fast * rl).floor()
    slow_s = (fast * rs).floor()
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col.lit(trs).alias("trs"),
            col("close").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("high").mean().rolling(fast).alias("avg_l1"),
            col("low").mean().rolling(fast).alias("avg_s1"),
        )
        .with_cols(
            col("high").mean().rolling(slow_l).alias("avg_l2"),
            col("low").mean().rolling(slow_s).alias("avg_s2"),
            col("close").shift(fast).expanding().alias("close_fast"),
            col("high").count().expanding().alias("bar_no"),
        )
        .with_cols(
            (
                (col("close") > col("avg_l1"))
                & (col("close") > col("avg_l2"))
                & (col("close") > col("close_fast"))
            )
            .fill_null(False)
            .alias("condition1"),
            (
                (col("close") < col("avg_s1"))
                & (col("close") < col("avg_s2"))
                & (col("close") < col("close_fast"))
            )
            .fill_null(False)
            .alias("condition2"),
            (
                (col("close_1") > col("avg_l1"))
                | (col("close_1") > col("avg_l2"))
            )
            .fill_null(False)
            .alias("condition3"),
            (
                (col("close_1") < col("avg_s1"))
                | (col("close_1") < col("avg_s2"))
            )
            .fill_null(False)
            .alias("condition4"),
        )
        .with_cols(
            col('avg_l1', 'avg_l2', 'avg_s1', 'avg_s2', 'condition1', 'condition2', 'condition3', 'condition4').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("avg_l1_1") > col("avg_l2_1"))
                & col("condition1_1")
                & col("condition3_1")
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("avg_s1_1") < col("avg_s2_1"))
                & col("condition2_1")
                & col("condition4_1")
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            col("pos_pre").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("pos_pre_1") != 1.0)).fill_null(False).alias("open_long_pre"),
            ((col("pos_pre") == -1.0) & (col("pos_pre_1") != -1.0)).fill_null(False).alias("open_short_pre"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "open_long_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
            col("open_long_pre", "open_short_pre").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "low", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("lower_after_entry"),
            col("open_short_pre", "open_long_pre", "high", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("higher_after_entry"),
        )
        .with_cols(
            (col("lower_after_entry") - col("open") * col("trs") * 0.001).alias("myprice2"),
            (col("higher_after_entry") + col("open") * col("trs") * 0.001).alias("myprice3"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & (col("low") <= col("myprice2"))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & (col("high") >= col("myprice3"))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "sf06",
        "按 SF06 源码实现：双均线与 Close[Fast] 方向过滤开仓，持仓后以 Higher/LowerAfterEntry ± Open*TRS 的动态线止损。",
    )

def build_sf07_expr() -> Expr:
    n = pms("sf07_n", 2, 240, 2).limit(20)
    k1 = pms("sf07_k1", 0.1, 20.0, 0.1).limit(20)
    trs = pms("sf07_trs", 1.0, 100.0, 1.0).limit(20)
    exit_on_close_mins = pms("sf07_exit_on_close_mins", 9.00, 23.59, 9.00).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("datetime").dt.date().alias("day"),
            (col("datetime").dt.hour() * 100 + col("datetime").dt.minute()).alias("hhmm"),
            col(
                col("datetime").count().alias("i_bar"),
                col("open").first_value().alias("open_d0"),
            ).expanding().over(col("datetime").dt.date()),
            col.lit(n).cast(pl.UInt32).alias("n"),
            col.lit(k1).alias("k1"),
            col.lit(trs).alias("trs"),
            col.lit(exit_on_close_mins).alias("exit_hhmm"),
            col('high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(col(
            col("high").max().alias("hha"),
            col("low").min().alias("lla"),
            col("close").max().alias("hca"),
            col("close").min().alias("lca"),
        ).expanding().over(col("day")))
        .with_cols(
            col(col("i_bar") == col("n"), col("hha"), col.null).if_else().alias("hh_seed"),
            col(col("i_bar") == col("n"), col("lla"), col.null).if_else().alias("ll_seed"),
            col(col("i_bar") == col("n"), col("hca"), col.null).if_else().alias("hc_seed"),
            col(col("i_bar") == col("n"), col("lca"), col.null).if_else().alias("lc_seed"),
        )
        .with_cols(
            col("hh_seed", "ll_seed", "hc_seed", "lc_seed")
            .ffill()
            .alias("hh", "ll", "hc", "lc")
            .expanding()
            .over(col("day"))
        )
        .with_cols(
            ((col("hh") - col("lc")) >= (col("hc") - col("ll")))
            .fill_null(False)
            .alias("rng_flag"),
        )
        .with_cols(
            col("rng_flag", col("hh") - col("lc"), col("hh") - col("ll")).if_else()
            .alias("buyrange"),
            col("rng_flag", col("hh") - col("ll"), col("hh") - col("lc")).if_else()
            .alias("sellrange"),
        )
        .with_cols(
            (col("buyrange") * col("k1") * 0.001).alias("buytrig"),
            (col("sellrange") * col("k1") * 0.001).alias("selltrig"),
            (col("hhmm") < col("exit_hhmm") * 100.0).fill_null(False).alias("time_ok"),
        )
        .with_cols(
            (col("open_d0", "hh").max(axis=1) + col("buytrig")).alias("buyposition"),
            (col("open_d0", "ll").min(axis=1) - col("selltrig")).alias("sellposition"),
        )
        .with_cols(
            (
                col("time_ok")
                & (col("i_bar") > col("n"))
                & (col("high") >= col("buyposition"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                col("time_ok")
                & (col("i_bar") > col("n"))
                & (col("low") <= col("sellposition"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            col("pos_pre").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("pos_pre_1") != 1.0)).fill_null(False).alias("open_long_pre"),
            ((col("pos_pre") == -1.0) & (col("pos_pre_1") != -1.0)).fill_null(False).alias("open_short_pre"),
            (col("hhmm") >= col("exit_hhmm") * 100.0).fill_null(False).alias("eod_exit"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "open_long_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
            col("open_long_pre", "open_short_pre").count_last().expanding().alias("bars_long", "bars_short"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "low", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("low_after_entry"),
            col("open_short_pre", "open_long_pre", "high", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("high_after_entry"),
        )
        .with_cols(
            (col("low_after_entry") - col("open") * col("trs") * 0.001).alias("myprice2"),
            (col("high_after_entry") + col("open") * col("trs") * 0.001).alias("myprice3"),
        )
        .with_cols(
            (
                ((col("pos_pre") == 1.0) & (col("bars_long") > 0) & (col("low") <= col("myprice2")))
                | ((col("pos_pre") == 1.0) & col("eod_exit"))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                ((col("pos_pre") == -1.0) & (col("bars_short") > 0) & (col("high") >= col("myprice3")))
                | ((col("pos_pre") == -1.0) & col("eod_exit"))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "sf07",
        "按 SF07 源码主逻辑实现：基于日内前 N 根区间构建突破轨道开仓，持仓后用 Lower/HighAfterEntry ± Open*TRS 的动态线止损，并在收盘前平仓。",
    )

def build_sf08_expr() -> Expr:
    length = pms("sf08_length", 5, 240, 40).limit(20)
    m = pms("sf08_m", 2, 200, 20).limit(20)
    ts = pms("sf08_ts", 1.0, 120.0, 35.0).limit(20)
    length3 = length * 3
    slow = ((length / 3.0) + 0.5).floor()
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("bar_no"),
            col("close", "high", "low").shift(1).expanding().add_suffix("1"),
            col("high", "low").shift(2).expanding().add_suffix("2"),
            col.lit(ts).alias("ts"),
            col.lit(m).cast(pl.UInt32).alias("m_u"),
        )
        .with_cols(
            col("high").max().rolling(length).alias("highest_value"),
            col("low").min().rolling(length).alias("lowest_value"),
        )
        .with_cols(
            col("close_1").mean().rolling(length3).alias("ma1"),
        )
        .with_cols(
            (col("highest_value") - col("lowest_value")).alias("hl_span"),
            (col("close") - col("lowest_value")).alias("cl_span"),
        )
        .with_cols(
            col("hl_span", "cl_span").sum().rolling(slow).alias("sum_hl", "sum_cl"),
        )
        .with_cols(
            col(col("sum_hl") == 0.0, 0.0, col("sum_cl") / col("sum_hl") * 100.0).if_else().alias("k_value"),
        )
        .with_cols(
            col("k_value").mean().rolling(slow).alias("d_value"),
        )
        .with_cols(
            col('k_value', 'd_value').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("k_value") > col("d_value")) & (col("k_value_1") <= col("d_value_1"))).fill_null(False).alias("longdk"),
            ((col("k_value") < col("d_value")) & (col("k_value_1") >= col("d_value_1"))).fill_null(False).alias("shortdk"),
        )
        .with_cols(
            col("longdk", "shortdk").shift(1).expanding().add_suffix("1"),
            col("longdk", "shortdk").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            col("longdk_1", "bar_no", col.null).if_else().alias("bar_long_seed"),
            col("shortdk_1", "bar_no", col.null).if_else().alias("bar_short_seed"),
        )
        .with_cols(
            col("bar_long_seed").ffill().expanding().alias("bar_long"),
            col("bar_short_seed").ffill().expanding().alias("bar_short"),
        )
        .with_cols(
            col(
                (col("bar_long").cast(pl.Int64) - col("bar_short").cast(pl.Int64)).cast(pl.Float64),
                col("m_u").cast(pl.Float64),
            ).max(axis=1)
            .fill_null(col("m_u").cast(pl.Float64))
            .cast(pl.UInt32)
            .alias("win_h"),
            col(
                (col("bar_short").cast(pl.Int64) - col("bar_long").cast(pl.Int64)).cast(pl.Float64),
                col("m_u").cast(pl.Float64),
            ).max(axis=1)
            .fill_null(col("m_u").cast(pl.Float64))
            .cast(pl.UInt32)
            .alias("win_l"),
        )
        .with_cols(
            col("high_1").max().rolling_dynamic("win_h", window_max=4096, min_samples=1).alias("highup"),
            col("low_1").min().rolling_dynamic("win_l", window_max=4096, min_samples=1).alias("lowdown"),
        )
        .with_cols(
            col("longdk_1", "highup", col.null).if_else().alias("hh_seed"),
            col("shortdk_1", "lowdown", col.null).if_else().alias("ll_seed"),
            _state_seed_expr("longdk_2", "shortdk_2", alias="kg_seed"),
        )
        .with_cols(
            col("hh_seed").ffill().expanding().alias("hh"),
            col("ll_seed").ffill().expanding().alias("ll"),
            col("kg_seed").ffill().expanding().alias("kg_raw"),
        )
        .with_cols(
            col("kg_raw").fill_null(0.0).alias("kg"),
        )
        .with_cols(
            col('hh', 'll').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("kg") > 0.0)
                & (col("high") >= col("hh_1"))
                & (col("high_1") < col("hh_1"))
                & (col("hh_1") > 0.0)
                & (col("close_1") > col("ma1"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("kg") < 0.0)
                & (col("low") <= col("ll_1"))
                & (col("low_1") > col("ll_1"))
                & (col("ll_1") > 0.0)
                & (col("close_1") < col("ma1"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            col("pos_pre").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("pos_pre_1") != 1.0)).fill_null(False).alias("open_long_pre"),
            ((col("pos_pre") == -1.0) & (col("pos_pre_1") != -1.0)).fill_null(False).alias("open_short_pre"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "open_long_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
        )
        .with_cols(
            _bars_pos_from_open_sig_expr("open_long_pre", "open_short_pre", "bars_pos"),
            col("open_long_pre", "open_short_pre", "low", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("low_after_entry"),
            col("open_short_pre", "open_long_pre", "high", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("high_after_entry"),
        )
        .with_cols(
            col(
                col("pos_pre") == 0.0,
                1.0,
                col(
                    1.0 - (col("bars_pos").cast(pl.Float64) + 1.0) * 0.1,
                    0.5,
                ).max(axis=1),
            )
            .if_else()
            .alias("liqka"),
        )
        .with_cols(
            (col("low_after_entry") - (col("open") * col("ts") * 0.001) * col("liqka")).alias("dliq"),
            (col("high_after_entry") + (col("open") * col("ts") * 0.001) * col("liqka")).alias("kliq"),
        )
        .with_cols(
            col("dliq", "kliq").shift(1).expanding().add_suffix("1"),
            col("dliq", "kliq").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("low_1") <= col("dliq_1"))
                & (col("low_2") >= col("dliq_2"))
                & (col("dliq_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("high_1") >= col("kliq_1"))
                & (col("high_2") <= col("kliq_2"))
                & (col("kliq_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "sf08",
        "按 SF08 源码实现：KD 交叉后构建区间突破开仓，叠加长周期均线过滤，持仓后使用 liQKA 自适应吊灯线离场。",
    )

def build_sf09_expr() -> Expr:
    length = pms("sf09_length", 5, 240, 5).limit(20)
    x = pms("sf09_x", 2, 80, 2).limit(20)
    n = pms("sf09_n", 1.0, 20.0, 1.0).limit(20)
    ts = pms("sf09_ts", 1.0, 120.0, 1.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
            col.lit(ts).alias("ts"),
            col.lit(n).alias("n"),
        )
        .with_cols(
            (col("close") - col("close_1")).alias("price_a"),
            (col("volume") * (col("high") - col("low"))).alias("new_var"),
        )
        .with_cols(
            col(col("price_a") > 0.0, col("new_var"), 0.0).if_else().alias("buy_flow"),
            col(col("price_a") < 0.0, -col("new_var"), 0.0).if_else().alias("sell_flow"),
        )
        .with_cols(
            col("buy_flow", "sell_flow").sum().rolling(length).alias("buy_cout", "sell_cout"),
            col("high").max().rolling(length).alias("hh"),
            col("low").min().rolling(length).alias("ll"),
        )
        .with_cols(
            col(col("sell_cout") == 0.0, col.null, col("buy_cout") / col("sell_cout").abs()).if_else().alias("buysellcond"),
        )
        .with_cols(
            col("buysellcond").max().rolling(x).alias("cout_d"),
            col("buysellcond").min().rolling(x).alias("cout_k"),
            col('hh', 'll').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col('cout_d', 'cout_k').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("buysellcond") > col("cout_d_1")).fill_null(False).alias("buycross"),
            (col("buysellcond") < col("cout_k_1")).fill_null(False).alias("sellcross"),
        )
        .with_cols(
            col('buycross', 'sellcross').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("buycross_1") & (col("high") >= col("hh_1"))).fill_null(False).alias("open_long_raw"),
            (col("sellcross_1") & (col("low") <= col("ll_1"))).fill_null(False).alias("open_short_raw"),
            col("open", "hh_1").max(axis=1).alias("entry_long_now"),
            col("open", "ll_1").min(axis=1).alias("entry_short_now"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            col("pos_pre").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("pos_pre_1") != 1.0)).fill_null(False).alias("open_long_pre"),
            ((col("pos_pre") == -1.0) & (col("pos_pre_1") != -1.0)).fill_null(False).alias("open_short_pre"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "entry_long_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "open_long_pre", "entry_short_now")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
        )
        .with_cols(
            _bars_pos_from_open_sig_expr("open_long_pre", "open_short_pre", "bars_pos"),
            col("open_long_pre", "open_short_pre", "high", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("bkstop"),
            col("open_short_pre", "open_long_pre", "low", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("skstop"),
            col("open_long_pre", "open_short_pre", "low", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("low_after_entry"),
            col("open_short_pre", "open_long_pre", "high", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("high_after_entry"),
        )
        .with_cols(
            (
                (col("high") - col("entry_long") > col("entry_long") * 0.05)
                & (col("bkstop") - col("close") > col("bkstop") * (0.01 * col("n")))
            )
            .fill_null(False)
            .alias("bkstop_price"),
            (
                (col("entry_short") - col("low") > col("entry_short") * 0.05)
                & (col("close") - col("skstop") > col("skstop") * (0.01 * col("n")))
            )
            .fill_null(False)
            .alias("skstop_price"),
        )
        .with_cols(
            col('bkstop_price', 'skstop_price').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col(col("pos_pre") == 0.0, 1.0, col(
                    1.0 - (col("bars_pos").cast(pl.Float64) + 1.0) * 0.1,
                    0.3,
                ).max(axis=1)).if_else()
            .alias("liqka"),
        )
        .with_cols(
            (col("low_after_entry") - (col("open") * col("ts") * 0.001) * col("liqka")).alias("dliq"),
            (col("high_after_entry") + (col("open") * col("ts") * 0.001) * col("liqka")).alias("kliq"),
        )
        .with_cols(
            col('dliq', 'kliq').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                ((col("pos_pre") == 1.0) & (col("bars_pos") > 0) & col("bkstop_price_1"))
                | ((col("pos_pre") == 1.0) & (col("bars_pos") > 0) & (col("low") <= col("dliq_1")) & (col("dliq_1") > 0.0))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                ((col("pos_pre") == -1.0) & (col("bars_pos") > 0) & col("skstop_price_1"))
                | ((col("pos_pre") == -1.0) & (col("bars_pos") > 0) & (col("high") >= col("kliq_1")) & (col("kliq_1") > 0.0))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "sf09",
        "按 SF09 源码主逻辑实现：基于多空资金流比值突破与区间突破开仓，叠加利润回撤与 liQKA 吊灯止损双重离场。",
    )

def build_sf10_expr() -> Expr:
    k = pms("sf10_k", 0.1, 5.0, 0.4).limit(20)
    n = pms("sf10_n", 0.1, 10.0, 1.5).limit(20)
    startpro1 = pms("sf10_startpro1", 0.1, 20.0, 1.0).limit(20)
    stoppro1 = pms("sf10_stoppro1", 1.0, 120.0, 80.0).limit(20)
    startpro2 = pms("sf10_startpro2", 0.1, 30.0, 3.0).limit(20)
    stoppro2 = pms("sf10_stoppro2", 1.0, 120.0, 30.0).limit(20)
    startpro3 = pms("sf10_startpro3", 0.1, 40.0, 5.0).limit(20)
    stoppro3 = pms("sf10_stoppro3", 1.0, 120.0, 20.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("datetime").dt.date().alias("day"),
            col(
                col("datetime").count().alias("bars_in_day"),
                col("high").max().alias("day_high_cum"),
                col("low").min().alias("day_low_cum"),
                col("open").first_value().alias("open_d0"),
            ).expanding().over(col("datetime").dt.date()),
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
            col.lit(k).alias("k"),
            col.lit(n).alias("n"),
        )
        .with_cols(
            col('day', 'day_high_cum', 'day_low_cum').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("day") != col("day_1")).fill_null(True).alias("is_new_day"),
        )
        .with_cols(
            col("is_new_day", "day_high_cum_1", col.null).if_else().alias("yhigh_seed"),
            col("is_new_day", "day_low_cum_1", col.null).if_else().alias("ylow_seed"),
            col("is_new_day", "close_1", col.null).if_else().alias("yclose_seed"),
        )
        .with_cols(
            col("yhigh_seed", "ylow_seed", "yclose_seed")
            .ffill()
            .alias("yhigh", "ylow", "yclose")
            .expanding()
            .over(col("day"))
        )
        .with_cols(
            col(col("yhigh") - col("yclose"), col("yclose") - col("ylow")).max(axis=1).alias("band"),
        )
        .with_cols(
            (col("open_d0") + col("band") * col("k")).alias("band_up"),
            (col("open_d0") - col("band") * col("k")).alias("band_dn"),
        )
        .with_cols(
            col("is_new_day", "band_up", col.null).if_else().alias("band_up_day_seed"),
            col("is_new_day", "band_dn", col.null).if_else().alias("band_dn_day_seed"),
        )
        .with_cols(
            col("band_up_day_seed").ffill().expanding().alias("band_up_day"),
            col("band_dn_day_seed").ffill().expanding().alias("band_dn_day"),
            col("band_up_day_seed", "band_dn_day_seed")
            .ffill()
            .shift(1)
            .expanding()
            .alias("r_band_up", "r_band_dn"),
        )
        .with_cols(
            (col("band_up_day") - col("band_dn_day")).alias("hl_band"),
            (col("r_band_up") - col("r_band_dn")).alias("rhl_band"),
        )
        .with_cols(
            (
                (col("close") >= col("band_up"))
                & (col("low_1") < col("band_up"))
                & (col("hl_band") > col("rhl_band"))
            )
            .fill_null(False)
            .alias("dk"),
            (
                (col("close") <= col("band_dn"))
                & (col("high_1") > col("band_dn"))
                & (col("hl_band") > col("rhl_band"))
            )
            .fill_null(False)
            .alias("kk"),
        )
        .with_cols(
            col("dk").fill_null(False).alias("open_long_raw"),
            col("kk").fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            col("pos_pre").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("pos_pre_1") != 1.0)).fill_null(False).alias("open_long_pre"),
            ((col("pos_pre") == -1.0) & (col("pos_pre_1") != -1.0)).fill_null(False).alias("open_short_pre"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "close")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("lastprice_long"),
            col("open_short_pre", "open_long_pre", "close")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("lastprice_short"),
            _bars_pos_from_open_sig_expr("open_long_pre", "open_short_pre", "bars_pos"),
            col("open_long_pre", "open_short_pre", "high", "close")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("high_after_entry"),
            col("open_short_pre", "open_long_pre", "low", "close")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("low_after_entry"),
        )
        .with_cols(
            col("high_after_entry", "low_after_entry").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("close") <= (col("lastprice_long") - col("hl_band") * col("n")))
            )
            .fill_null(False)
            .alias("exit_long_pdk"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("close") >= (col("lastprice_short") + col("hl_band") * col("n")))
            )
            .fill_null(False)
            .alias("exit_short_pkk"),
        )
        .with_cols(
            col("open_long_pre", "close", "low")
            .stra
            .exit_by_profit_drawdown(startpro3, stoppro3)
            .expanding()
            .fill_null(False)
            .alias("exit_long_stage3"),
            col("open_short_pre", "close", "high")
            .stra
            .exit_by_profit_drawdown(startpro3, stoppro3, True)
            .expanding()
            .fill_null(False)
            .alias("exit_short_stage3"),
            col("open_long_pre", "close", "low")
            .stra
            .exit_by_profit_drawdown(startpro2, stoppro2)
            .expanding()
            .fill_null(False)
            .alias("exit_long_stage2"),
            col("open_short_pre", "close", "high")
            .stra
            .exit_by_profit_drawdown(startpro2, stoppro2, True)
            .expanding()
            .fill_null(False)
            .alias("exit_short_stage2"),
            col("open_long_pre", "close", "low")
            .stra
            .exit_by_profit_drawdown(startpro1, stoppro2)
            .expanding()
            .fill_null(False)
            .alias("exit_long_stage1"),
            col("open_short_pre", "close", "high")
            .stra
            .exit_by_profit_drawdown(startpro1, stoppro1, True)
            .expanding()
            .fill_null(False)
            .alias("exit_short_stage1"),
        )
        .with_cols(
            col("exit_long_pdk", "exit_long_stage1", "exit_long_stage2", "exit_long_stage3").any(axis=1)
            .fill_null(False)
            .alias("exit_long_raw"),
            col("exit_short_pkk", "exit_short_stage1", "exit_short_stage2", "exit_short_stage3").any(axis=1)
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "sf10",
        "按 SF10 源码主逻辑实现：基于昨日区间构建日内 Band 突破开仓，随后以 LastPrice±HLBand*N 与分段回撤止盈组合离场。",
    )

def build_sf11_expr() -> Expr:
    x = pms("sf11_x", 2, 120, 2).limit(20)
    tr = pms("sf11_tr", 5, 300, 5).limit(20)
    ts = pms("sf11_ts", 1.0, 120.0, 1.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("bar_no"),
            col.lit(x).cast(pl.UInt32).alias("x_u"),
            col.lit(ts).alias("ts"),
            col("close", "high", "low").shift(1).expanding().add_suffix("1"),
            col("high", "low").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            (col("bar_no").cast(pl.UInt32) % col.lit(tr).cast(pl.UInt32) == 0).fill_null(False).alias("recalc"),
            col("volume").mean().rolling(x).alias("vol_mid"),
        )
        .with_cols(
            col(col("volume") >= col("vol_mid"), col("high_1"), col.null).if_else().alias("high_va"),
            col(col("volume") >= col("vol_mid"), col("low_1"), col.null).if_else().alias("low_va"),
        )
        .with_cols(
            col("high_va").max().rolling(x).alias("hhv_raw"),
            col("low_va").min().rolling(x).alias("llv_raw"),
        )
        .with_cols(
            col("recalc", col("hhv_raw", "high").max(axis=1), col.null).if_else()
            .alias("band_up_seed"),
            col("recalc", col("llv_raw", "low").min(axis=1), col.null).if_else()
            .alias("band_dn_seed"),
        )
        .with_cols(
            col("band_up_seed").ffill().expanding().alias("band_up"),
            col("band_dn_seed").ffill().expanding().alias("band_dn"),
        )
        .with_cols(
            ((col("band_up") + col("band_dn")) * 0.5).alias("hl"),
            col('band_up', 'band_dn').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("close") > col("band_up"))
                & (col("close_1") <= col("band_up_1"))
                & (col("band_up") == col("band_up_1"))
            )
            .fill_null(False)
            .alias("cond1"),
            (
                (col("close") < col("band_dn"))
                & (col("close_1") >= col("band_dn_1"))
                & (col("band_dn") == col("band_dn_1"))
            )
            .fill_null(False)
            .alias("cond2"),
        )
        .with_cols(
            col('cond1', 'cond2').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("cond1_1").fill_null(False).alias("open_long_raw"),
            col("cond2_1").fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            col("pos_pre").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("pos_pre_1") != 1.0)).fill_null(False).alias("open_long_pre"),
            ((col("pos_pre") == -1.0) & (col("pos_pre_1") != -1.0)).fill_null(False).alias("open_short_pre"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "hl")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("pprice"),
            _bars_pos_from_open_sig_expr("open_long_pre", "open_short_pre", "bars_pos"),
            col("open_long_pre", "open_short_pre", "low", "hl")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("low_after_entry"),
            col("open_short_pre", "open_long_pre", "high", "hl")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("high_after_entry"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("bars_pos") > 0) & (col("low") <= col("pprice")))
            .fill_null(False)
            .alias("exit_long_band"),
            ((col("pos_pre") == -1.0) & (col("bars_pos") > 0) & (col("high") >= col("pprice")))
            .fill_null(False)
            .alias("exit_short_band"),
        )
        .with_cols(
            col(col("pos_pre") == 0.0, 1.0, col(
                    1.0 - (col("bars_pos").cast(pl.Float64) + 1.0) * 0.1,
                    0.3,
                ).max(axis=1)).if_else()
            .alias("liqka"),
        )
        .with_cols(
            (col("low_after_entry") - (col("open") * col("ts") * 0.001) * col("liqka")).alias("dliq"),
            (col("high_after_entry") + (col("open") * col("ts") * 0.001) * col("liqka")).alias("kliq"),
        )
        .with_cols(
            col('dliq', 'kliq').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                col("exit_long_band")
                | (
                    (col("pos_pre") == 1.0)
                    & (col("bars_pos") > 0)
                    & (col("low") <= col("dliq_1"))
                    & (col("dliq_1") > 0.0)
                )
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                col("exit_short_band")
                | (
                    (col("pos_pre") == -1.0)
                    & (col("bars_pos") > 0)
                    & (col("high") >= col("kliq_1"))
                    & (col("kliq_1") > 0.0)
                )
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "sf11",
        "按 SF11 源码主逻辑实现：基于成交量活跃区间形成的上下轨突破开仓，并以中轴价+liQKA 吊灯线联合离场。",
    )

def build_sf12_expr() -> Expr:
    r1 = pms("sf12_r1", 5, 240, 5).limit(20)
    x = pms("sf12_x", 0.1, 5.0, 0.1).limit(20)
    trs = pms("sf12_trs", 1.0, 120.0, 1.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col.lit(x).alias("x"),
            col.lit(trs).alias("trs"),
            col("close", "high", "low").shift(1).expanding().add_suffix("1"),
            col("high", "low").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            col("high_1").max().rolling(r1).alias("hh"),
            col("low_1").min().rolling(r1).alias("ll"),
            col("high_1").max().rolling((r1 * 2.0), (r1 * 2.0)).alias("hh2"),
            col("low_1").min().rolling((r1 * 2.0), (r1 * 2.0)).alias("ll2"),
            col("high_1", "low_1").sum().rolling(r1).alias("sum_high_r1", "sum_low_r1"),
            col("high_1", "low_1").sum().rolling((r1 * 2.0), (r1 * 2.0)).alias("sum_high_r1_2", "sum_low_r1_2"),
        )
        .with_cols(
            (
                r1.cast(pl.Float64) * (col("hh") - col("ll"))
                - col("sum_high_r1")
                + col("sum_low_r1")
            )
            .alias("ntd1"),
            (
                (col.lit(r1) * 2.0).cast(pl.UInt32).cast(pl.Float64) * (col("hh2") - col("ll2"))
                - col("sum_high_r1_2")
                + col("sum_low_r1_2")
            )
            .alias("ntd2"),
        )
        .with_cols(
            (col("ntd1") / col("close")).alias("ntd1_ratio"),
            (col("ntd2") / col("close")).alias("ntd2_ratio"),
        )
        .with_cols(
            col("ntd1_ratio", "ntd2_ratio").sum().rolling(r1).alias("ntd1_sum", "ntd2_sum"),
        )
        .with_cols(
            col(col("ntd1_sum") == 0.0, 0.0, col("ntd1_ratio") / col("ntd1_sum")).if_else().alias("n1"),
            col(col("ntd2_sum") == 0.0, 0.0, col("ntd2_ratio") / col("ntd2_sum")).if_else().alias("n2"),
            ((col("hh") + col("ll")) * 0.5).alias("hl_mid"),
            (col("ntd1") / r1.cast(pl.Float64)).alias("avg_ntd"),
        )
        .with_cols(
            (col("n1") / col("n2") > 1.0).fill_null(False).alias("condition_filter"),
            (col("hl_mid") + col("avg_ntd") * col("x")).alias("band_up"),
            (col("hl_mid") - col("avg_ntd") * col("x")).alias("band_dn"),
        )
        .with_cols(
            col('condition_filter', 'band_up', 'band_dn', 'hh', 'll').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("condition_filter_1", "band_up_1", "hh_1").if_else().alias("h_max"),
            col("condition_filter_1", "band_dn_1", "ll_1").if_else().alias("l_min"),
        )
        .with_cols(
            col('h_max', 'l_min').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("close") > col("h_max"))
                & (col("close_1") <= col("h_max_1"))
            )
            .fill_null(False)
            .alias("condition_d"),
            (
                (col("close") < col("l_min"))
                & (col("close_1") >= col("l_min_1"))
            )
            .fill_null(False)
            .alias("condition_k"),
        )
        .with_cols(
            col('condition_d', 'condition_k').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("condition_d_1").fill_null(False).alias("open_long_raw"),
            col("condition_k_1").fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            col("pos_pre").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("pos_pre_1") != 1.0)).fill_null(False).alias("open_long_pre"),
            ((col("pos_pre") == -1.0) & (col("pos_pre_1") != -1.0)).fill_null(False).alias("open_short_pre"),
        )
        .with_cols(
            _bars_pos_from_open_sig_expr("open_long_pre", "open_short_pre", "bars_pos"),
            col("open_long_pre", "open_short_pre", "low", "open")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("low_after_entry"),
            col("open_short_pre", "open_long_pre", "high", "open")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("high_after_entry"),
        )
        .with_cols(
            col(col("pos_pre") == 0.0, 1.0, col(
                    1.0 - (col("bars_pos").cast(pl.Float64) + 1.0) * 0.1,
                    0.5,
                ).max(axis=1)).if_else()
            .alias("liqka"),
        )
        .with_cols(
            (col("low_after_entry") - (col("open") * col("trs") * 0.001) * col("liqka")).alias("dliq"),
            (col("high_after_entry") + (col("open") * col("trs") * 0.001) * col("liqka")).alias("kliq"),
        )
        .with_cols(
            col("dliq", "kliq").shift(1).expanding().add_suffix("1"),
            col("dliq", "kliq").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("low_1") <= col("dliq_1"))
                & (col("low_2") >= col("dliq_2"))
                & (col("dliq_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("high_1") >= col("kliq_1"))
                & (col("high_2") <= col("kliq_2"))
                & (col("kliq_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "sf12",
        "按 SF12 源码实现：用 R1/R1*2 波动率比值自适应选择突破区间开仓，并以 liQKA 自适应吊灯线离场。",
    )

def build_sf13_expr() -> Expr:
    length = pms("sf13_length", 5, 200, 5).limit(20)
    x = pms("sf13_x", 0.1, 20.0, 0.1).limit(20)
    trs = pms("sf13_trs", 1.0, 100.0, 1.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("datetime").dt.date().alias("day"),
            col("close", "high", "low").shift(1).expanding().add_suffix("1"),
            col("high", "low").shift(2).expanding().add_suffix("2"),
            col('high', 'low').shift(3).expanding().add_suffix("3"),
            col('high', 'low').shift(4).expanding().add_suffix("4"),
            col('high', 'low').shift(5).expanding().add_suffix("5"),
            col.lit(x).alias("x"),
            col.lit(trs).alias("trs"),
        )
        .with_cols(
            col("day").shift(1).expanding().add_suffix("1"),
            col("high", "low", "close").ta.atr(length).expanding().alias("atr"),
        )
        .with_cols(
            (col("day") != col("day_1")).fill_null(True).alias("is_new_day"),
            col("atr").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("is_new_day", "open", col.null).if_else().alias("open_d0_seed"),
            (col("high_1") - col("close_1")).alias("range_up"),
            (col("close_1") - col("low_1")).alias("range_down"),
            ((
                (col("high_1") - col("low_1"))
                + (col("high_2") - col("low_2"))
                + (col("high_3") - col("low_3"))
                + (col("high_4") - col("low_4"))
            ) / 4.0)
            .alias("trange"),
        )
        .with_cols(
            col("open_d0_seed").ffill().expanding().alias("open_d0"),
            col("range_up", "trange").min(axis=1).alias("range_up_min"),
            col("range_down", "trange").min(axis=1).alias("range_dn_min"),
        )
        .with_cols(
            col("range_up_min").ta.ema(5).expanding().alias("ema_up"),
            col("range_dn_min").ta.ema(5).expanding().alias("ema_dn"),
            (col("open_d0") + col("atr_1") * col("x")).alias("hh"),
            (col("open_d0") - col("atr_1") * col("x")).alias("ll"),
            col("high_5").max().rolling(length).alias("high5_hh"),
            col("low_5").min().rolling(length).alias("low5_ll"),
        )
        .with_cols(
            col('ema_up', 'ema_dn').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("high5_hh", "hh").max(axis=1) + col("ema_up", "ema_up_1").max(axis=1)).alias("upband"),
            (col("low5_ll", "ll").min(axis=1) - col("ema_dn", "ema_dn_1").max(axis=1)).alias("dnband"),
        )
        .with_cols(
            col("is_new_day", "upband", col.null).if_else().alias("rupband_seed"),
            col("is_new_day", "dnband", col.null).if_else().alias("rdnband_seed"),
            col('upband', 'dnband').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("rupband_seed").ffill().expanding().alias("rupband"),
            col("rdnband_seed").ffill().expanding().alias("rdnband"),
            col("rupband_seed", "rdnband_seed")
            .ffill()
            .shift(1)
            .expanding()
            .alias("rupband_1", "rdnband_1"),
        )
        .with_cols(
            ((col("high") >= col("upband_1")) & (col("upband_1") > col("rupband_1")))
            .fill_null(False)
            .alias("open_long_raw"),
            ((col("low") <= col("dnband_1")) & (col("dnband_1") < col("rdnband_1")))
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            col("pos_pre").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("pos_pre_1") != 1.0)).fill_null(False).alias("open_long_pre"),
            ((col("pos_pre") == -1.0) & (col("pos_pre_1") != -1.0)).fill_null(False).alias("open_short_pre"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "open_long_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
        )
        .with_cols(
            _bars_pos_from_open_sig_expr("open_long_pre", "open_short_pre", "bars_pos"),
            col("open_long_pre", "open_short_pre", "low", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("low_after_entry"),
            col("open_short_pre", "open_long_pre", "high", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("high_after_entry"),
        )
        .with_cols(
            col(col("pos_pre") == 0.0, 1.0, col(
                    1.0 - (col("bars_pos").cast(pl.Float64) + 1.0) * 0.1,
                    0.5,
                ).max(axis=1)).if_else()
            .alias("liqka"),
        )
        .with_cols(
            (col("low_after_entry") - col("open") * col("trs") * 0.001 * col("liqka")).alias("dliq"),
            (col("high_after_entry") + col("open") * col("trs") * 0.001 * col("liqka")).alias("kliq"),
        )
        .with_cols(
            col('dliq', 'kliq').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("low") <= col("dliq_1"))
                & (col("dliq_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("high") >= col("kliq_1"))
                & (col("kliq_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "sf13",
        "按 SF13 源码实现：ATR+开盘偏移通道突破开仓，叠加通道扩张过滤，持仓后采用 liQKA 自适应吊灯止损出场。",
    )

def build_sf14_expr() -> Expr:
    periods = pms("sf14_periods", 5, 240, 5).limit(20)
    multiplier = pms("sf14_multiplier", 1.0, 100.0, 1.0).limit(20)
    n = pms("sf14_n", 1.0, 20.0, 1.0).limit(20)
    trs = pms("sf14_trs", 1.0, 120.0, 1.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("datetime").dt.date().alias("day"),
            col("open").first_value().expanding().over(col("datetime").dt.date()).alias("open_d0"),
            col("high", "low", "close").ta.atr(periods).expanding().alias("atr"),
        )
        .with_cols(
            ((col("high") + col("low")) * 0.5).alias("hl2"),
        )
        .with_cols(
            (col("hl2") - multiplier * col("atr")).alias("up_raw"),
            (col("hl2") + multiplier * col("atr")).alias("dn_raw"),
        )
        .with_cols(
            col("close", "up_raw", "dn_raw").stra.sf14_supertrend_state().expanding(),
        )
        .with_cols(
            (col("kg") == 1.0).fill_null(False).alias("buy_signal"),
            (col("kg") == -1.0).fill_null(False).alias("sell_signal"),
            (col("open_d0") + col("atr") * n).alias("hh"),
            (col("open_d0") - col("atr") * n).alias("ll"),
        )
        .with_cols(
            col('buy_signal', 'sell_signal', 'hh', 'll').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("buy_signal_1") & (col("high") >= col("hh_1"))).fill_null(False).alias("open_long_raw"),
            (col("sell_signal_1") & (col("low") <= col("ll_1"))).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            col("pos_pre").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("pos_pre_1") != 1.0)).fill_null(False).alias("open_long_pre"),
            ((col("pos_pre") == -1.0) & (col("pos_pre_1") != -1.0)).fill_null(False).alias("open_short_pre"),
        )
        .with_cols(
            _bars_pos_from_open_sig_expr("open_long_pre", "open_short_pre", "bars_pos"),
            col("open_long_pre", "open_short_pre", "low", "open")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("low_after_entry"),
            col("open_short_pre", "open_long_pre", "high", "open")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("high_after_entry"),
        )
        .with_cols(
            col(col("pos_pre") == 0.0, 1.0, col(
                    1.0 - (col("bars_pos").cast(pl.Float64) + 1.0) * 0.1,
                    0.5,
                ).max(axis=1)).if_else()
            .alias("liqka"),
        )
        .with_cols(
            (col("low_after_entry") - (col("open") * trs * 0.001) * col("liqka")).alias("dliq"),
            (col("high_after_entry") + (col("open") * trs * 0.001) * col("liqka")).alias("kliq"),
        )
        .with_cols(
            col('dliq', 'kliq').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("low") <= col("dliq_1"))
                & (col("dliq_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("high") >= col("kliq_1"))
                & (col("kliq_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "sf14",
        "按 SF14 源码实现：SuperTrend 方向状态 + OpenD±ATR*N 突破开仓，持仓后以 liQKA 自适应吊灯止损离场。",
    )

def build_sf15_expr() -> Expr:
    x = pms("sf15_x", 2, 120, 2).limit(20)
    p = pms("sf15_p", 0.1, 5.0, 0.1).limit(20)
    n = pms("sf15_n", 5, 240, 5).limit(20)
    trs = pms("sf15_trs", 1.0, 120.0, 1.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col.lit(x).cast(pl.UInt32).alias("x_u"),
            col.lit(trs).alias("trs"),
            col("close").shift(1).expanding().add_suffix("1"),
            col("close").shift(n).expanding().alias("close_n"),
            col('high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("high_1").max().rolling(n).alias("sh"),
            col("low_1").min().rolling(n).alias("sl"),
            (col("close") - col("close_n")).abs().alias("netchg"),
            (col("close") - col("close_1")).abs().alias("abs_diff"),
        )
        .with_cols(
            col("abs_diff").sum().rolling(n).alias("totchg"),
        )
        .with_cols(
            col(col("totchg") > 0.0, col("netchg") / col("totchg") * 10.0, 0.0).if_else().alias("effratio"),
            col("high", "low", "close").ta.atr(n).expanding().alias("atr"),
        )
        .with_cols(
            (col("effratio") > p).fill_null(False).alias("filter"),
            col('sh', 'sl').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("close") > col("sh")) & (col("close_1") <= col("sh_1"))).fill_null(False).alias("cond1"),
            ((col("close") < col("sl")) & (col("close_1") >= col("sl_1"))).fill_null(False).alias("cond2"),
        )
        .with_cols(
            col('cond1', 'cond2').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("cond1_1") & (col("sh_1") > 0.0) & col("filter")).fill_null(False).alias("open_long_raw"),
            (col("cond2_1") & (col("sl_1") > 0.0) & col("filter")).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            col("pos_pre").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("pos_pre_1") != 1.0)).fill_null(False).alias("open_long_pre"),
            ((col("pos_pre") == -1.0) & (col("pos_pre_1") != -1.0)).fill_null(False).alias("open_short_pre"),
        )
        .with_cols(
            _bars_pos_from_open_sig_expr("open_long_pre", "open_short_pre", "bars_pos"),
            col("open_long_pre", "open_short_pre", "low", "open")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("low_after_entry"),
            col("open_short_pre", "open_long_pre", "high", "open")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("high_after_entry"),
        )
        .with_cols(
            col(col("pos_pre") == 0.0, 1.0, col(
                    1.0 - (col("bars_pos").cast(pl.Float64) + 1.0) * 0.1,
                    0.3,
                ).max(axis=1)).if_else()
            .alias("liqka"),
        )
        .with_cols(
            (col("low_after_entry") - (col("open") * col("trs") * 0.001) * col("liqka")).alias("dliq"),
            (col("high_after_entry") + (col("open") * col("trs") * 0.001) * col("liqka")).alias("kliq"),
        )
        .with_cols(
            col('dliq', 'kliq').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("low") <= col("dliq_1"))
                & (col("dliq_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("high") >= col("kliq_1"))
                & (col("kliq_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "sf15",
        "按 SF15 源码主逻辑实现：效率比（ER）过滤后触发区间波峰/波谷突破开仓，持仓后使用 liQKA 吊灯止损出场。",
    )

def build_sf16_expr() -> Expr:
    n = pms("sf16_n", 5, 240, 25).limit(20)
    m = pms("sf16_m", 5, 320, 70).limit(20)
    x = pms("sf16_x", 0.1, 3.0, 0.6).limit(20)
    trs = pms("sf16_trs", 1.0, 120.0, 40.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col.lit(x).alias("x"),
            col.lit(trs).alias("trs"),
            col("close").mean().rolling(n).alias("ma1"),
            col("close").mean().rolling(m).alias("ma60"),
            col("high").max().rolling(n).alias("hh"),
            col("low").min().rolling(n).alias("ll"),
            col("close", "high", "low").shift(1).expanding().add_suffix("1"),
            col("high", "low").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            col('ma1', 'ma60').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (((col("ma60") - col("ma60_1")) / col("ma60")) * 100.0).alias("linear"),
            ((col("ma1") > col("ma60")) & (col("ma1_1") <= col("ma60_1"))).fill_null(False).alias("cross_up_ma"),
            ((col("ma1") < col("ma60")) & (col("ma1_1") >= col("ma60_1"))).fill_null(False).alias("cross_dn_ma"),
        )
        .with_cols(
            col("cross_up_ma", "cross_dn_ma", "linear")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("star_up"),
            col("cross_dn_ma", "cross_up_ma", "linear")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("star_dn"),
            col("cross_up_ma", "cross_dn_ma", "linear", "linear")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("min_after_up"),
            col("cross_dn_ma", "cross_up_ma", "linear", "linear")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("max_after_dn"),
        )
        .with_cols(
            (
                (col("linear") > col("min_after_up"))
                & (col("linear") > col("star_up") * (1.0 + col("x")))
                & (col("min_after_up") <= col("star_up"))
                & (col("ma1") > col("ma60"))
                & (col("star_up") != 0.0)
            )
            .fill_null(False)
            .alias("k_long"),
            (
                (col("linear") < col("max_after_dn"))
                & (col("linear") < col("star_dn") * (1.0 - col("x")))
                & (col("max_after_dn") >= col("star_dn"))
                & (col("ma1") < col("ma60"))
                & (col("star_dn") != 0.0)
            )
            .fill_null(False)
            .alias("k_short"),
        )
        .with_cols(
            _pos_from_open_raw_expr("k_long", "k_short", "k_state"),
            col('hh', 'll').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("k_state").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("k_state_1") > 0.0)
                & (col("high") >= col("hh_1"))
                & (col("hh_1") > 0.0)
                & (col("close_1") < col("hh_1"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("k_state_1") < 0.0)
                & (col("low") <= col("ll_1"))
                & (col("ll_1") > 0.0)
                & (col("close_1") > col("ll_1"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            col("pos_pre").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("pos_pre_1") != 1.0)).fill_null(False).alias("open_long_pre"),
            ((col("pos_pre") == -1.0) & (col("pos_pre_1") != -1.0)).fill_null(False).alias("open_short_pre"),
        )
        .with_cols(
            _bars_pos_from_open_sig_expr("open_long_pre", "open_short_pre", "bars_pos"),
            col("open_long_pre", "open_short_pre", "low", "open")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("low_after_entry"),
            col("open_short_pre", "open_long_pre", "high", "open")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("high_after_entry"),
        )
        .with_cols(
            col(col("pos_pre") == 0.0, 1.0, col(
                    1.0 - (col("bars_pos").cast(pl.Float64) + 1.0) * 0.1,
                    0.3,
                ).max(axis=1)).if_else()
            .alias("liqka"),
        )
        .with_cols(
            (col("low_after_entry") - (col("open") * col("trs") * 0.001) * col("liqka")).alias("dliq"),
            (col("high_after_entry") + (col("open") * col("trs") * 0.001) * col("liqka")).alias("kliq"),
        )
        .with_cols(
            col('dliq', 'kliq').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("low") <= col("dliq_1"))
                & (col("dliq_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("high") >= col("kliq_1"))
                & (col("kliq_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "sf16",
        "按 SF16 源码主逻辑实现：凹凸均线状态触发后做 N 周期突破入场，持仓后以 liQKA 自适应吊灯线止损出场。",
    )

def build_sf17_expr() -> Expr:
    m = pms("sf17_m", 2, 80, 10).limit(20)
    s = pms("sf17_s", 1, 20, 3).limit(20)
    trs = pms("sf17_trs", 1.0, 80.0, 30.0).limit(20)
    m_div_s = pms("sf17_m_div_s", 1, 40, 3).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            (((col("high") + col("low") + col("close")) / 3.0)).alias("hlc3"),
            col.lit(trs).alias("trs"),
            col('high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("hlc3").ewm(2.0 / (m_div_s + 1.0)).expanding().alias("sma_1"),
            col("hlc3").ewm(2.0 / (m + 1.0)).expanding().alias("sma_long"),
        )
        .with_cols(
            col("sma_1").ta.ema(m_div_s).expanding().alias("sma_2"),
            col("sma_long").ta.ema(m).expanding().alias("smalong_ma"),
            col("high_1").max().rolling(m).alias("hh"),
            col("low_1").min().rolling(m).alias("ll"),
        )
        .with_cols(
            (col("sma_1") - col("smalong_ma")).alias("marange"),
        )
        .with_cols(
            col("marange").ta.ema(m_div_s).expanding().alias("marange_ma"),
        )
        .with_cols(
            (
                (col("marange") > col("marange_ma"))
                & (col("marange") > 0.0)
                & (col("sma_1") > col("sma_2"))
                & (col("sma_long") > col("smalong_ma"))
                & (col("sma_2") > col("smalong_ma"))
            )
            .fill_null(False)
            .alias("dk"),
            (
                (col("marange") < col("marange_ma"))
                & (col("marange") < 0.0)
                & (col("sma_1") < col("sma_2"))
                & (col("sma_long") < col("smalong_ma"))
                & (col("sma_2") < col("smalong_ma"))
            )
            .fill_null(False)
            .alias("kk"),
        )
        .with_cols(
            col('dk', 'kk', 'hh', 'll').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("dk_1") & (col("high") >= col("hh_1"))).fill_null(False).alias("open_long_raw"),
            (col("kk_1") & (col("low") <= col("ll_1"))).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            col("pos_pre").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("pos_pre_1") != 1.0)).fill_null(False).alias("open_long_pre"),
            ((col("pos_pre") == -1.0) & (col("pos_pre_1") != -1.0)).fill_null(False).alias("open_short_pre"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "open_long_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "low", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("low_after_entry"),
            col("open_short_pre", "open_long_pre", "high", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("high_after_entry"),
        )
        .with_cols(
            _bars_pos_from_open_sig_expr("open_long_pre", "open_short_pre", "bars_pos"),
        )
        .with_cols(
            col(col("pos_pre") == 0.0, 1.0, col(
                    1.0 - (col("bars_pos").cast(pl.Float64) + 1.0) * 0.1,
                    0.3,
                ).max(axis=1)).if_else()
            .alias("liqka"),
        )
        .with_cols(
            (col("low_after_entry") - col("open") * col("trs") * 0.001 * col("liqka")).alias("dliq"),
            (col("high_after_entry") + col("open") * col("trs") * 0.001 * col("liqka")).alias("kliq"),
        )
        .with_cols(
            col('dliq', 'kliq').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("low") <= col("dliq_1"))
                & (col("dliq_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("high") >= col("kliq_1"))
                & (col("kliq_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "sf17",
        "按 SF17 源码主逻辑实现：多层均线波动差过滤突破开仓，持仓后以 liQKA 自适应吊灯线止损离场。",
    )

def build_sf18_expr() -> Expr:
    n = pms("sf18_n", 1, 20, 2).limit(20)
    x = pms("sf18_x", 2, 120, 5).limit(20)
    trs = pms("sf18_trs", 1.0, 120.0, 25.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("bar_no"),
            col.lit(x).cast(pl.UInt32).alias("x_u"),
            col.lit(trs).alias("trs"),
            col("high", "low").shift(1).expanding().add_suffix("1"),
            col("high", "low").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            col("close").ewm(2.0 / (n * 4 + 1.0)).expanding().alias("ema_fast"),
            col("close").ewm(2.0 / (n * 9 + 1.0)).expanding().alias("ema_slow"),
        )
        .with_cols(
            (col("ema_fast") - col("ema_slow")).alias("macd_value"),
        )
        .with_cols(
            col("macd_value").ewm(2.0 / (n * 3 + 1.0)).expanding().alias("avg_macd"),
            col("macd_value").shift(1).expanding().alias("macd_1"),
        )
        .with_cols(
            col("avg_macd").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("macd_value") > col("avg_macd")) & (col("macd_1") <= col("avg_macd_1"))).fill_null(False).alias("cross_up"),
            ((col("macd_value") < col("avg_macd")) & (col("macd_1") >= col("avg_macd_1"))).fill_null(False).alias("cross_dn"),
        )
        .with_cols(
            col("cross_up", "cross_dn").shift(1).expanding().add_suffix("1"),
            col("cross_up", "cross_dn").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            col("cross_up_1", "bar_no", col.null).if_else().alias("hbar_seed"),
            col("cross_dn_1", "bar_no", col.null).if_else().alias("lbar_seed"),
        )
        .with_cols(
            col("hbar_seed").ffill().expanding().alias("hbar"),
            col("lbar_seed").ffill().expanding().alias("lbar"),
        )
        .with_cols(
            col(
                (col("hbar").cast(pl.Int64) - col("lbar").cast(pl.Int64)).cast(pl.Float64),
                col("x_u").cast(pl.Float64),
            ).max(axis=1)
            .fill_null(col("x_u").cast(pl.Float64))
            .cast(pl.UInt32)
            .alias("win_h"),
            col(
                (col("lbar").cast(pl.Int64) - col("hbar").cast(pl.Int64)).cast(pl.Float64),
                col("x_u").cast(pl.Float64),
            ).max(axis=1)
            .fill_null(col("x_u").cast(pl.Float64))
            .cast(pl.UInt32)
            .alias("win_l"),
        )
        .with_cols(
            col("high_1").max().rolling_dynamic("win_h", window_max=8192, min_samples=1).alias("highup"),
            col("low_1").min().rolling_dynamic("win_l", window_max=8192, min_samples=1).alias("lowdown"),
        )
        .with_cols(
            col("cross_up_1", "highup", col.null).if_else().alias("hd_seed"),
            col("cross_dn_1", "lowdown", col.null).if_else().alias("ld_seed"),
            _state_seed_expr("cross_up_2", "cross_dn_2", alias="k_seed"),
        )
        .with_cols(
            col("hd_seed").ffill().expanding().alias("hd"),
            col("ld_seed").ffill().expanding().alias("ld"),
            col("k_seed").ffill().expanding().alias("k_raw"),
            col("close").shift(1).expanding().add_suffix("1"),
            col("avg_macd").shift(2).expanding().add_suffix("2"),
            col("close").shift(3).expanding().add_suffix("3"),
        )
        .with_cols(
            col("k_raw").fill_null(0.0).alias("k"),
        )
        .with_cols(
            (
                col("cross_up_2")
                & (col("avg_macd") < 0.0)
                & (col("close_1") < col("close_3"))
                & (col("avg_macd") > col("avg_macd_2"))
            )
            .fill_null(False)
            .alias("condition_d"),
            (
                col("cross_dn_2")
                & (col("avg_macd") > 0.0)
                & (col("close_1") > col("close_3"))
                & (col("avg_macd") < col("avg_macd_2"))
            )
            .fill_null(False)
            .alias("condition_k"),
        )
        .with_cols(
            col(
                ((col("k") > 0.0) & (col("high") >= col("hd")) & (col("hd") > 0.0)).fill_null(False),
                col("condition_d"),
            ).any(axis=1).fill_null(False).alias("open_long_raw"),
            col(
                ((col("k") < 0.0) & (col("low") <= col("ld")) & (col("ld") > 0.0)).fill_null(False),
                col("condition_k"),
            ).any(axis=1).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            col("pos_pre").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("pos_pre_1") != 1.0)).fill_null(False).alias("open_long_pre"),
            ((col("pos_pre") == -1.0) & (col("pos_pre_1") != -1.0)).fill_null(False).alias("open_short_pre"),
        )
        .with_cols(
            _bars_pos_from_open_sig_expr("open_long_pre", "open_short_pre", "bars_pos"),
            col("open_long_pre", "open_short_pre", "low", "open")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("low_after_entry"),
            col("open_short_pre", "open_long_pre", "high", "open")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("high_after_entry"),
        )
        .with_cols(
            col(col("pos_pre") == 0.0, 1.0, col(
                    1.0 - (col("bars_pos").cast(pl.Float64) + 1.0) * 0.1,
                    0.3,
                ).max(axis=1)).if_else()
            .alias("liqka"),
        )
        .with_cols(
            (col("low_after_entry") - (col("open") * col("trs") * 0.001) * col("liqka")).alias("dliq"),
            (col("high_after_entry") + (col("open") * col("trs") * 0.001) * col("liqka")).alias("kliq"),
        )
        .with_cols(
            col('dliq', 'kliq').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("low") <= col("dliq_1"))
                & (col("dliq_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("high") >= col("kliq_1"))
                & (col("kliq_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "sf18",
        "按 SF18 源码主逻辑实现：MACD 交叉分段区间突破与背离条件入场，持仓后由 liQKA 自适应吊灯线止损离场。",
    )

def build_sf19_expr() -> Expr:
    length = pms("sf19_length", 2, 160, 10).limit(20)
    signal_length = pms("sf19_signal_length", 2, 80, 5).limit(20)
    n = pms("sf19_n", 1, 12, 3).limit(20)
    trs = pms("sf19_trs", 1.0, 120.0, 50.0).limit(20)
    sig_long = signal_length * n
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("open", "close", "high", "low").shift(1).expanding().add_suffix("1"),
            col("high", "low").shift(2).expanding().add_suffix("2"),
            col.lit(trs).alias("trs"),
        )
        .with_cols(
            col("high_1").max().rolling(length).alias("hh"),
            col("low_1").min().rolling(length).alias("ll"),
            (col("close_1") * col("volume")).alias("pv_1"),
        )
        .with_cols(
            col("pv_1", "volume").sum().rolling(signal_length).alias("pv_short", "v_short"),
            col("pv_1", "volume").sum().rolling(sig_long).alias("pv_long", "v_long"),
        )
        .with_cols(
            col(col("v_short") == 0.0, col.null, col("pv_short") / col("v_short")).if_else().alias("vwap_a"),
            col(col("v_long") == 0.0, col.null, col("pv_long") / col("v_long")).if_else().alias("vwap_b"),
        )
        .with_cols(
            col('vwap_a', 'vwap_b').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("vwap_a") > col("vwap_b"))
                & (col("vwap_a_1") <= col("vwap_b_1"))
                & (col("vwap_a") > 0.0)
                & (col("vwap_b") > 0.0)
            )
            .fill_null(False)
            .alias("bullish"),
            (
                (col("vwap_a") < col("vwap_b"))
                & (col("vwap_a_1") >= col("vwap_b_1"))
                & (col("vwap_a") > 0.0)
                & (col("vwap_b") > 0.0)
            )
            .fill_null(False)
            .alias("bearish"),
        )
        .with_cols(
            col('bullish', 'bearish').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("bullish_1", "hh", col.null).if_else().alias("hband_seed"),
            col("bearish_1", "ll", col.null).if_else().alias("lband_seed"),
            _state_seed_expr("bullish_1", "bearish_1", alias="kg_seed"),
        )
        .with_cols(
            col("hband_seed").ffill().expanding().alias("hband"),
            col("lband_seed").ffill().expanding().alias("lband"),
            col("kg_seed").ffill().expanding().alias("kg_raw"),
        )
        .with_cols(
            col("kg_raw").fill_null(0.0).alias("kg"),
        )
        .with_cols(
            col('hband', 'lband').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("kg") == 1.0)
                & (col("high") >= col("hband_1"))
                & (col("close_1") > col("open_1"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("kg") == -1.0)
                & (col("low") <= col("lband_1"))
                & (col("close_1") < col("open_1"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            col("pos_pre").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("pos_pre_1") != 1.0)).fill_null(False).alias("open_long_pre"),
            ((col("pos_pre") == -1.0) & (col("pos_pre_1") != -1.0)).fill_null(False).alias("open_short_pre"),
        )
        .with_cols(
            _bars_pos_from_open_sig_expr("open_long_pre", "open_short_pre", "bars_pos"),
            col("open_long_pre", "open_short_pre", "low", "open")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("low_after_entry"),
            col("open_short_pre", "open_long_pre", "high", "open")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("high_after_entry"),
        )
        .with_cols(
            col(col("pos_pre") == 0.0, 1.0, col(
                    1.0 - (col("bars_pos").cast(pl.Float64) + 1.0) * 0.1,
                    0.3,
                ).max(axis=1)).if_else()
            .alias("liqka"),
        )
        .with_cols(
            (col("low_after_entry") - (col("open") * col("trs") * 0.001) * col("liqka")).alias("dliq"),
            (col("high_after_entry") + (col("open") * col("trs") * 0.001) * col("liqka")).alias("kliq"),
        )
        .with_cols(
            col('dliq', 'kliq').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_pos") > 0)
                & (col("low") <= col("dliq_1"))
                & (col("dliq_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_pos") > 0)
                & (col("high") >= col("kliq_1"))
                & (col("kliq_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "sf19",
        "按 SF19 源码主逻辑实现：短长 VWAP 交叉确定方向并触发区间突破开仓，持仓后通过 liQKA 自适应吊灯线离场。",
    )

def build_sf20_expr() -> Expr:
    lenth = pms("sf20_lenth", 5, 300, 5).limit(20)
    longshort_stop = pms("sf20_longshort_stop", 0.5, 40.0, 0.5).limit(20)
    longma = pms("sf20_longma", 5, 500, 5).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("datetime").dt.date().alias("day"),
            col('close', 'high', 'low', 'volume').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("day").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("day") != col("day_1")).fill_null(True).alias("is_new_day"),
            (col("high") - col("low")).alias("range"),
            col("close_1").mean().rolling(longma).alias("ma"),
        )
        .with_cols(
            col("is_new_day", "open", col.null).if_else().alias("open_d0_seed"),
        )
        .with_cols(
            col("open_d0_seed").ffill().expanding().alias("open_d0"),
            col("range").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("high_1") - col("close_1") + col("open_d0")).alias("up_src"),
            (col("low_1") - col("close_1") + col("open_d0")).alias("dn_src"),
            ((col("high") + col("low")) * 0.5).alias("median"),
        )
        .with_cols(
            col("up_src").max().rolling(lenth).alias("up_avg"),
            col("dn_src").min().rolling(lenth).alias("low_avg"),
            (col("median") > col("high_1")).fill_null(False).alias("upband"),
            (col("median") < col("low_1")).fill_null(False).alias("downband"),
        )
        .with_cols(
            col('up_avg', 'low_avg', 'upband', 'downband').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("close_1") > col("ma"))
                & col("upband_1")
                & (col("close_1") > col("up_avg_1"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("close_1") < col("ma"))
                & col("downband_1")
                & (col("close_1") < col("low_avg_1"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            col("pos_pre").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("pos_pre_1") != 1.0)).fill_null(False).alias("open_long_pre"),
            ((col("pos_pre") == -1.0) & (col("pos_pre_1") != -1.0)).fill_null(False).alias("open_short_pre"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre").count_last().expanding().alias("bars_long", "bars_short"),
            _bars_pos_from_open_sig_expr("open_long_pre", "open_short_pre", "bars_pos"),
            col("open_long_pre", "open_short_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "open_long_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "low", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("low_after_entry"),
            col("open_short_pre", "open_long_pre", "high", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("high_after_entry"),
        )
        .with_cols(
            _pick_by_pos_expr("pos_pre", col("low_after_entry"), col("high_after_entry"), 0.0, "entprice"),
        )
        .with_cols(
            _pick_by_pos_expr(
                "pos_pre",
                col("entprice") * (1.0 - longshort_stop * 0.001),
                col("entprice") * (1.0 + longshort_stop * 0.001),
                0.0,
                "vwapprice",
            ),
            (col("bars_pos") + 1).cast(pl.UInt32).alias("window_pos"),
        )
        .with_cols(
            (col("volume_1") * col("vwapprice")).alias("vwap_num"),
        )
        .with_cols(
            col("vwap_num").sum().rolling_dynamic("window_pos", window_max=5000, min_samples=1).alias("vwap_num_roll"),
            col("volume_1").sum().rolling_dynamic("window_pos", window_max=5000, min_samples=1).alias("vwap_den_roll"),
        )
        .with_cols(
            (col("vwap_num_roll") / col("vwap_den_roll")).alias("vwap_a"),
            col('vwap_num_roll', 'vwap_den_roll').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("vwap_num_roll_1") / col("vwap_den_roll_1")).alias("vwap_a_1"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & (col("low") <= col("vwap_a_1"))
                & (col("vwap_a_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & (col("high") >= col("vwap_a_1"))
                & (col("vwap_a_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "sf20",
        "按 SF20 源码实现：区间偏移突破+长均线过滤开仓，持仓后按量加权自适应 VWAP_A 线触发止盈止损离场。",
    )

def build_sf21_expr() -> Expr:
    n = pms("sf21_n", 2, 120, 120).limit(20)
    ts = pms("sf21_ts", 10.0, 99.0, 99.0).limit(20)
    tss = pms("sf21_tss", 1.0, 50.0, 50.0).limit(20)
    longshort_stop = pms("sf21_longshort_stop", 1.0, 40.0, 40.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").shift(1).expanding().add_suffix("1"),
            col("close").shift(2).expanding().add_suffix("2"),
            col("volume").shift(1).expanding().alias("vol_1"),
        )
        .with_cols(
            (col("close_1") > col("close_2")).fill_null(False).alias("cond_close_prev"),
            (col("close_1") - col("close_2")).abs().alias("true_range_prev"),
        )
        .with_cols(
            col("cond_close_prev", 1.0, 0.0).if_else().alias("up_cnt"),
            col("cond_close_prev", col("true_range_prev"), 0.0).if_else().alias("up_range"),
            col("cond_close_prev", 0.0, col("true_range_prev")).if_else().alias("dn_range"),
        )
        .with_cols(
            col("up_cnt", "up_range", "dn_range").sum().rolling(n).alias("sum_up", "sum_uprange", "sum_dnrange"),
        )
        .with_cols(
            (col("sum_up") / n * 100.0).alias("psy"),
            (col("sum_uprange") / (col("sum_uprange") + col("sum_dnrange")) * 100.0).alias("rangey"),
        )
        .with_cols(
            col("rangey").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("rangey") > ts) & (col("rangey_1") <= ts) & (col("psy") > 50.0))
            .fill_null(False)
            .alias("cond1"),
            ((col("rangey") < tss) & (col("rangey_1") >= tss) & (col("psy") < 50.0))
            .fill_null(False)
            .alias("cond2"),
        )
        .with_cols(
            col('cond1', 'cond2').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("cond1_1").fill_null(False).alias("open_long_raw"),
            col("cond2_1").fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            col("pos_pre").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("pos_pre_1") != 1.0)).fill_null(False).alias("open_long_pre"),
            ((col("pos_pre") == -1.0) & (col("pos_pre_1") != -1.0)).fill_null(False).alias("open_short_pre"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre").count_last().expanding().alias("bars_long", "bars_short"),
            _bars_pos_from_open_sig_expr("open_long_pre", "open_short_pre", "bars_pos"),
            col("open_long_pre", "open_short_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "open_long_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "low", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("low_after_entry"),
            col("open_short_pre", "open_long_pre", "high", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("high_after_entry"),
        )
        .with_cols(
            _pick_by_pos_expr("pos_pre", col("low_after_entry"), col("high_after_entry"), 0.0, "entprice"),
        )
        .with_cols(
            _pick_by_pos_expr(
                "pos_pre",
                col("entprice") * (1.0 - longshort_stop * 0.001),
                col("entprice") * (1.0 + longshort_stop * 0.001),
                0.0,
                "vwapprice",
            ),
            (col("bars_pos") + 1).cast(pl.UInt32).alias("window_pos"),
        )
        .with_cols(
            (col("vol_1") * col("vwapprice")).alias("vwap_num"),
        )
        .with_cols(
            col("vwap_num").sum().rolling_dynamic("window_pos", window_max=5000, min_samples=1).alias("vwap_num_roll"),
            col("vol_1").sum().rolling_dynamic("window_pos", window_max=5000, min_samples=1).alias("vwap_den_roll"),
        )
        .with_cols(
            col('vwap_num_roll', 'vwap_den_roll').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("vwap_num_roll_1") / col("vwap_den_roll_1")).alias("vwap_a_1"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_long") > 0)
                & (col("low") <= col("vwap_a_1"))
                & (col("vwap_a_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_short") > 0)
                & (col("high") >= col("vwap_a_1"))
                & (col("vwap_a_1") > 0.0)
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "sf21",
        "按 SF21 源码实现：rangeY 与 PSY 阈值交叉开仓，持仓后通过量加权 VWAP_A 自适应线止盈止损离场。",
    )

def build_t01_expr() -> Expr:
    lookback_a = pms("t01_lookback_a", 1, 20, 1).limit(20)  # Params3
    lookback_b = pms("t01_lookback_b", 1, 20, 1).limit(20)  # Params4
    down_k = pms("t01_down_k", 0.1, 4.0, 0.1).limit(20)  # Params2
    up_k = pms("t01_up_k", 0.1, 4.0, 0.1).limit(20)  # Params1
    start_hhmm = pms("t01_start_hhmm", 0.0, 23.59, 0.0).limit(20)  # Params5

    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            (col("datetime").dt.hour() * 100 + col("datetime").dt.minute()).alias("hhmm"),
            col(
                col("datetime").count().alias("bars_in_day"),
                col("open").first_value().alias("day_open"),
                col("high").max().alias("day_high_cum"),
                col("low").min().alias("day_low_cum"),
            ).expanding().over(col("datetime").dt.date()),
        )
        .with_cols(
            col("day_high_cum", "day_low_cum", "close")
            .shift(1)
            .expanding()
            .alias("day_high_peer_1", "day_low_peer_1", "close_peer_1")
            .over("bars_in_day"),
        )
        .with_cols(
            col(
                col("day_high_peer_1").max().alias("hh_a"),
                col("day_low_peer_1").min().alias("ll_a"),
                col("close_peer_1").max().alias("cc_max_a"),
                col("close_peer_1").min().alias("cc_min_a"),
            ).rolling(lookback_a),
            col(
                col("day_high_peer_1").max().alias("hh_b"),
                col("day_low_peer_1").min().alias("ll_b"),
                col("close_peer_1").max().alias("cc_max_b"),
                col("close_peer_1").min().alias("cc_min_b"),
            ).rolling(lookback_b),
        )
        .with_cols(
            col("hh_a", "ll_a", "cc_max_a", "cc_min_a", "hh_b", "ll_b", "cc_max_b", "cc_min_b")
            .over("bars_in_day"),
        )
        .with_cols(
            col(
                col("hh_a") - col("cc_min_a"),
                col("cc_max_a") - col("ll_a"),
            ).max(axis=1).alias("span_up"),
            col(
                col("hh_b") - col("cc_min_b"),
                col("cc_max_b") - col("ll_b"),
            ).max(axis=1).alias("span_dn"),
        )
        .with_cols(
            (col("hhmm") >= start_hhmm * 100.0)
            .fill_null(False)
            .alias("time_ok"),
        )
        .with_cols(
            (col("time_ok") & (col("high") >= (col("day_open") + col("span_up") * up_k)))
            .fill_null(False)
            .alias("open_long_cond"),
            (col("time_ok") & (col("low") <= (col("day_open") - col("span_dn") * down_k)))
            .fill_null(False)
            .alias("open_short_cond"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_cond", "open_short_cond", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "t01",
        "按 T01 逻辑实现：基于 bars_in_day 对齐跨日同进度数据，组合 day_high/day_low/close 的滚动区间波动生成阈值，超过起始时间后突破阈值开仓，反向信号触发反手。",
    )

def build_t02_expr() -> Expr:
    par_1 = pms("t02_par_1", 1, 20, 2).limit(20)
    par_2 = pms("t02_par_2", 2, 80, 12).limit(20)
    par_3 = pms("t02_par_3", 0.1, 10.0, 4.25).limit(20)
    par_4 = pms("t02_par_4", 1.0, 120.0, 32.0).limit(20)
    par_5 = pms("t02_par_5", 0.1, 20.0, 2.0).limit(20)
    par_6 = pms("t02_par_6", 0.1, 20.0, 1.0).limit(20)
    par_7 = pms("t02_par_7", 5, 200, 86).limit(20)
    par_8 = pms("t02_par_8", 0.1, 20.0, 6.0).limit(20)
    par_9 = pms("t02_par_9", 0, 2359, 930).limit(20)
    par_10 = pms("t02_par_10", 0, 2359, 1400).limit(20)
    e = (
        _full_kline_expr()
        .with_cols(
            (
                col("datetime").dt.hour().cast(pl.Int32) * 100
                + col("datetime").dt.minute().cast(pl.Int32)
            ).alias("hhmm"),
        )
        .with_cols(
            (col("high") - col("low")).alias("hl"),
            col("high", "low", "close").ta.atr(par_7).expanding().alias("atr"),
            col("open").mean().rolling(par_2).alias("open_mean"),
        )
        .with_cols(
            col('hl', 'atr', 'high', 'low').shift(1).expanding().alias('hl_1', 'atr_prev', 'high_1', 'low_1'),
        )
        .with_cols(
            col("hl_1").mean().rolling(par_1).alias("hl_prev"),
        )
        .with_cols(
            (col("open_mean") + col("hl_prev") * par_3).alias("entry_up"),
            (col("open_mean") - col("hl_prev") * par_3).alias("entry_dn"),
            ((col("hhmm") > par_9) & (col("hhmm") < par_10))
            .fill_null(False)
            .alias("time_ok"),
        )
        .with_cols(
            (col("time_ok") & (col("high") >= col("entry_up")))
            .fill_null(False)
            .alias("open_long_raw"),
            (col("time_ok") & (col("low") <= col("entry_dn")))
            .fill_null(False)
            .alias("open_short_raw"),
            col("open", "entry_up").max(axis=1).alias("entry_long_now"),
            col("open", "entry_dn").min(axis=1).alias("entry_short_now"),
        )
        .with_cols(
            (col("open_long_raw") & (~col("open_short_raw")))
            .fill_null(False)
            .alias("open_long_base"),
            col("open_short_raw").fill_null(False).alias("close_long_rev"),
            col("open_short_raw").fill_null(False).alias("open_short_base"),
            (col("open_long_raw") & (~col("open_short_raw")))
            .fill_null(False)
            .alias("close_short_rev"),
        )
        # long pass0
        .with_cols(
            col("open_long_base", "close_long_rev")
            .stra
            .to_hold_one_side()
            .expanding()
            .alias("hold_l0"),
        )
        .with_cols(
            col("hold_l0")
            .stra
            .hold_to_one_side_signals(1.0)
            .select(
                col("open_sig", "exit_sig").alias("open_l0", "exit_l0"),
            ),
        )
        .with_cols(
            col("open_l0").count_last().expanding().alias("bars_open_l0"),
            col("open_l0", "entry_long_now", col.null).if_else()
            .alias("entry_raw_l0"),
        )
        .with_cols(
            (col("bars_open_l0").fill_null(0) + 1).cast(pl.UInt32).alias("bars_open_l0_w"),
            col("entry_raw_l0").ffill().expanding().alias("entry_ffill_l0"),
        )
        .with_cols(
            col(col("hold_l0") == 1.0, col("entry_ffill_l0"), col.null).if_else()
            .alias("entry_l0"),
        )
        .with_cols(
            col("open_l0", "entry_l0", "high_1").if_else()
            .alias("favorable_l0"),
        )
        .with_cols(
            col("favorable_l0").max().expanding_since("open_l0").alias("best_l0_raw"),
        )
        .with_cols(
            col(col("hold_l0") == 1.0, col("best_l0_raw"), col.null).if_else()
            .alias("best_l0"),
        )
        .with_cols(
            col(col("best_l0") >= col("entry_l0") * (1.0 + par_5 / 1000.0), col("best_l0") * (1.0 - par_6 / 1000.0), col("entry_l0") * (1.0 - par_4 / 1000.0)).if_else()
            .alias("stop_line_l0"),
            (col("best_l0") - par_8 * col("atr_prev")).alias("atr_line_l0"),
        )
        .with_cols(
            (
                (col("hold_l0") == 1.0)
                & (~col("open_l0"))
                & (
                    (col("low") <= col("stop_line_l0"))
                    | (col("low") <= col("atr_line_l0"))
                )
            )
            .fill_null(False)
            .alias("stop_l0"),
        )
        .with_cols(col("close_long_rev", "stop_l0").any(axis=1).alias("close_l1"))
        # long pass1
        .with_cols(
            col("open_long_base", "close_l1")
            .stra
            .to_hold_one_side()
            .expanding()
            .alias("hold_l1"),
        )
        .with_cols(
            col("hold_l1")
            .stra
            .hold_to_one_side_signals(1.0)
            .select(
                col("open_sig", "exit_sig").alias("open_l1", "exit_l1"),
            ),
        )
        .with_cols(
            col("open_l1").count_last().expanding().alias("bars_open_l1"),
            col("open_l1", "entry_long_now", col.null).if_else()
            .alias("entry_raw_l1"),
        )
        .with_cols(
            (col("bars_open_l1").fill_null(0) + 1).cast(pl.UInt32).alias("bars_open_l1_w"),
            col("entry_raw_l1").ffill().expanding().alias("entry_ffill_l1"),
        )
        .with_cols(
            col(col("hold_l1") == 1.0, col("entry_ffill_l1"), col.null).if_else()
            .alias("entry_l1"),
        )
        .with_cols(
            col("open_l1", "entry_l1", "high_1").if_else()
            .alias("favorable_l1"),
        )
        .with_cols(
            col("favorable_l1").max().expanding_since("open_l1").alias("best_l1_raw"),
        )
        .with_cols(
            col(col("hold_l1") == 1.0, col("best_l1_raw"), col.null).if_else()
            .alias("best_l1"),
        )
        .with_cols(
            col(col("best_l1") >= col("entry_l1") * (1.0 + par_5 / 1000.0), col("best_l1") * (1.0 - par_6 / 1000.0), col("entry_l1") * (1.0 - par_4 / 1000.0)).if_else()
            .alias("stop_line_l1"),
            (col("best_l1") - par_8 * col("atr_prev")).alias("atr_line_l1"),
        )
        .with_cols(
            (
                (col("hold_l1") == 1.0)
                & (~col("open_l1"))
                & (
                    (col("low") <= col("stop_line_l1"))
                    | (col("low") <= col("atr_line_l1"))
                )
            )
            .fill_null(False)
            .alias("stop_l1"),
        )
        .with_cols(
            col("close_long_rev", "stop_l1").any(axis=1).alias("close_l2"),
        )
        .with_cols(
            col("open_long_base", "close_l2")
            .stra
            .to_hold_one_side()
            .expanding()
            .alias("hold_long"),
        )
        # short pass0
        .with_cols(
            col("open_short_base", "close_short_rev")
            .stra
            .to_hold_one_side(-1.0)
            .expanding()
            .alias("hold_s0"),
        )
        .with_cols(
            col("hold_s0")
            .stra
            .hold_to_one_side_signals(-1.0)
            .select(
                col("open_sig", "exit_sig").alias("open_s0", "exit_s0"),
            ),
        )
        .with_cols(
            col("open_s0").count_last().expanding().alias("bars_open_s0"),
            col("open_s0", "entry_short_now", col.null).if_else()
            .alias("entry_raw_s0"),
        )
        .with_cols(
            (col("bars_open_s0").fill_null(0) + 1).cast(pl.UInt32).alias("bars_open_s0_w"),
            col("entry_raw_s0").ffill().expanding().alias("entry_ffill_s0"),
        )
        .with_cols(
            col(col("hold_s0") == -1.0, col("entry_ffill_s0"), col.null).if_else()
            .alias("entry_s0"),
        )
        .with_cols(
            col("open_s0", "entry_s0", "low_1").if_else()
            .alias("favorable_s0"),
        )
        .with_cols(
            col("favorable_s0").min().expanding_since("open_s0").alias("best_s0_raw"),
        )
        .with_cols(
            col(col("hold_s0") == -1.0, col("best_s0_raw"), col.null).if_else()
            .alias("best_s0"),
        )
        .with_cols(
            col(col("best_s0") <= col("entry_s0") * (1.0 - par_5 / 1000.0), col("best_s0") * (1.0 + par_6 / 1000.0), col("entry_s0") * (1.0 + par_4 / 1000.0)).if_else()
            .alias("stop_line_s0"),
            (col("best_s0") + par_8 * col("atr_prev")).alias("atr_line_s0"),
        )
        .with_cols(
            (
                (col("hold_s0") == -1.0)
                & (~col("open_s0"))
                & (
                    (col("high") >= col("stop_line_s0"))
                    | (col("high") >= col("atr_line_s0"))
                )
            )
            .fill_null(False)
            .alias("stop_s0"),
        )
        .with_cols(col("close_short_rev", "stop_s0").any(axis=1).alias("close_s1"))
        # short pass1
        .with_cols(
            col("open_short_base", "close_s1")
            .stra
            .to_hold_one_side(-1.0)
            .expanding()
            .alias("hold_s1"),
        )
        .with_cols(
            col("hold_s1")
            .stra
            .hold_to_one_side_signals(-1.0)
            .select(
                col("open_sig", "exit_sig").alias("open_s1", "exit_s1"),
            ),
        )
        .with_cols(
            col("open_s1").count_last().expanding().alias("bars_open_s1"),
            col("open_s1", "entry_short_now", col.null).if_else()
            .alias("entry_raw_s1"),
        )
        .with_cols(
            (col("bars_open_s1").fill_null(0) + 1).cast(pl.UInt32).alias("bars_open_s1_w"),
            col("entry_raw_s1").ffill().expanding().alias("entry_ffill_s1"),
        )
        .with_cols(
            col(col("hold_s1") == -1.0, col("entry_ffill_s1"), col.null).if_else()
            .alias("entry_s1"),
        )
        .with_cols(
            col("open_s1", "entry_s1", "low_1").if_else()
            .alias("favorable_s1"),
        )
        .with_cols(
            col("favorable_s1").min().expanding_since("open_s1").alias("best_s1_raw"),
        )
        .with_cols(
            col(col("hold_s1") == -1.0, col("best_s1_raw"), col.null).if_else()
            .alias("best_s1"),
        )
        .with_cols(
            col(col("best_s1") <= col("entry_s1") * (1.0 - par_5 / 1000.0), col("best_s1") * (1.0 + par_6 / 1000.0), col("entry_s1") * (1.0 + par_4 / 1000.0)).if_else()
            .alias("stop_line_s1"),
            (col("best_s1") + par_8 * col("atr_prev")).alias("atr_line_s1"),
        )
        .with_cols(
            (
                (col("hold_s1") == -1.0)
                & (~col("open_s1"))
                & (
                    (col("high") >= col("stop_line_s1"))
                    | (col("high") >= col("atr_line_s1"))
                )
            )
            .fill_null(False)
            .alias("stop_s1"),
        )
        .with_cols(
            col("close_short_rev", "stop_s1").any(axis=1).alias("close_s2"),
        )
        .with_cols(
            col("open_short_base", "close_s2")
            .stra
            .to_hold_one_side(-1.0)
            .expanding()
            .alias("hold_short"),
        )
        .with_cols(
            col("hold_long")
            .stra
            .hold_to_one_side_signals(1.0)
            .select(
                col("open_sig", "exit_sig").alias("open_long_sig", "exit_long_sig"),
            ),
            col("hold_short")
            .stra
            .hold_to_one_side_signals(-1.0)
            .select(
                col("open_sig", "exit_sig").alias("open_short_sig", "exit_short_sig"),
            ),
        )
        .select(
            "open_long_sig",
            "open_short_sig",
            "exit_long_sig",
            "exit_short_sig",
        )
    )
    return _with_meta(
        e,
        "t02",
        "按 T02 PreBreakATR 实现：拆成小算子组合（entry_price 跟踪、best_since_entry 跟踪）并通过多次状态收敛构造四路开平仓信号。",
    )

def build_t03_expr() -> Expr:
    length = pms("t03_length", 2, 80, 2).limit(20)
    length_rsi = pms("t03_length_rsi", 2, 80, 2).limit(20)
    mtm_length = pms("t03_mtm_length", 2, 80, 2).limit(20)
    longband = pms("t03_longband", 1.0, 95.0, 10.0).limit(20)
    shortband = pms("t03_shortband", 5.0, 99.0, 90.0).limit(20)
    stop_n = pms("t03_stop_n", 2, 80, 2).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").shift(length).expanding().alias("close_len"),
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
            col("high", "low", "close").ta.trange().expanding().alias("tr"),
            col("close").ta.rsi(length_rsi).expanding().alias("rsi_v"),
        )
        .with_cols(
            (col("close") - col("close_len")).alias("mtm"),
            col("tr").mean().rolling(stop_n).alias("stop_point"),
        )
        .with_cols(
            col("mtm").mean().rolling(mtm_length).alias("mtm_ma"),
        )
        .with_cols(
            col("rsi_v", "mtm", "mtm_ma").shift(1).expanding().add_suffix("1"),
            col("mtm", "mtm_ma").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            (
                (col("mtm_1") > col("mtm_ma_1"))
                & (col("mtm_2") <= col("mtm_ma_2"))
                & (col("rsi_v_1") >= longband)
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("mtm_1") < col("mtm_ma_1"))
                & (col("mtm_2") >= col("mtm_ma_2"))
                & (col("rsi_v_1") <= shortband)
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            (col("open_long_raw") & (~col("open_long_raw")))
            .fill_null(False)
            .alias("exit_long_seed"),
            (col("open_short_raw") & (~col("open_short_raw")))
            .fill_null(False)
            .alias("exit_short_seed"),
        )
        # pass0: 无止损先得到第一轮持仓
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_seed", "open_short_raw", "exit_short_seed", "hold_0"),
        )
        .with_cols(
            col("hold_0")
            .stra
            .hold_to_one_side_signals(1.0)
            .select(
                col("open_sig", "exit_sig").alias("open_l0", "exit_l0"),
            ),
            col("hold_0")
            .stra
            .hold_to_one_side_signals(-1.0)
            .select(
                col("open_sig", "exit_sig").alias("open_s0", "exit_s0"),
            ),
        )
        .with_cols(
            col("open_l0", "open_s0").count_last().expanding().alias("bars_open_l0", "bars_open_s0"),
        )
        .with_cols(
            (col("bars_open_l0").fill_null(0) + 1).cast(pl.UInt32).alias("bars_open_l0_w"),
            (col("bars_open_s0").fill_null(0) + 1).cast(pl.UInt32).alias("bars_open_s0_w"),
        )
        .with_cols(
            col("high").max().expanding_since("open_l0").alias("high_after_l0_raw"),
            col("low").min().expanding_since("open_s0").alias("low_after_s0_raw"),
        )
        .with_cols(
            col(col("hold_0") == 1.0, col("high_after_l0_raw"), col.null).if_else()
            .alias("high_after_l0"),
            col(col("hold_0") == -1.0, col("low_after_s0_raw"), col.null).if_else()
            .alias("low_after_s0"),
        )
        .with_cols(
            col('high_after_l0', 'low_after_s0').shift(2).expanding().add_suffix("2"),
            (col("low", "low_1").min(axis=1) - col("stop_point")).alias("long_stop_seed_0"),
            (col("high", "high_1").max(axis=1) + col("stop_point")).alias("short_stop_seed_0"),
        )
        .with_cols(
            (
                col("open_l0")
                | (
                    (col("hold_0") == 1.0)
                    & (col("bars_open_l0") > 0)
                    & (col("close_1") > col("high_after_l0_2"))
                )
            )
            .fill_null(False)
            .alias("reset_long_0"),
            (
                col("open_s0")
                | (
                    (col("hold_0") == -1.0)
                    & (col("bars_open_s0") > 0)
                    & (col("close_1") < col("low_after_s0_2"))
                )
            )
            .fill_null(False)
            .alias("reset_short_0"),
        )
        .with_cols(
            col("reset_long_0", "long_stop_seed_0", col.null).if_else()
            .alias("long_stop_raw_0"),
            col("reset_short_0", "short_stop_seed_0", col.null).if_else()
            .alias("short_stop_raw_0"),
        )
        .with_cols(
            col("long_stop_raw_0").ffill().expanding().alias("long_stop_0"),
            col("short_stop_raw_0").ffill().expanding().alias("short_stop_0"),
        )
        .with_cols(
            ((col("hold_0") == 1.0) & (col("low") <= col("long_stop_0")))
            .fill_null(False)
            .alias("exit_long_0"),
            ((col("hold_0") == -1.0) & (col("high") >= col("short_stop_0")))
            .fill_null(False)
            .alias("exit_short_0"),
        )
        # pass1: 用第一轮止损重算持仓，再重算一次止损
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_0", "open_short_raw", "exit_short_0", "hold_1"),
        )
        .with_cols(
            col("hold_1")
            .stra
            .hold_to_one_side_signals(1.0)
            .select(
                col("open_sig", "exit_sig").alias("open_l1", "exit_l1"),
            ),
            col("hold_1")
            .stra
            .hold_to_one_side_signals(-1.0)
            .select(
                col("open_sig", "exit_sig").alias("open_s1", "exit_s1"),
            ),
        )
        .with_cols(
            col("open_l1", "open_s1").count_last().expanding().alias("bars_open_l1", "bars_open_s1"),
        )
        .with_cols(
            (col("bars_open_l1").fill_null(0) + 1).cast(pl.UInt32).alias("bars_open_l1_w"),
            (col("bars_open_s1").fill_null(0) + 1).cast(pl.UInt32).alias("bars_open_s1_w"),
        )
        .with_cols(
            col("high").max().expanding_since("open_l1").alias("high_after_l1_raw"),
            col("low").min().expanding_since("open_s1").alias("low_after_s1_raw"),
        )
        .with_cols(
            col(col("hold_1") == 1.0, col("high_after_l1_raw"), col.null).if_else()
            .alias("high_after_l1"),
            col(col("hold_1") == -1.0, col("low_after_s1_raw"), col.null).if_else()
            .alias("low_after_s1"),
        )
        .with_cols(
            col('high_after_l1', 'low_after_s1').shift(2).expanding().add_suffix("2"),
            (col("low", "low_1").min(axis=1) - col("stop_point")).alias("long_stop_seed_1"),
            (col("high", "high_1").max(axis=1) + col("stop_point")).alias("short_stop_seed_1"),
        )
        .with_cols(
            (
                col("open_l1")
                | (
                    (col("hold_1") == 1.0)
                    & (col("bars_open_l1") > 0)
                    & (col("close_1") > col("high_after_l1_2"))
                )
            )
            .fill_null(False)
            .alias("reset_long_1"),
            (
                col("open_s1")
                | (
                    (col("hold_1") == -1.0)
                    & (col("bars_open_s1") > 0)
                    & (col("close_1") < col("low_after_s1_2"))
                )
            )
            .fill_null(False)
            .alias("reset_short_1"),
        )
        .with_cols(
            col("reset_long_1", "long_stop_seed_1", col.null).if_else()
            .alias("long_stop_raw_1"),
            col("reset_short_1", "short_stop_seed_1", col.null).if_else()
            .alias("short_stop_raw_1"),
        )
        .with_cols(
            col("long_stop_raw_1").ffill().expanding().alias("long_stop_1"),
            col("short_stop_raw_1").ffill().expanding().alias("short_stop_1"),
        )
        .with_cols(
            ((col("hold_1") == 1.0) & (col("low") <= col("long_stop_1")))
            .fill_null(False)
            .alias("exit_long_1"),
            ((col("hold_1") == -1.0) & (col("high") >= col("short_stop_1")))
            .fill_null(False)
            .alias("exit_short_1"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_1", "open_short_raw", "exit_short_1", "hold_f"),
        )
        .select(
            _four_signals_from_pos_expr("hold_f")
        )
    )
    return _with_meta(
        e,
        "t03",
        "按 T03 QM_MTMcRSI 源码实现：MTM[1]/MTMMA[1] 交叉 + RSI[1] 阈值入场，止损线按 bars_since_entry 与入场后极值条件动态抬升/下移，并用两轮状态收敛重算持仓。",
    )

def build_t04_expr() -> Expr:
    length1 = pms("t04_length1", 2, 80, 11).limit(20)
    length2 = pms("t04_length2", 2, 120, 42).limit(20)
    length3 = pms("t04_length3", 2, 160, 53).limit(20)
    length4 = pms("t04_length4", 20, 400, 178).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").shift(1).expanding().add_suffix("1"),
            col("close").shift(2).expanding().add_suffix("2"),
            col("close").shift(9).expanding().add_suffix("9"),
            col("close").shift(138).expanding().add_suffix("138"),
            col("close").shift(length1).expanding().alias("close_l1"),
            col("close").shift(length4).expanding().alias("close_l4"),
            col("open").shift(1).expanding().add_suffix("1"),
            col("open").shift(length2).expanding().alias("open_l2"),
            col("open").shift(length3).expanding().alias("open_l3"),
        )
        .with_cols(
            (
                (col("close_2") > col("close_l1"))
                & (col("close_9") > col("close_l4"))
                & (col("close_1") > col("open_1"))
                & (col("close_138") > col("open_l2"))
                & (col("close_1") > col("open_l3"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("close_2") < col("close_l1"))
                & (col("close_9") < col("close_l4"))
                & (col("close_1") < col("open_1"))
                & (col("close_138") < col("open_l2"))
                & (col("close_1") < col("open_l3"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "t04",
        "按 T04 日本云图源码实现：基于多组跨周期的 close/open 位移关系构造多空条件，满足条件时开仓，反向条件触发时反手平仓。",
    )

def build_t05_expr() -> Expr:
    n1 = pms("t05_n1", 1, 80, 1).limit(20)
    n2 = pms("t05_n2", 10, 400, 10).limit(20)
    n3 = pms("t05_n3", 1, 160, 1).limit(20)
    alpha2 = 2.0 / (n2 + 1.0)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").mean().rolling(n1).alias("ma_n1"),
            col("close").ewm(alpha2).expanding().alias("ema_n2"),
        )
        .with_cols(
            (col("ma_n1") - col("ema_n2")).alias("os"),
        )
        .with_cols(
            col("os").mean().rolling(n3).alias("aos"),
        )
        .with_cols(
            col("aos").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("aos") > 0.0) & (col("aos_1") <= 0.0))
            .fill_null(False)
            .alias("open_long_raw"),
            ((col("aos") < 0.0) & (col("aos_1") >= 0.0))
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "t05",
        "按 T05 RUMI 源码实现：OS=MA(close,N1)-EMA(close,N2)，AOS=MA(OS,N3)，AOS 上穿/下穿 0 触发多空开仓，反向信号用于反手平仓。",
    )

def build_t06_expr() -> Expr:
    period = pms("t06_period", 2, 120, 2).limit(20)
    period1 = pms("t06_period1", 2, 60, 2).limit(20)
    filt = pms("t06_filter", 0.1, 20.0, 0.1).limit(20)
    filter_period = pms("t06_filter_period", 2, 160, 2).limit(20)
    smooth_len = pms("t06_smooth_len", 1.0, 8.0, 1.0).limit(20)
    eff = pms("t06_eff", 0.1, 1.0, 0.1).limit(20)
    dema_len = pms("t06_dema_len", 2, 80, 2).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close")
            .stra
            .t06_ama_band(period, period1, filt, filter_period, smooth_len, eff, dema_len)
            .expanding(),
        )
        .with_cols(
            col("ama").shift(1).expanding().add_suffix("1"),
            col("ama").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            col(
                col(col("ama") > col("ama_1"), col("ama_1") < col("ama_2")).all(axis=1).fill_null(False),
                col("ama_1"),
                col.null,
            )
            .if_else()
            .alias("ext_low_seed"),
            col(
                col(col("ama") < col("ama_1"), col("ama_1") > col("ama_2")).all(axis=1).fill_null(False),
                col("ama_1"),
                col.null,
            )
            .if_else()
            .alias("ext_high_seed"),
        )
        .with_cols(
            col("ext_low_seed").ffill().expanding().alias("ext_low"),
            col("ext_high_seed").ffill().expanding().alias("ext_high"),
        )
        .with_cols(
            col(
                col("ama") > col("ama_1"),
                col("ext_low") + col("ifilter"),
                col.null,
            )
            .if_else()
            .alias("pending_long_stop"),
            col(
                col("ama") < col("ama_1"),
                col("ext_high") - col("ifilter"),
                col.null,
            )
            .if_else()
            .alias("pending_short_stop"),
        )
        .with_cols(
            col('pending_long_stop', 'pending_short_stop').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col(
                col("pending_long_stop_1").is_not_null(),
                col("high") >= col("pending_long_stop_1"),
            )
            .all(axis=1)
            .fill_null(False)
            .alias("open_long_raw"),
            col(
                col("pending_short_stop_1").is_not_null(),
                col("low") <= col("pending_short_stop_1"),
            )
            .all(axis=1)
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "t06",
        "按 T06 碧血剑源码实现：基于效率比自适应均线与拐点带宽构造下一根 stop 触发入场，反向触发时反手离场。",
    )

def build_t07_expr() -> Expr:
    fast_ma = pms("t07_fast_ma", 2, 80, 2).limit(20)
    slow_ma = pms("t07_slow_ma", 4, 160, 4).limit(20)
    macd_ma = pms("t07_macd_ma", 2, 80, 2).limit(20)
    ncos = pms("t07_ncos", 1, 20, 1).limit(20)
    nbars = pms("t07_nbars", 5, 300, 5).limit(20)
    trail_bar = pms("t07_trail_bar", 1, 60, 1).limit(20)
    atr_n = pms("t07_atr_n", 2, 30, 2).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("high", "low", "close").ta.trange().expanding().alias("tr"),
            col("close").ta.ema(fast_ma).expanding().alias("ema_fast"),
            col("close").ta.ema(slow_ma).expanding().alias("ema_slow"),
        )
        .with_cols(
            (col("ema_fast") - col("ema_slow")).alias("macd_v"),
            col("tr").mean().rolling(atr_n).alias("atr"),
        )
        .with_cols(
            col("macd_v").ta.ema(macd_ma).expanding().alias("macd_sig"),
        )
        .with_cols(
            (col("macd_v") - col("macd_sig")).alias("mdif"),
        )
        .with_cols(
            col("mdif").stra.cross_up.fill_null(False).alias("cross_up"),
            col("mdif").stra.cross_down.fill_null(False).alias("cross_down"),
        )
        .with_cols(
            col("cross_up", "cross_down").any(axis=1).fill_null(False).alias("cross"),
        )
        .select(
            col("cross", "high", "low", "atr")
            .stra
            .t07_range_orders(ncos, nbars, trail_bar)
            .expanding()
        )
    )
    return _with_meta(
        e,
        "t07",
        "按 T07 源码实现：记录 MACD 零轴交叉后的区间高低并在限定 bars 内挂 stop 入场，持仓后按 TrailBar 的区间止损线出场。",
    )

def build_t08_expr() -> Expr:
    malen1 = pms("t08_malen1", 1, 40, 1).limit(20)
    malen2 = pms("t08_malen2", 20, 220, 20).limit(20)
    bolen = pms("t08_bolen", 2, 180, 2).limit(20)
    pp = pms("t08_pp", 0.0, 4.0, 0.0).limit(20)
    pp2 = pms("t08_pp2", 0.0, 4.0, 0.0).limit(20)
    dqsma = pms("t08_dqsma", 20, 500, 20).limit(20)
    atr_n = pms("t08_atr_n", 2, 100, 2).limit(20)

    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").mean().rolling(malen1).alias("av_ma1"),
            col("close").mean().rolling(malen2).alias("av_ma2"),
            col("close").mean().rolling(dqsma).alias("av_dq"),
            col("high").max().rolling(bolen).alias("highest_bolen"),
            col("low").min().rolling(bolen).alias("lowest_bolen"),
        )
        .with_cols(
            col("high", "low", "close").ta.trange().expanding().alias("tr")
        )
        .with_cols(
            col("tr").mean().rolling(atr_n).alias("av_range"),
        )
        .with_cols(
            (col("highest_bolen") + col("av_range") * pp).alias("entry_up"),
            (col("lowest_bolen") - col("av_range") * pp2).alias("entry_dn"),
            ((col("av_ma1") > col("av_ma2")) & (col("close") > col("av_dq")))
            .fill_null(False)
            .alias("open_long_order"),
            ((col("av_ma1") < col("av_ma2")) & (col("close") < col("av_dq")))
            .fill_null(False)
            .alias("open_short_order"),
            (col("av_ma1") < col("av_ma2")).fill_null(False).alias("exit_long_order"),
            (col("av_ma1") > col("av_ma2")).fill_null(False).alias("exit_short_order"),
        )
        .with_cols(
            col('open_long_order', 'open_short_order', 'exit_long_order', 'exit_short_order', 'entry_up', 'entry_dn').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("open_long_order_1") & (col("high") >= col("entry_up_1")))
            .fill_null(False)
            .alias("open_long_raw"),
            (col("open_short_order_1") & (col("low") <= col("entry_dn_1")))
            .fill_null(False)
            .alias("open_short_raw"),
            (col("exit_long_order_1") & (col("low") <= col("entry_dn_1")))
            .fill_null(False)
            .alias("exit_long_raw"),
            (col("exit_short_order_1") & (col("high") >= col("entry_up_1")))
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos_raw"),
        )
        .select(_four_signals_from_pos_expr("pos_raw"))
    )
    return _with_meta(
        e,
        "t08",
        "按 T08 屠龙刀期货源码实现：均线方向+大周期均线过滤后，次根 bar 触发 Donchian+ATR 停损单开仓；趋势反向时按对侧停损位平仓。",
    )

def build_t09_expr() -> Expr:
    length1 = pms("t09_length1", 2, 120, 2).limit(20)
    length2 = pms("t09_length2", 2, 120, 2).limit(20)
    hl_length = pms("t09_hl_length", 2, 120, 2).limit(20)
    range_limit = pms("t09_range_limit", 0.5, 10.0, 0.5).limit(20)
    loop_limit = pms("t09_loop_limit", 1, 12, 1).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("volume").mean().rolling(length1).alias("vol_ma"),
            (col("high") - col("low")).alias("hl_raw"),
        )
        .with_cols(
            col("vol_ma").mean().rolling(length2).alias("vol_ma2"),
            col("hl_raw").mean().rolling(hl_length).alias("hl"),
            col('close', 'open').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("vol_ma", "vol_ma2", "hl").shift(1).expanding().add_suffix("1"),
            col("vol_ma", "vol_ma2").shift(2).expanding().add_suffix("2"),
            col(col("close_1") > col("open_1"), 1.0, -1.0).if_else()
            .alias("bar_value_1"),
        )
        .with_cols(
            col("bar_value_1").sum().rolling(loop_limit).alias("bar_value_sum"),
            (
                (col("vol_ma_1") > col("vol_ma2_1"))
                & (col("vol_ma_2") > col("vol_ma2_2"))
            )
            .fill_null(False)
            .alias("up_con"),
            (col("open") - range_limit * col("hl_1")).alias("long_stop"),
            (col("open") + range_limit * col("hl_1")).alias("short_stop"),
        )
        .with_cols(
            (
                col("up_con")
                & (col("bar_value_sum") == loop_limit)
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                col("up_con")
                & (col("bar_value_sum") == (loop_limit * -1.0))
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            col("open_long_sig", "open_short_sig").any(axis=1).fill_null(False).alias("open_any_pre"),
        )
        .with_cols(
            col("open_any_pre").count_last().expanding().alias("bars_since_entry"),
        )
        .with_cols(
            _bars_in_pos_expr("pos_pre", "bars_since_entry", "bars_in_pos"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_in_pos") > 0)
                & (col("low") <= col("long_stop"))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_in_pos") > 0)
                & (col("high") >= col("short_stop"))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "t09",
        "按 T09 AK47 期货源码实现：VolMA 双重平滑与近 LoopLimit 根 K 线方向一致时开仓，并按 Open±RangeLimit*HL[1] 做保护止损。",
    )

def build_t10_expr() -> Expr:
    high_low_line = pms("t10_high_low_line", 10, 300, 10).limit(20)
    fast_len = pms("t10_fast_len", 2, 120, 2).limit(20)
    slow_len = pms("t10_slow_len", 0.01, 2.0, 0.01).limit(20)
    fast_len_l = pms("t10_fast_len_l", 2, 120, 2).limit(20)
    trailing_start = pms("t10_trailing_start", 1.0, 500.0, 1.0).limit(20)
    trailing_stop = pms("t10_trailing_stop", 1.0, 100.0, 1.0).limit(20)
    min_point = pms("t10_min_point", 0.1, 20.0, 0.1).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").ta.ema(fast_len).expanding().alias("ema_close_fast"),
            col("open").ta.ema(fast_len).expanding().alias("ema_open_fast"),
            col("close").ta.ema(fast_len_l).expanding().alias("ema_close_fast_l"),
            col("open").ta.ema(fast_len_l).expanding().alias("ema_open_fast_l"),
            col("high").max().rolling(high_low_line).alias("donch_hi"),
            col("low").min().rolling(high_low_line).alias("donch_lo"),
        )
        .with_cols(
            (
                (col("ema_close_fast") > col("ema_open_fast"))
                & ((col("ema_close_fast") - col("ema_open_fast")) > slow_len * 0.1)
            )
            .fill_null(False)
            .alias("cond_long"),
            (
                (col("ema_close_fast_l") < col("ema_open_fast_l"))
                & ((col("ema_open_fast_l") - col("ema_close_fast_l")) > slow_len * 0.1)
            )
            .fill_null(False)
            .alias("cond_short"),
        )
        .with_cols(
            col('cond_long', 'cond_short', 'donch_hi', 'donch_lo').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("cond_long_1") & (col("high") >= col("donch_hi_1")))
            .fill_null(False)
            .alias("open_long_entry"),
            (col("cond_short_1") & (col("low") <= col("donch_lo_1")))
            .fill_null(False)
            .alias("open_short_entry"),
            col("open", "donch_hi_1").max(axis=1).alias("entry_long_now"),
            col("open", "donch_lo_1").min(axis=1).alias("entry_short_now"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_entry", "open_short_entry", "pos_entry"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_entry"),
        )
        .with_cols(
            col("open_long_sig", "open_short_sig").any(axis=1).fill_null(False).alias("open_any_entry"),
            _entry_seed_expr(),
        )
        .with_cols(
            col("entry_seed").ffill().expanding().alias("entry_price"),
            col("open_any_entry").count_last().expanding().alias("bars_since_entry"),
        )
        .with_cols(
            _bars_in_pos_expr("pos_entry", "bars_since_entry", "bars_in_pos"),
            col(col("bars_since_entry") == 0, col("close"), col("high")).if_else().alias("high_track"),
            col(col("bars_since_entry") == 0, col("close"), col("low")).if_else().alias("low_track"),
        )
        .with_cols(
            col("high_track").max().expanding_since("open_any_entry").alias("highest_after_entry"),
            col("low_track").min().expanding_since("open_any_entry").alias("lowest_after_entry"),
        )
        .with_cols(
            col((col("pos_entry") != 0.0)
                & (col("bars_in_pos") > 0)
                & (
                    col("highest_after_entry")
                    >= (col("entry_price") + trailing_start * min_point)
                ), col("highest_after_entry") - trailing_stop * min_point, col.null).if_else()
            .alias("rev_short_seed"),
            col((col("pos_entry") != 0.0)
                & (col("bars_in_pos") > 0)
                & (
                    col("lowest_after_entry")
                    <= (col("entry_price") - trailing_start * min_point)
                ), col("lowest_after_entry") + trailing_stop * min_point, col.null).if_else()
            .alias("rev_long_seed"),
        )
        .with_cols(col(
            "rev_short_seed",
            "rev_long_seed",
        ).shift(1).expanding().alias("rev_short_1", "rev_long_1"))
        .with_cols(
            (
                (col("pos_entry") != -1.0)
                & col("rev_short_1").is_not_null()
                & (col("low") <= col("rev_short_1"))
            )
            .fill_null(False)
            .alias("open_short_rev"),
            (
                (col("pos_entry") != 1.0)
                & col("rev_long_1").is_not_null()
                & (col("high") >= col("rev_long_1"))
            )
            .fill_null(False)
            .alias("open_long_rev"),
        )
        .with_cols(
            col("open_long_entry", "open_long_rev").any(axis=1).fill_null(False).alias("open_long_raw"),
            col("open_short_entry", "open_short_rev").any(axis=1).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "t10",
        "按 T10 八卦两仪源码实现：均线差分过滤后的 Donchian stop 入场，持仓后按进场后极值触发追踪反手。",
    )

def build_t11_expr() -> Expr:
    blen_pre = pms("t11_blen_pre", 2, 120, 2).limit(20)
    blen_vol = pms("t11_blen_vol", 2, 60, 2).limit(20)
    drop_start = pms("t11_drop_start", 1.0, 50.0, 1.0).limit(20)
    drop_k = pms("t11_drop_k", 0.5, 12.0, 0.5).limit(20)
    atr_n = pms("t11_atr_n", 2, 80, 2).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("high", "low", "close").shift(1).expanding().add_suffix("1"),
            col("high", "low").shift(2).expanding().add_suffix("2"),
            col("high", "low", "close").ta.trange().expanding().alias("tr"),
        )
        .with_cols(
            col("low").max().rolling(blen_pre).alias("pre_hp"),
            col("high").min().rolling(blen_pre).alias("pre_lp"),
            col("volume").mean().rolling(blen_vol).alias("mavol"),
            col("tr").mean().rolling(atr_n).alias("atr"),
        )
        .with_cols(
            col("pre_hp", "pre_lp", "mavol", "atr").shift(1).expanding().add_suffix("1"),
            col("pre_hp", "pre_lp", "mavol").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            (
                (col("low_1") >= col("pre_hp_1"))
                & (col("low_2") >= col("pre_lp_2"))
                & (col("high") >= col("high_1"))
                & (col("mavol_1") >= col("mavol_2"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("high_1") <= col("pre_lp_1"))
                & (col("high_2") <= col("pre_hp_2"))
                & (col("low") <= col("low_1"))
                & (col("mavol_1") >= col("mavol_2"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
            col("open", "high_1").max(axis=1).alias("entry_long_now"),
            col("open", "low_1").min(axis=1).alias("entry_short_now"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            col("open_long_sig", "open_short_sig").any(axis=1).fill_null(False).alias("open_any_pre"),
            _entry_seed_expr(),
        )
        .with_cols(
            col("entry_seed").ffill().expanding().alias("entry_price"),
            col("open_any_pre").count_last().expanding().alias("bars_since_entry"),
        )
        .with_cols(
            _bars_in_pos_expr("pos_pre", "bars_since_entry", "bars_in_pos"),
            col(
                col("bars_since_entry") == 0,
                col("entry_price"),
                col(
                    col("bars_since_entry") == 1,
                    col("close_1", "entry_price").max(axis=1),
                    col("high_1"),
                ).if_else(),
            ).if_else().alias("hi_track"),
            col(
                col("bars_since_entry") == 0,
                col("entry_price"),
                col(
                    col("bars_since_entry") == 1,
                    col("close_1", "entry_price").min(axis=1),
                    col("low_1"),
                ).if_else(),
            ).if_else().alias("lo_track"),
        )
        .with_cols(
            col("hi_track").max().expanding_since("open_any_pre").alias("hi_after_entry"),
            col("lo_track").min().expanding_since("open_any_pre").alias("lo_after_entry"),
        )
        .with_cols(
            (
                (col("hi_after_entry") >= col("entry_price") * (1.0 + drop_start * 0.001))
                | (col("lo_after_entry") <= col("entry_price") * (1.0 - drop_start * 0.001))
            )
            .fill_null(False)
            .alias("drop_start_flag"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_in_pos") >= 1)
                & col("drop_start_flag")
                & (col("low") <= (col("hi_after_entry") - drop_k * col("atr_1")))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_in_pos") >= 1)
                & col("drop_start_flag")
                & (col("high") >= (col("lo_after_entry") + drop_k * col("atr_1")))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "t11",
        "按 T11 VolBreak 源码实现：放量突破条件开仓，进场后在达到阈值后启动 ATR 吊灯止损离场（多空双向）。",
    )

def build_t12_expr() -> Expr:
    n = pms("t12_n", 2, 40, 2).limit(20)
    stoploss_set = pms("t12_stoplossset", 0.5, 10.0, 0.5).limit(20)
    length1 = pms("t12_length1", 2, 120, 2).limit(20)
    length2 = pms("t12_length2", 2, 120, 2).limit(20)
    length3 = pms("t12_length3", 2, 120, 2).limit(20)
    length4 = pms("t12_length4", 2, 200, 2).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").mean().rolling(length1).alias("ma3"),
            col("close").mean().rolling(length2).alias("ma6"),
            col("close").mean().rolling(length3).alias("ma12"),
            col("close").mean().rolling(length4).alias("ma24"),
            col("close").shift(1).expanding().add_suffix("1"),
            col("close").shift(2).expanding().add_suffix("2"),
            col("close").shift(3).expanding().add_suffix("3"),
            col("close").shift(4).expanding().add_suffix("4"),
            col("open").shift(1).expanding().add_suffix("1"),
            col("open").shift(2).expanding().add_suffix("2"),
            col("open").shift(3).expanding().add_suffix("3"),
            col("open").shift(4).expanding().add_suffix("4"),
            col('high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("ma3", "ma6", "ma12", "ma24").mean(axis=1).alias("myavg"),
            col("high_1").max().rolling(n).alias("a1"),
            col("low_1").min().rolling(n).alias("a2"),
            col("high", "low", "close").ta.trange().expanding().alias("tr"),
        )
        .with_cols(
            col("myavg").shift(1).expanding().add_suffix("1"),
            col("myavg").shift(2).expanding().add_suffix("2"),
            col("myavg").shift(3).expanding().add_suffix("3"),
            col("myavg").shift(4).expanding().add_suffix("4"),
            col("myavg").shift(5).expanding().add_suffix("5"),
            col("myavg").shift(6).expanding().add_suffix("6"),
            col("myavg").shift(7).expanding().add_suffix("7"),
            col("tr").mean().rolling(14).alias("atr"),
        )
        .with_cols(
            (
                (col("myavg_1") > col("myavg_2"))
                & (col("myavg_2") > col("myavg_3"))
                & (col("myavg_3") > col("myavg_4"))
                & (col("myavg_4") > col("myavg_5"))
                & (col("myavg_5") > col("myavg_6"))
                & (col("myavg_6") > col("myavg_7"))
            )
            .fill_null(False)
            .alias("condition1"),
            (
                (col("myavg_1") < col("myavg_2"))
                & (col("myavg_2") < col("myavg_3"))
                & (col("myavg_3") < col("myavg_4"))
                & (col("myavg_4") < col("myavg_5"))
                & (col("myavg_5") < col("myavg_6"))
                & (col("myavg_6") < col("myavg_7"))
            )
            .fill_null(False)
            .alias("condition2"),
            (
                (col("close_1") > col("close_2")).cast(pl.Int32)
                + (col("close_2") > col("close_3")).cast(pl.Int32)
                + (col("close_3") > col("close_4")).cast(pl.Int32)
            )
            .alias("panduan1"),
            (
                (col("close_1") > col("open_1")).cast(pl.Int32)
                + (col("close_2") > col("open_2")).cast(pl.Int32)
                + (col("close_3") > col("open_3")).cast(pl.Int32)
                + (col("close_4") > col("open_4")).cast(pl.Int32)
            )
            .alias("panduan2"),
            (
                (col("close_1") < col("close_2")).cast(pl.Int32)
                + (col("close_2") < col("close_3")).cast(pl.Int32)
                + (col("close_3") < col("close_4")).cast(pl.Int32)
            )
            .alias("panduan3"),
            (
                (col("close_1") < col("open_1")).cast(pl.Int32)
                + (col("close_2") < col("open_2")).cast(pl.Int32)
                + (col("close_3") < col("open_3")).cast(pl.Int32)
                + (col("close_4") < col("open_4")).cast(pl.Int32)
            )
            .alias("panduan4"),
        )
        .with_cols(
            (
                col("condition1")
                & (col("panduan1") == 2)
                & (col("panduan2") >= 3)
                & (col("close_1") > col("myavg_1"))
                & (col("close_1") > col("open_1"))
                & (col("high") > col("a1"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                col("condition2")
                & (col("panduan3") >= 1)
                & (col("panduan4") >= 2)
                & (col("close_1") < col("myavg_1"))
                & (col("close_1") < col("open_1"))
                & (col("low") < col("a2"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
            col("open", "a1").max(axis=1).alias("entry_long_now"),
            col("open", "a2").min(axis=1).alias("entry_short_now"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            col("open_long_sig", "open_short_sig").any(axis=1).fill_null(False).alias("open_any_pre"),
            _entry_seed_expr(),
            col("atr").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("entry_seed").ffill().expanding().alias("entry_price"),
            col("open_any_pre").count_last().expanding().alias("bars_since_entry"),
        )
        .with_cols(
            _bars_in_pos_expr("pos_pre", "bars_since_entry", "bars_in_pos"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_in_pos") > 0)
                & (col("low") < (col("entry_price") - stoploss_set * col("atr_1")))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_in_pos") > 0)
                & (col("high") > (col("entry_price") + stoploss_set * col("atr_1")))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "t12",
        "按 T12（黑剑期货 TB 版）实现：组合均线排列+突破开仓，并加入 LastEntryPrice±Stoplossset*ATR[1] 的止损平仓。",
    )

def build_t13_expr() -> Expr:
    k1 = pms("t13_k1", 2, 120, 2).limit(20)
    length = pms("t13_length", 2, 120, 2).limit(20)
    len1 = pms("t13_len1", 2, 240, 2).limit(20)
    len2 = pms("t13_len2", 2, 80, 2).limit(20)
    len3 = pms("t13_len3", 1, 40, 1).limit(20)
    m = pms("t13_m", 0.1, 10.0, 0.1).limit(20)
    atr_len = pms("t13_atr_len", 2, 80, 2).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col('high', 'low', 'close').shift(1).expanding().add_suffix("1"),
            col("close").shift(40).expanding().add_suffix("40"),
            col.lit(m).alias("m"),
        )
        .with_cols(
            col("close").max().rolling(len1).alias("hhv"),
            col("close").min().rolling(len1).alias("llv"),
            col("low").ta.ema(len2).expanding().alias("ma"),
            col("high", "low", "close").ta.atr(atr_len).expanding().alias("trange"),
        )
        .with_cols(
            col("hhv").shift(len3).expanding().alias("hhv_l3"),
            col("llv").shift(len3).expanding().alias("llv_l3"),
            col("ma").shift(len3).expanding().alias("ma_l3"),
        )
        .with_cols(
            (
                (col("low_1") >= col("hhv_l3"))
                & (col("high_1") >= col("ma_l3"))
                & (col("high") >= col("high_1"))
            )
            .fill_null(False)
            .alias("con1"),
            (
                (col("high_1") <= col("llv_l3"))
                & (col("low_1") <= col("ma_l3"))
                & (col("low") <= col("low_1"))
            )
            .fill_null(False)
            .alias("con2"),
            ((col("close") - col("close_40")).abs() >= col("m") * col("trange"))
            .fill_null(False)
            .alias("efficiency"),
        )
        .with_cols(
            col("efficiency").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("con1", "efficiency_1").all(axis=1).fill_null(False).alias("open_long_raw"),
            col("con2", "efficiency_1").all(axis=1).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "t13",
        "按 T13 源码实现：前序区间+均线条件突破并叠加效率过滤（|C-C[40]| 与 M*ATR 比较）开仓，反向信号反手。",
    )

def build_t14_expr() -> Expr:
    length = pms("t14_length", 2, 120, 18).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("open", "close").shift(1).expanding().add_suffix("1"),
            col("open", "close").shift(2).expanding().add_suffix("2"),
            col("close").shift(3).expanding().add_suffix("3"),
            col("close").shift(4).expanding().add_suffix("4"),
            col('high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("volume").mean().rolling(length).alias("vol_ma"),
        )
        .with_cols(
            col("vol_ma").mean().rolling(length).alias("vol_ma2"),
        )
        .with_cols(
            col("vol_ma", "vol_ma2").shift(1).expanding().add_suffix("1"),
            col("vol_ma").shift(2).expanding().add_suffix("2"),
            (
                col("open_1") * 0.1375
                + col("open") * 0.1375
                + col("close_1") * 0.1125
                + col("close") * 0.1125
                + col("high_1") * 0.125
                + col("high") * 0.125
                + col("low_1") * 0.125
                + col("low") * 0.125
            ).alias("aapv"),
        )
        .with_cols(
            col("aapv").shift(1).expanding().add_suffix("1"),
            col("aapv").shift(2).expanding().add_suffix("2"),
            col("aapv").shift(3).expanding().add_suffix("3"),
            col("aapv").shift(4).expanding().add_suffix("4"),
        )
        .with_cols(
            (
                (col("vol_ma_1") > col("vol_ma2_1"))
                & (col("vol_ma_2") > col("vol_ma2_1"))
            )
            .fill_null(False)
            .alias("up_con"),
            (
                (col("close_1") >= col("aapv_1"))
                & (col("close_2") >= col("aapv_2"))
                & (col("close_3") >= col("aapv_3"))
                & (col("close_4") >= col("aapv_4"))
            )
            .fill_null(False)
            .alias("buy_con"),
            (
                (col("close_1") <= col("aapv_1"))
                & (col("close_2") <= col("aapv_2"))
                & (col("close_3") <= col("aapv_3"))
                & (col("close_4") <= col("aapv_4"))
            )
            .fill_null(False)
            .alias("sell_con"),
        )
        .with_cols(
            (
                col("buy_con")
                & col("up_con")
                & (col("close_1") > col("open_1"))
                & (col("close_2") > col("open_2"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                col("sell_con")
                & col("up_con")
                & (col("close_1") < col("open_1"))
                & (col("close_2") < col("open_2"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "t14",
        "按 T14 AAPV 源码实现：成交量趋势过滤 + AAPV 连续4根条件触发开仓，反向条件触发反手。",
    )

def build_t15_expr() -> Expr:
    rlth = pms("t15_rlth", 2, 120, 2).limit(20)
    rpnt = pms("t15_rpnt", 0.1, 50.0, 0.1).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("high").max().rolling(rlth).alias("mhighest_raw"),
            col("low").min().rolling(rlth).alias("mlowest_raw"),
        )
        .with_cols(
            col('mhighest_raw', 'mlowest_raw').shift(1).expanding().alias('mhighest', 'mlowest'),
        )
        .with_cols(
            col((col("mhighest") + col("mlowest")) == 0.0, col.null, (col("mhighest") - col("mlowest"))
                / ((col("mhighest") + col("mlowest")) * 0.5)
                * 100.0).if_else()
            .alias("nechane")
        )
        .with_cols(
            (
                (col("nechane") <= rpnt)
                & (col("high") >= col("mhighest"))
            )
            .fill_null(False)
            .alias("open_long_sig_raw"),
            (
                (col("nechane") <= rpnt)
                & (col("low") <= col("mlowest"))
            )
            .fill_null(False)
            .alias("open_short_sig_raw"),
        )
        .select(
            col("open_long_sig_raw").alias("open_long_sig"),
            (col("open_short_sig_raw") & (~col("open_long_sig_raw"))).alias("open_short_sig"),
        )
        .with_cols(
            col("open_short_sig", "open_long_sig").alias("exit_long_sig", "exit_short_sig"),
        )
        .select(
            "open_long_sig",
            "open_short_sig",
            "exit_long_sig",
            "exit_short_sig",
        )
    )
    return _with_meta(
        e,
        "t15",
        "按 T15 期货源码实现：用 rolling+shift 的状态化 Nechane 指标过滤后触发前高/前低突破开仓，反向信号作为平仓信号。",
    )

def build_t16_expr() -> Expr:
    n1 = pms("t16_n1", 1, 20, 2).limit(20)
    n2 = pms("t16_n2", 1, 40, 2).limit(20)
    count = pms("t16_count", 5, 300, 30).limit(20)
    e = col("high", "low", "close").stra.t16_state_counter(n1, n2, count).expanding()
    return _with_meta(
        e,
        "t16",
        "按 T16 源码实现：基于前 N2 区间突破计数（g1/g2）与周期重置的状态机开仓，反向信号反手。",
    )

def build_t17_expr() -> Expr:
    length = pms("t17_length", 5, 240, 5).limit(20)
    j = pms("t17_j", 1.0, 500.0, 1.0).limit(20)
    # 源文件：T17_LPTT捐赠版/TradeStation/期货/T17期货代码.txt
    # LinearReg(close,length)->mta; LinearReg(mta*50,length)->mtb; mt=mta*mtb*50
    # 开多: marketposition<>1 and mt>J and mta>0
    # 开空: marketposition<>-1 and mt>J and mta<0
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("idx"),
        )
        .with_cols(
            col("close", "idx")
            .stock
            .ols(None, False, "beta")
            .rolling(length)
            .alias("mta"),
        )
        .with_cols(
            (col("mta") * 50.0).alias("mta50"),
        )
        .with_cols(
            col("mta50", "idx")
            .stock
            .ols(None, False, "beta")
            .rolling(length)
            .alias("mtb"),
        )
        .with_cols(
            (col("mta") * col("mtb") * 50.0).alias("mt"),
        )
        .with_cols(
            ((col("mt") > j) & (col("mta") > 0.0))
            .fill_null(False)
            .alias("open_long_raw"),
            ((col("mt") > j) & (col("mta") < 0.0))
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "t17",
        "按 T17 源策略实现：先对 close 做线性回归斜率 mta，再对 mta*50 做线性回归斜率 mtb，构造 mt=mta*mtb*50；当 mt>J 且 mta 同向时开仓，反向开仓同时平仓。",
    )

def build_t18_expr() -> Expr:
    n1 = pms("t18_n1", 5, 200, 5).limit(20)
    n2 = pms("t18_n2", 2, 120, 2).limit(20)
    aa = pms("t18_aa", 1.0, 80.0, 1.0).limit(20)
    k = pms("t18_k", 0.2, 6.0, 0.2).limit(20)
    af_step = pms("t18_af_step", 0.001, 0.2, 0.001).limit(20)
    af_limit = pms("t18_af_limit", 0.01, 1.0, 0.01).limit(20)
    length = pms("t18_length", 5, 200, 5).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").std().rolling(n1).alias("std_v"),
            col("close").mean().rolling(length).alias("midline"),
        )
        .with_cols(
            (k * col("std_v") + col("midline")).alias("upband"),
            (col("midline") - k * col("std_v")).alias("lowband"),
            col("std_v").min().rolling(n2).alias("lowest_std"),
        )
        .with_cols(
            col("upband").max().rolling(n1).alias("max_var"),
            col("lowband").min().rolling(n1).alias("min_var"),
        )
        .with_cols(
            col(col("lowest_std") < aa, col("close") > col("max_var"))
            .all(axis=1)
            .fill_null(False)
            .alias("open_long_raw"),
            col(col("lowest_std") < aa, col("close") < col("min_var"))
            .all(axis=1)
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            col("open_long_sig", "high", "low")
            .stra
            .exit_by_psar_af(0.02, af_step, af_limit)
            .expanding()
            .alias("exit_long_psar"),
            col("open_short_sig", "high", "low")
            .stra
            .exit_by_psar_af(0.02, af_step, af_limit, True)
            .expanding()
            .alias("exit_short_psar"),
            col(col("pos_pre") == 1.0, col("close") <= col("min_var"))
            .all(axis=1)
            .fill_null(False)
            .alias("exit_long_band"),
            col(col("pos_pre") == -1.0, col("close") >= col("max_var"))
            .all(axis=1)
            .fill_null(False)
            .alias("exit_short_band"),
        )
        .with_cols(
            col("exit_long_psar", "exit_long_band").any(axis=1).fill_null(False).alias("exit_long_raw"),
            col("exit_short_psar", "exit_short_band").any(axis=1).fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "t18",
        "按 T18 原策略实现：波动率收敛突变突破开仓（n1/n2/aa/k），并叠加源代码中的动态抛物线止盈/止损逻辑。",
    )

def build_t19_expr() -> Expr:
    length1 = pms("t19_length1", 2, 120, 8).limit(20)
    len2n = pms("t19_len2n", 1.0, 40.0, 15.0).limit(20)
    pnt = pms("t19_pnt", 0.01, 10.0, 0.46).limit(20)
    xx = pms("t19_xx", 1, 50, 5).limit(20)
    l2 = (length1 * len2n).ceil()
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col('close', 'high', 'low').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("close_1").mean().rolling(length1).alias("inner1"),
            col("close_1").mean().rolling(l2).alias("inner2"),
            col("high_1").max().rolling(length1).alias("hh"),
            col("low_1").min().rolling(length1).alias("ll"),
        )
        .with_cols(
            col("inner1").mean().rolling(xx).alias("ma1"),
            col("inner2").mean().rolling(xx).alias("ma2"),
        )
        .with_cols(
            col('ma1', 'ma2').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("ma1_1") <= col("ma2_1")) & (col("ma1") > col("ma2")))
            .fill_null(False)
            .alias("cross_up"),
            ((col("ma1_1") >= col("ma2_1")) & (col("ma1") < col("ma2")))
            .fill_null(False)
            .alias("cross_down"),
        )
        .with_cols(
            col("cross_up", col("hh") * (1.0 + pnt * 0.01), col.null).if_else()
            .alias("le_seed"),
            col("cross_down", col("ll") * (1.0 - pnt * 0.01), col.null).if_else()
            .alias("se_seed"),
            _pos_from_open_raw_expr("cross_up", "cross_down", "setup_side"),
        )
        .with_cols(
            col("le_seed").ffill().expanding().alias("le_price"),
            col("se_seed").ffill().expanding().alias("se_price"),
        )
        .with_cols(
            col('setup_side', 'le_price', 'se_price').shift(1).expanding().alias('setup_side_1', 'le_1', 'se_1'),
        )
        .with_cols(
            ((col("setup_side_1") == 1.0) & (col("high") >= col("le_1")))
            .fill_null(False)
            .alias("open_long_raw"),
            ((col("setup_side_1") == -1.0) & (col("low") <= col("se_1")))
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "t19",
        "按 T19 期货源码实现：双均线交叉产生 Buy/SellSetup，次 bar 触发 LEPrice/SEPrice 突破时开仓（含反手平仓）。",
    )

def build_t20_expr() -> Expr:
    length = pms("t20_length", 2, 120, 2).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").count().expanding().alias("idx"),
            col("close").shift(1).expanding().add_suffix("1"),
            col("close").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            col("close_1", "idx")
            .stock
            .ols(None, False, "beta")
            .rolling(length)
            .alias("mta"),
        )
        .with_cols(
            col("mta").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("mta_1") <= 0.0) & (col("mta") > 0.0))
            .fill_null(False)
            .alias("cross_up"),
            ((col("mta_1") >= 0.0) & (col("mta") < 0.0))
            .fill_null(False)
            .alias("cross_down"),
        )
        .with_cols(
            col("cross_up", col("close_1", "close_2").max(axis=1), col.null).if_else()
            .alias("hh_seed"),
            col("cross_down", col("close_1", "close_2").min(axis=1), col.null).if_else()
            .alias("ll_seed"),
        )
        .with_cols(
            col("hh_seed").ffill().expanding().alias("hh"),
            col("ll_seed").ffill().expanding().alias("ll"),
        )
        .with_cols(
            col('hh', 'll').shift(1).expanding().alias('hh_prev', 'll_prev'),
        )
        .with_cols(
            (
                (col("close_1") > col("hh_prev"))
                & (col("hh_prev") > col("ll_prev"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("close_1") < col("ll_prev"))
                & (col("hh_prev") > col("ll_prev"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "t20",
        "按 T20 期货源码实现：基于 LinearReg(close[1]) 斜率过零更新极值区间，随后以 close[1] 对 hh/ll 突破触发开仓（含反手平仓）。",
    )

def build_t21_expr() -> Expr:
    m = pms("t21_m", 2, 120, 2).limit(20)
    n = pms("t21_n", 2, 120, 2).limit(20)
    k = pms("t21_k", 0.2, 8.0, 0.2).limit(20)
    y = pms("t21_y", 1, 60, 1).limit(20)
    offset = pms("t21_offset", 0.0, 20.0, 0.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col.lit(k).alias("k"),
            col("close", "volume", "open", "high", "low").shift(1).expanding().add_suffix("1"),
            col("high", "low").shift(2).expanding().add_suffix("2"),
        )
        .with_cols(
            (col("close") - col("close_1")).alias("movmid"),
            (col("high") - col("low")).alias("hl"),
            (col("volume") - col("volume_1")).abs().sqrt().alias("vol_diff_root"),
            col("high", "low", "close").ta.trange().expanding().alias("tr"),
        )
        .with_cols(
            _if_chain_expr(
                [
                    (col(col("close_1").is_null(), col("volume_1").is_null()).any(axis=1), 0.0),
                    (col("hl") == 0.0, 0.0),
                    (
                        (col("vol_diff_root") / col("hl")) > 0.0,
                        col("movmid") / (col("vol_diff_root") / col("hl")) * 100.0,
                    ),
                ],
            
    default_value=0.0,
            
    alias="emv",
            ),
        )
        .with_cols(
            col("emv").mean().rolling(m).alias("avg"),
            col("tr").mean().rolling(n).alias("atr"),
            col("close").mean().rolling(n).alias("ma"),
        )
        .with_cols(
            (col("ma") + col("k") * col("atr")).alias("up"),
            (col("ma") - col("k") * col("atr")).alias("down"),
        )
        .with_cols(
            col('emv', 'avg', 'up', 'down').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (((col("open_1", "close_1").min(axis=1) + col("high_1")) * 0.5) >= col("up_1"))
                & (col("high_1") >= col("high_2"))
                & (col("emv_1") >= col("avg_1"))
                & (col("avg_1") >= 0.0)
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (((col("open_1", "close_1").max(axis=1) + col("low_1")) * 0.5) <= col("down_1"))
                & (col("low_1") <= col("low_2"))
                & (col("emv_1") <= col("avg_1"))
                & (col("avg_1") <= 0.0)
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "t21",
        "按 T21 期货源码实现 EMV+ATR 通道突破；其中源码 OpenInt 在当前常用 K 线输入缺失时用 volume 差分近似。",
    )

def build_t22_expr() -> Expr:
    length = pms("t22_length", 2, 60, 6).limit(20)
    stoploss_set = pms("t22_stoplossset", 0.1, 10.0, 2.0).limit(20)
    amplitude_set = pms("t22_amplitude_set", 1.0, 99.0, 80.0).limit(20)
    xz = pms("t22_xz", 1, 100, 16).limit(20)
    offset = pms("t22_offset", 0.0, 20.0, 0.0).limit(20)
    len3 = (length * 3).floor()
    len_half = (length / 2).floor()
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("datetime").dt.date().alias("day"),
            col("close").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("day").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("day") != col("day_1")).fill_null(True).alias("is_new_day"),
            col(
                col("high").max().alias("high_channel"),
                col("low").min().alias("low_channel"),
                col("open").first_value().alias("day_open"),
            ).expanding().over("day"),
        )
        .with_cols(
            col("high").max().rolling(len3).alias("hh_3len"),
            col("low").min().rolling(len3).alias("ll_3len"),
            col("close").mean().rolling(len_half).alias("fast_ma"),
            col("close").mean().rolling(length).alias("slow_ma"),
        )
        .with_cols(
            col((col("hh_3len") - col("ll_3len")).abs() <= 1e-12, col.null, (col("close") - col("ll_3len")).abs() / (col("hh_3len") - col("ll_3len")) * 100.0).if_else()
            .alias("rate_htl"),
        )
        .with_cols(
            col("rate_htl").mean().rolling(length).alias("avg_rate"),
            col('day_open', 'high_channel', 'low_channel', 'fast_ma', 'slow_ma').shift(1).expanding().alias('prev_day_open', 'high_channel_1', 'low_channel_1', 'fast_ma_1', 'slow_ma_1'),
        )
        .with_cols(
            col("avg_rate").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("low_channel_1") != 0.0)
                & ((col("high_channel_1") / col("low_channel_1")) < (1.0 + 0.001 * xz))
            )
            .fill_null(False)
            .alias("range_ok"),
            (col("open") + offset).alias("entry_long_now"),
            (col("open") - offset).alias("entry_short_now"),
        )
        .with_cols(
            (
                (col("avg_rate_1") > amplitude_set)
                & (col("fast_ma_1") > col("slow_ma_1"))
                & (col("close_1") > col("fast_ma_1"))
                & col("range_ok")
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("avg_rate_1") < (100.0 - amplitude_set))
                & (col("fast_ma_1") < col("slow_ma_1"))
                & (col("close_1") < col("fast_ma_1"))
                & col("range_ok")
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            _four_signals_from_pos_expr("pos_pre"),
        )
        .with_cols(
            col("open_long_sig", "open_short_sig").any(axis=1).fill_null(False).alias("open_any_pre"),
            _entry_seed_expr(),
        )
        .with_cols(
            col("entry_seed").ffill().expanding().alias("entry_price"),
            col("open_any_pre").count_last().expanding().alias("bars_since_entry"),
            (col("prev_day_open") * stoploss_set * 0.01).alias("stop_point"),
        )
        .with_cols(
            _bars_in_pos_expr("pos_pre", "bars_since_entry", "bars_in_pos"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("bars_in_pos") > 0)
                & (col("low") <= (col("entry_price") - col("stop_point")))
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("bars_in_pos") > 0)
                & (col("high") >= (col("entry_price") + col("stop_point")))
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "t22",
        "按 T22 期货源码实现：振幅占比与快慢均线条件开仓，叠加日内振幅压缩过滤，并按 OpenD(1)*StopLossSet 止损。",
    )

def build_t29_expr() -> Expr:
    lookback = pms("t29_lookback", 1, 20, 2).limit(20)
    k1 = pms("t29_k1", 0.1, 4.0, 0.7).limit(20)
    k2 = pms("t29_k2", 0.1, 4.0, 0.7).limit(20)
    start_hhmm = pms("t29_start_hhmm", 0, 2359, 900).limit(20)
    end_hhmm = pms("t29_end_hhmm", 0, 2359, 2330).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("datetime").dt.date().alias("day"),
            (col("datetime").dt.hour() * 100 + col("datetime").dt.minute()).alias("hhmm"),
            col(
                col("datetime").count().alias("bars_in_day"),
                col("open").first_value().alias("day_open"),
                col("high").max().alias("day_high_cum"),
                col("low").min().alias("day_low_cum"),
                col("close").last_value().alias("day_close_last"),
            ).expanding().over(col("datetime").dt.date()),
        )
        .with_cols(
            col('day', 'day_high_cum', 'day_low_cum', 'close').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("day") != col("day_1")).fill_null(True).alias("is_new_day"),
        )
        .with_cols(
            col("is_new_day", "day_high_cum_1", col.null).if_else().alias("prev_high_seed"),
            col("is_new_day", "day_low_cum_1", col.null).if_else().alias("prev_low_seed"),
            col("is_new_day", "close_1", col.null).if_else().alias("prev_close_seed"),
        )
        .with_cols(
            col("prev_high_seed", "prev_low_seed", "prev_close_seed")
            .ffill()
            .alias("prev_high", "prev_low", "prev_close")
            .expanding()
            .over(col("day"))
        )
        .with_cols(
            col(
                col("prev_high") - col("prev_close"),
                col("prev_close") - col("prev_low"),
            ).max(axis=1).alias("range_1d"),
        )
        .with_cols(
            col("range_1d")
            .max()
            .rolling(lookback)
            .over("bars_in_day")
            .alias("range_n"),
        )
        .with_cols(
            (col("day_open") + k1 * col("range_n")).alias("buy_line"),
            (col("day_open") - k2 * col("range_n")).alias("sell_line"),
            (
                (col("hhmm") >= start_hhmm)
                & (col("hhmm") <= end_hhmm)
            )
            .fill_null(False)
            .alias("time_ok"),
        )
        .with_cols(
            (col("time_ok") & (col("high") >= col("buy_line"))).fill_null(False).alias("open_long_raw"),
            (col("time_ok") & (col("low") <= col("sell_line"))).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            (
                (col("pos_pre") == 1.0)
                & (col("hhmm") >= end_hhmm)
            )
            .fill_null(False)
            .alias("exit_long_raw"),
            (
                (col("pos_pre") == -1.0)
                & (col("hhmm") >= end_hhmm)
            )
            .fill_null(False)
            .alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "t29",
        "按 T29（Dual Thrust）源码思想实现：基于前日波动区间构建日内突破轨，交易时段内突破开仓，收盘时段统一平仓。",
    )

def build_fshgmz1_expr() -> Expr:
    k1 = pms("fshgmz1_k1", 2, 120, 28).limit(20)
    length = pms("fshgmz1_length", 5, 240, 75).limit(20)
    fast_length = pms("fshgmz1_fast_length", 2, 60, 4).limit(20)
    slow_length = pms("fshgmz1_slow_length", 2, 80, 8).limit(20)
    macd_length = pms("fshgmz1_macd_length", 1, 30, 3).limit(20)
    trailing_start = pms("fshgmz1_trailing_start", 1.0, 800.0, 200.0).limit(20)
    trailing_stop = pms("fshgmz1_trailing_stop", 1.0, 120.0, 15.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col('high', 'low', 'close').shift(1).expanding().add_suffix("1"),
            (((col("high") + col("low") + col("close")) / 3.0)).alias("hlc3"),
        )
        .with_cols(
            col("hlc3").mean().rolling(macd_length).alias("ma1"),
            col("hlc3").mean().rolling(fast_length).alias("ma2"),
            col("hlc3").mean().rolling(slow_length).alias("ma3"),
            col("hlc3").mean().rolling(length).alias("midline"),
            col("hlc3").std().rolling(length).alias("band"),
            col("high_1").max().rolling(k1).alias("a1"),
            col("low_1").min().rolling(k1).alias("a2"),
        )
        .with_cols(
            col('ma1', 'ma2', 'ma3', 'midline', 'a1', 'a2').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("open") > col("midline")).fill_null(False).alias("condition13"),
            (col("open") < col("midline")).fill_null(False).alias("condition14"),
            (col("high_1") > col("ma1_1")).fill_null(False).alias("dt1"),
            (col("ma1_1") > col("ma2_1")).fill_null(False).alias("dt2"),
            (col("ma2_1") > col("ma3_1")).fill_null(False).alias("dt3"),
            (col("low_1") < col("ma1_1")).fill_null(False).alias("kt1"),
            (col("ma1_1") < col("ma2_1")).fill_null(False).alias("kt2"),
            (col("ma2_1") < col("ma3_1")).fill_null(False).alias("kt3"),
        )
        .with_cols(
            (
                (col("high") > col("a1"))
                & (col("high_1") <= col("a1_1"))
                & col("condition13")
                & col("dt1")
                & col("dt2")
                & col("dt3")
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("low") < col("a2"))
                & (col("low_1") >= col("a2_1"))
                & col("condition14")
                & col("kt1")
                & col("kt2")
                & col("kt3")
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            col("pos_pre").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("pos_pre_1") != 1.0)).fill_null(False).alias("open_long_pre"),
            ((col("pos_pre") == -1.0) & (col("pos_pre_1") != -1.0)).fill_null(False).alias("open_short_pre"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "open_long_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
            _bars_pos_from_open_sig_expr("open_long_pre", "open_short_pre", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "high", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("highest_after_entry"),
            col("open_short_pre", "open_long_pre", "low", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("lowest_after_entry"),
        )
        .with_cols(
            col('highest_after_entry', 'lowest_after_entry').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            _exit_trailing_long_expr(
                start_expr=trailing_start,
                stop_expr=trailing_stop,
            ),
            _exit_trailing_short_expr(
                start_expr=trailing_start,
                stop_expr=trailing_stop,
            ),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "fshgmz1",
        "按 fshgmz1 源码实现：区间突破+均线结构过滤开仓，持仓后固定止损与分段跟踪止盈并行离场。",
    )

def build_fshgmz2_expr() -> Expr:
    k1 = pms("fshgmz2_k1", 2, 120, 27).limit(20)
    length = pms("fshgmz2_length", 5, 240, 75).limit(20)
    fast_length = pms("fshgmz2_fast_length", 2, 60, 3).limit(20)
    slow_length = pms("fshgmz2_slow_length", 2, 80, 4).limit(20)
    macd_length = pms("fshgmz2_macd_length", 1, 30, 2).limit(20)
    trailing_start = pms("fshgmz2_trailing_start", 1.0, 800.0, 200.0).limit(20)
    trailing_stop = pms("fshgmz2_trailing_stop", 1.0, 120.0, 15.0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col('high', 'low', 'close').shift(1).expanding().add_suffix("1"),
            (((col("high") + col("low") + col("close")) / 3.0)).alias("hlc3"),
        )
        .with_cols(
            col("hlc3").mean().rolling(length).alias("midline"),
            col("hlc3").std().rolling(length).alias("band"),
            col("high_1").max().rolling(k1).alias("a1"),
            col("low_1").min().rolling(k1).alias("a2"),
        )
        .with_cols(
            col('midline', 'a1', 'a2').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("open") > col("midline")).fill_null(False).alias("condition13"),
            (col("open") < col("midline")).fill_null(False).alias("condition14"),
            col("hlc3").ta.ema(fast_length).expanding().alias("ema_fast_1"),
            col("hlc3").ta.ema(slow_length).expanding().alias("ema_slow_1"),
            col("hlc3").ta.ema(fast_length * 3).expanding().alias("ema_fast_3"),
            col("hlc3").ta.ema(slow_length * 3).expanding().alias("ema_slow_3"),
            col("hlc3").ta.ema(fast_length * 5).expanding().alias("ema_fast_5"),
            col("hlc3").ta.ema(slow_length * 5).expanding().alias("ema_slow_5"),
            col("hlc3").ta.ema(fast_length * 8).expanding().alias("ema_fast_15"),
            col("hlc3").ta.ema(slow_length * 8).expanding().alias("ema_slow_15"),
            col("hlc3").ta.ema(fast_length * 15).expanding().alias("ema_fast_30"),
            col("hlc3").ta.ema(slow_length * 15).expanding().alias("ema_slow_30"),
        )
        .with_cols(
            (col("ema_fast_1") - col("ema_slow_1")).alias("macd_1"),
            (col("ema_fast_3") - col("ema_slow_3")).alias("macd_3"),
            (col("ema_fast_5") - col("ema_slow_5")).alias("macd_5"),
            (col("ema_fast_15") - col("ema_slow_15")).alias("macd_15"),
            (col("ema_fast_30") - col("ema_slow_30")).alias("macd_30"),
        )
        .with_cols(
            col("macd_1").ta.ema(macd_length).expanding().alias("macd_avg_1"),
            col("macd_3").ta.ema(macd_length * 3).expanding().alias("macd_avg_3"),
            col("macd_5").ta.ema(macd_length * 5).expanding().alias("macd_avg_5"),
            col("macd_15").ta.ema(macd_length * 8).expanding().alias("macd_avg_15"),
            col("macd_30").ta.ema(macd_length * 15).expanding().alias("macd_avg_30"),
        )
        .with_cols(
            (col("macd_1") - col("macd_avg_1")).alias("macd_diff_1"),
            (col("macd_3") - col("macd_avg_3")).alias("macd_diff_3"),
            (col("macd_5") - col("macd_avg_5")).alias("macd_diff_5"),
            (col("macd_15") - col("macd_avg_15")).alias("macd_diff_15"),
            (col("macd_30") - col("macd_avg_30")).alias("macd_diff_30"),
        )
        .with_cols(
            col(
                col("macd_diff_1") > 0.0,
                col("macd_diff_3") > 0.0,
                col("macd_diff_5") > 0.0,
                col("macd_diff_15") > 0.0,
                col("macd_diff_30") > 0.0,
            ).all(axis=1).fill_null(False).alias("long_entry_con"),
            col(
                col("macd_diff_1") < 0.0,
                col("macd_diff_3") < 0.0,
                col("macd_diff_5") < 0.0,
                col("macd_diff_15") < 0.0,
                col("macd_diff_30") < 0.0,
            ).all(axis=1).fill_null(False).alias("short_entry_con"),
        )
        .with_cols(
            col(
                col("high") > col("a1"),
                col("high_1") <= col("a1_1"),
                "condition13",
                "long_entry_con",
            ).all(axis=1).fill_null(False).alias("open_long_raw"),
            col(
                col("low") < col("a2"),
                col("low_1") >= col("a2_1"),
                "condition14",
                "short_entry_con",
            ).all(axis=1).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            col("pos_pre").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("pos_pre_1") != 1.0)).fill_null(False).alias("open_long_pre"),
            ((col("pos_pre") == -1.0) & (col("pos_pre_1") != -1.0)).fill_null(False).alias("open_short_pre"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "open_long_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
            _bars_pos_from_open_sig_expr("open_long_pre", "open_short_pre", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "high", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("highest_after_entry"),
            col("open_short_pre", "open_long_pre", "low", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("lowest_after_entry"),
        )
        .with_cols(
            col('highest_after_entry', 'lowest_after_entry').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            _exit_trailing_long_expr(
                start_expr=trailing_start,
                stop_expr=trailing_stop,
            ),
            _exit_trailing_short_expr(
                start_expr=trailing_start,
                stop_expr=trailing_stop,
            ),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "fshgmz2",
        "按 fshgmz2 源码实现：区间突破叠加多周期 MACD 一致性开仓，配合固定止损和分段跟踪止盈离场。",
    )

def build_fshgmz3_expr() -> Expr:
    p_param1 = pms("fshgmz3_p_param1", 0.1, 5.0, 0.1).limit(20)
    p_param2 = pms("fshgmz3_p_param2", 0.1, 5.0, 0.1).limit(20)
    p_dparam1 = pms("fshgmz3_p_dparam1", 1, 10, 1).limit(20)
    p_dparam2 = pms("fshgmz3_p_dparam2", 1, 10, 1).limit(20)
    length = pms("fshgmz3_length", 10, 300, 10).limit(20)
    trailing_start = pms("fshgmz3_trailing_start", 1.0, 800.0, 1.0).limit(20)
    trailing_stop = pms("fshgmz3_trailing_stop", 1.0, 120.0, 1.0).limit(20)
    trad_begin = pms("fshgmz3_trad_begin", 0, 2359, 0).limit(20)
    trad_end = pms("fshgmz3_trad_end", 0, 2400, 0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("datetime").dt.date().alias("day"),
            (col("datetime").dt.hour() * 100 + col("datetime").dt.minute()).alias("hhmm"),
            col(
                col("datetime").count().alias("bars_in_day"),
                col("open").first_value().alias("open_d0"),
            ).expanding().over(col("datetime").dt.date()),
            col('high', 'low', 'close').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("close").mean().rolling(length).alias("midline"),
            col(
                col("high").max().alias("day_high_cum"),
                col("low").min().alias("day_low_cum"),
            ).expanding().over(col("day")),
        )
        .with_cols(
            (col("open") > col("midline")).fill_null(False).alias("condition13"),
            (col("open") < col("midline")).fill_null(False).alias("condition14"),
            col("day_high_cum", "day_low_cum", "close")
            .shift(1)
            .expanding()
            .alias("day_high_peer_1", "day_low_peer_1", "close_peer_1")
            .over("bars_in_day"),
        )
        .with_cols(
            col(
                col("day_high_peer_1").max().alias("highest_1"),
                col("day_low_peer_1").min().alias("lowest_1"),
                col("close_peer_1").max().alias("close_high_1"),
                col("close_peer_1").min().alias("close_low_1"),
            ).rolling(p_dparam1),
            col(
                col("day_high_peer_1").max().alias("highest_2"),
                col("day_low_peer_1").min().alias("lowest_2"),
                col("close_peer_1").max().alias("close_high_2"),
                col("close_peer_1").min().alias("close_low_2"),
            ).rolling(p_dparam2),
        )
        .with_cols(
            col("highest_1", "lowest_1", "close_high_1", "close_low_1", "highest_2", "lowest_2", "close_high_2", "close_low_2")
            .over("bars_in_day"),
        )
        .with_cols(
            col(
                col("highest_1") - col("close_low_1"),
                col("close_high_1") - col("lowest_1"),
            ).max(axis=1).alias("buy_range"),
            col(
                col("highest_2") - col("close_low_2"),
                col("close_high_2") - col("lowest_2"),
            ).max(axis=1).alias("sell_range"),
        )
        .with_cols(
            (col("open_d0") + col("buy_range") * p_param1).alias("m_upper"),
            (col("open_d0") - col("sell_range") * p_param2).alias("m_lower"),
            col(
                col("hhmm") >= trad_begin,
                col("hhmm") <= trad_end,
            ).all(axis=1).fill_null(False).alias("time_ok"),
            col('condition13', 'condition14').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col(
                "time_ok",
                "condition13_1",
                col("high") >= col("m_upper"),
            ).all(axis=1).fill_null(False).alias("open_long_raw"),
            col(
                "time_ok",
                "condition14_1",
                col("low") <= col("m_lower"),
            ).all(axis=1).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            col("pos_pre").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("pos_pre_1") != 1.0)).fill_null(False).alias("open_long_pre"),
            ((col("pos_pre") == -1.0) & (col("pos_pre_1") != -1.0)).fill_null(False).alias("open_short_pre"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "open_long_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
            _bars_pos_from_open_sig_expr("open_long_pre", "open_short_pre", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "high", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("highest_after_entry"),
            col("open_short_pre", "open_long_pre", "low", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("lowest_after_entry"),
        )
        .with_cols(
            col('highest_after_entry', 'lowest_after_entry').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            _exit_trailing_long_expr(
                start_expr=trailing_start,
                stop_expr=trailing_stop,
            ),
            _exit_trailing_short_expr(
                start_expr=trailing_start,
                stop_expr=trailing_stop,
            ),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "fshgmz3",
        "按 fshgmz3 源码实现：日内 Dual-Thrust 区间突破（含开盘中线过滤）开仓，持仓后固定止损与跟踪止盈离场。",
    )

def build_fshgmz4_expr() -> Expr:
    p_param1 = pms("fshgmz4_p_param1", 0.1, 5.0, 0.1).limit(20)
    p_param2 = pms("fshgmz4_p_param2", 0.1, 5.0, 0.1).limit(20)
    p_dparam1 = pms("fshgmz4_p_dparam1", 1, 10, 1).limit(20)
    p_dparam2 = pms("fshgmz4_p_dparam2", 1, 10, 1).limit(20)
    length = pms("fshgmz4_length", 20, 400, 20).limit(20)
    length1 = pms("fshgmz4_length1", 1, 60, 1).limit(20)
    fast_length = pms("fshgmz4_fast_length", 2, 60, 2).limit(20)
    slow_length = pms("fshgmz4_slow_length", 2, 80, 2).limit(20)
    macd_length = pms("fshgmz4_macd_length", 1, 30, 1).limit(20)
    trailing_start = pms("fshgmz4_trailing_start", 1.0, 800.0, 1.0).limit(20)
    trailing_stop = pms("fshgmz4_trailing_stop", 1.0, 120.0, 1.0).limit(20)
    trad_begin = pms("fshgmz4_trad_begin", 0, 2359, 0).limit(20)
    trad_end = pms("fshgmz4_trad_end", 0, 2359, 0).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("datetime").dt.date().alias("day"),
            (col("datetime").dt.hour() * 100 + col("datetime").dt.minute()).alias("hhmm"),
            col(
                col("datetime").count().alias("bars_in_day"),
                col("open").first_value().alias("open_d0"),
            ).expanding().over(col("datetime").dt.date()),
            col('high', 'low', 'close').shift(1).expanding().add_suffix("1"),
            (((col("high") + col("low") + col("close")) / 3.0)).alias("hlc3"),
        )
        .with_cols(
            col("close").mean().rolling(length).alias("midline"),
            col("close").std().rolling(length).alias("band"),
            col("close").mean().rolling(length - length1).alias("midline1"),
            col("close").std().rolling(length - length1).alias("band1"),
            col("hlc3").mean().rolling(macd_length).alias("ma1"),
            col("hlc3").mean().rolling(fast_length).alias("ma2"),
            col("hlc3").mean().rolling(slow_length).alias("ma3"),
            col(
                col("high").max().alias("day_high_cum"),
                col("low").min().alias("day_low_cum"),
            ).expanding().over(col("day")),
        )
        .with_cols(
            (col("midline") + col("band") * 2.0).alias("upline"),
            (col("midline") - col("band") * 2.0).alias("downline"),
            (col("midline1") + col("band1") * 2.0).alias("upline1"),
            (col("midline1") - col("band1") * 2.0).alias("downline1"),
            col("day_high_cum", "day_low_cum", "close")
            .shift(1)
            .expanding()
            .alias("day_high_peer_1", "day_low_peer_1", "close_peer_1")
            .over("bars_in_day"),
        )
        .with_cols(
            col('upline', 'downline', 'upline1', 'downline1', 'ma1', 'ma2', 'ma3').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("upline1_1") < col("upline_1")).fill_null(False).alias("condition1"),
            (col("downline1_1") > col("downline_1")).fill_null(False).alias("condition2"),
            (col("upline1_1") > col("upline_1")).fill_null(False).alias("condition3"),
            (col("downline1_1") < col("downline_1")).fill_null(False).alias("condition4"),
            (col("high_1") > col("ma1_1")).fill_null(False).alias("dt1"),
            (col("ma1_1") > col("ma2_1")).fill_null(False).alias("dt2"),
            (col("ma2_1") > col("ma3_1")).fill_null(False).alias("dt3"),
            (col("low_1") < col("ma1_1")).fill_null(False).alias("kt1"),
            (col("ma1_1") < col("ma2_1")).fill_null(False).alias("kt2"),
            (col("ma2_1") < col("ma3_1")).fill_null(False).alias("kt3"),
        )
        .with_cols(
            col("hlc3").ta.ema(fast_length).expanding().alias("ema_fast_1"),
            col("hlc3").ta.ema(slow_length).expanding().alias("ema_slow_1"),
            col("hlc3").ta.ema(fast_length * 3).expanding().alias("ema_fast_3"),
            col("hlc3").ta.ema(slow_length * 3).expanding().alias("ema_slow_3"),
            col("hlc3").ta.ema(fast_length * 5).expanding().alias("ema_fast_5"),
            col("hlc3").ta.ema(slow_length * 5).expanding().alias("ema_slow_5"),
            col("hlc3").ta.ema(fast_length * 8).expanding().alias("ema_fast_15"),
            col("hlc3").ta.ema(slow_length * 8).expanding().alias("ema_slow_15"),
            col("hlc3").ta.ema(fast_length * 35).expanding().alias("ema_fast_30"),
            col("hlc3").ta.ema(slow_length * 35).expanding().alias("ema_slow_30"),
        )
        .with_cols(
            (col("ema_fast_1") - col("ema_slow_1")).alias("macd_1"),
            (col("ema_fast_3") - col("ema_slow_3")).alias("macd_3"),
            (col("ema_fast_5") - col("ema_slow_5")).alias("macd_5"),
            (col("ema_fast_15") - col("ema_slow_15")).alias("macd_15"),
            (col("ema_fast_30") - col("ema_slow_30")).alias("macd_30"),
        )
        .with_cols(
            col("macd_1").ta.ema(macd_length).expanding().alias("macd_avg_1"),
            col("macd_3").ta.ema(macd_length * 3).expanding().alias("macd_avg_3"),
            col("macd_5").ta.ema(macd_length * 5).expanding().alias("macd_avg_5"),
            col("macd_15").ta.ema(macd_length * 8).expanding().alias("macd_avg_15"),
            col("macd_30").ta.ema(macd_length * 35).expanding().alias("macd_avg_30"),
        )
        .with_cols(
            (col("macd_1") - col("macd_avg_1")).alias("macd_diff_1"),
            (col("macd_3") - col("macd_avg_3")).alias("macd_diff_3"),
            (col("macd_5") - col("macd_avg_5")).alias("macd_diff_5"),
            (col("macd_15") - col("macd_avg_15")).alias("macd_diff_15"),
            (col("macd_30") - col("macd_avg_30")).alias("macd_diff_30"),
        )
        .with_cols(
            (
                (col("macd_diff_1") > 0.0)
                & (col("macd_diff_3") > 0.0)
                & (col("macd_diff_5") > 0.0)
                & (col("macd_diff_15") > 0.0)
                & (col("macd_diff_30") > 0.0)
            )
            .fill_null(False)
            .alias("long_entry_con"),
            (
                (col("macd_diff_1") < 0.0)
                & (col("macd_diff_3") < 0.0)
                & (col("macd_diff_5") < 0.0)
                & (col("macd_diff_15") < 0.0)
                & (col("macd_diff_30") < 0.0)
            )
            .fill_null(False)
            .alias("short_entry_con"),
        )
        .with_cols(
            col(
                col("day_high_peer_1").max().alias("highest_1"),
                col("day_low_peer_1").min().alias("lowest_1"),
                col("close_peer_1").max().alias("close_high_1"),
                col("close_peer_1").min().alias("close_low_1"),
            ).rolling(p_dparam1),
            col(
                col("day_high_peer_1").max().alias("highest_2"),
                col("day_low_peer_1").min().alias("lowest_2"),
                col("close_peer_1").max().alias("close_high_2"),
                col("close_peer_1").min().alias("close_low_2"),
            ).rolling(p_dparam2),
        )
        .with_cols(
            col("highest_1", "lowest_1", "close_high_1", "close_low_1", "highest_2", "lowest_2", "close_high_2", "close_low_2")
            .over("bars_in_day"),
        )
        .with_cols(
            col(
                col("highest_1") - col("close_low_1"),
                col("close_high_1") - col("lowest_1"),
            )
            .max(axis=1)
            .alias("buy_range"),
            col(
                col("highest_2") - col("close_low_2"),
                col("close_high_2") - col("lowest_2"),
            )
            .max(axis=1)
            .alias("sell_range"),
        )
        .with_cols(
            (col("open_d0") + col("buy_range") * p_param1).alias("m_upper"),
            (col("open_d0") - col("sell_range") * p_param2).alias("m_lower"),
            col(
                col("hhmm") >= trad_begin,
                col("hhmm") <= trad_end,
            )
            .all(axis=1)
            .fill_null(False)
            .alias("time_ok"),
            col("condition1", "condition2", "condition3", "condition4", "long_entry_con", "short_entry_con")
            .shift(1)
            .expanding()
            .add_suffix("1"),
            col("dt1", "dt2", "dt3", "kt1", "kt2", "kt3")
            .shift(1)
            .expanding()
            .add_suffix("1"),
        )
        .with_cols(
            col(
                "time_ok",
                col(
                    col(
                        "condition2_1",
                        col("high") >= col("m_upper"),
                        "long_entry_con_1",
                        "dt1_1",
                        "dt2_1",
                        "dt3_1",
                    ).all(axis=1),
                    col("condition3_1", col("high") >= col("m_upper")).all(axis=1),
                ).any(axis=1),
            ).all(axis=1).fill_null(False).alias("open_long_raw"),
            col(
                "time_ok",
                col(
                    col(
                        "condition1_1",
                        col("low") <= col("m_lower"),
                        "short_entry_con_1",
                        "kt1_1",
                        "kt2_1",
                        "kt3_1",
                    ).all(axis=1),
                    col("condition4_1", col("low") <= col("m_lower")).all(axis=1),
                ).any(axis=1),
            ).all(axis=1).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos_pre"),
        )
        .with_cols(
            col("pos_pre").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            ((col("pos_pre") == 1.0) & (col("pos_pre_1") != 1.0)).fill_null(False).alias("open_long_pre"),
            ((col("pos_pre") == -1.0) & (col("pos_pre_1") != -1.0)).fill_null(False).alias("open_short_pre"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_long"),
            col("open_short_pre", "open_long_pre", "open")
            .stra
            .t02_track_entry_price()
            .expanding()
            .alias("entry_short"),
            _bars_pos_from_open_sig_expr("open_long_pre", "open_short_pre", "bars_pos"),
        )
        .with_cols(
            col("open_long_pre", "open_short_pre", "high", "entry_long")
            .stra
            .t02_track_best_since_entry(False)
            .expanding()
            .alias("highest_after_entry"),
            col("open_short_pre", "open_long_pre", "low", "entry_short")
            .stra
            .t02_track_best_since_entry(True)
            .expanding()
            .alias("lowest_after_entry"),
        )
        .with_cols(
            col('highest_after_entry', 'lowest_after_entry').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            _exit_trailing_long_expr(
                start_expr=trailing_start,
                stop_expr=trailing_stop,
            ),
            _exit_trailing_short_expr(
                start_expr=trailing_start,
                stop_expr=trailing_stop,
            ),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "fshgmz4",
        "按 fshgmz4 源码实现：双布林结构条件+多周期 MACD 一致性过滤下的日内区间突破开仓，并行固定止损与跟踪止盈离场。",
    )

def build_fshgmz5_expr() -> Expr:
    length = pms("fshgmz5_length", 40, 300, 40).limit(20)
    length_short = pms("fshgmz5_length_short", 10, 240, 10).limit(20)
    offset = pms("fshgmz5_offset", 0.5, 6.0, 0.5).limit(20)
    offset1 = pms("fshgmz5_offset1", 0.5, 6.0, 0.5).limit(20)
    fast_n = pms("fshgmz5_fast_n", 2, 20, 2).limit(20)
    slow_n = pms("fshgmz5_slow_n", 3, 40, 3).limit(20)
    macd_n = pms("fshgmz5_macd_n", 2, 20, 2).limit(20)

    e = (
        _full_kline_expr()
        .with_cols(
            ((col("high") + col("low") + col("close")) / 3.0).alias("hlc3"),
            col.lit(offset).alias("offset"),
            col.lit(offset1).alias("offset1"),
        )
        .with_cols(
            col("hlc3").mean().rolling(macd_n).alias("ma1"),
            col("hlc3").mean().rolling(fast_n).alias("ma2"),
            col("hlc3").mean().rolling(slow_n).alias("ma3"),
            col("close").mean().rolling(length).alias("mid"),
            col("close").std(2).rolling(length).alias("band"),
            col("close").mean().rolling(length_short).alias("mid1"),
            col("close").std(2).rolling(length_short).alias("band1"),
        )
        .with_cols(
            (col("mid") + col("offset") * col("band")).alias("up"),
            (col("mid") - col("offset") * col("band")).alias("down"),
            (col("mid1") + col("offset1") * col("band1")).alias("up1"),
            (col("mid1") - col("offset1") * col("band1")).alias("down1"),
            (col("ma2") - col("ma3")).alias("macd_v"),
            (col("high") > col("ma1")).alias("dt1"),
            (col("ma1") > col("ma2")).alias("dt2"),
            (col("ma2") > col("ma3")).alias("dt3"),
            (col("low") < col("ma1")).alias("kt1"),
            (col("ma1") < col("ma2")).alias("kt2"),
            (col("ma2") < col("ma3")).alias("kt3"),
        )
        .with_cols(
            col("up", "down", "up1", "down1", "mid", "dt1", "dt2", "dt3", "kt1", "kt2", "kt3")
            .shift(1)
            .expanding()
            .add_suffix("1"),
        )
        .with_cols(
            col("macd_v").mean().rolling(macd_n).alias("macd_avg"),
        )
        .with_cols(
            (col("macd_v") - col("macd_avg")).alias("macd_diff"),
            (col("up1_1") < col("up_1")).alias("cond1"),
            (col("down1_1") > col("down_1")).alias("cond2"),
            (col("up1_1") > col("up_1")).alias("cond3"),
            (col("down1_1") < col("down_1")).alias("cond4"),
        )
        .select(
            col(
                col(
                    "cond2",
                    col("open") > col("mid_1"),
                    col("high") >= col("up_1"),
                    col("macd_diff") > 0,
                    "dt1_1",
                    "dt2_1",
                    "dt3_1",
                ).all(axis=1),
                col("cond3", col("open") > col("mid_1"), col("high") >= col("up_1")).all(axis=1),
            )
            .any(axis=1)
            .fill_null(False)
            .alias("open_long_sig"),
            col(
                col(
                    "cond1",
                    col("open") < col("mid_1"),
                    col("low") <= col("down_1"),
                    col("macd_diff") < 0,
                    "kt1_1",
                    "kt2_1",
                    "kt3_1",
                ).all(axis=1),
                col("cond4", col("open") < col("mid_1"), col("low") <= col("down_1")).all(axis=1),
            )
            .any(axis=1)
            .fill_null(False)
            .alias("open_short_sig"),
            (col("macd_diff") < 0).fill_null(False).alias("exit_long_sig"),
            (col("macd_diff") > 0).fill_null(False).alias("exit_short_sig"),
        )
    )
    return _with_meta(
        e,
        "fshgmz5",
        "按 fshgmz5 原策略的条件分支实现：Condition2/3 需同时满足 open>midline，Condition1/4 需同时满足 open<midline。",
    )

def build_fshgmz6_expr() -> Expr:
    length = pms("fshgmz6_length", 40, 300, 40).limit(20)
    length_short = pms("fshgmz6_length_short", 10, 240, 10).limit(20)
    offset = pms("fshgmz6_offset", 0.5, 6.0, 0.5).limit(20)
    offset1 = pms("fshgmz6_offset1", 0.5, 6.0, 0.5).limit(20)
    fast_n = pms("fshgmz6_fast_n", 2, 20, 2).limit(20)
    slow_n = pms("fshgmz6_slow_n", 3, 40, 3).limit(20)
    macd_n = pms("fshgmz6_macd_n", 2, 20, 2).limit(20)

    e = (
        _full_kline_expr()
        .with_cols(
            ((col("high") + col("low") + col("close")) / 3.0).alias("hlc3"),
            col.lit(offset).alias("offset"),
            col.lit(offset1).alias("offset1"),
        )
        .with_cols(
            col("hlc3").mean().rolling(macd_n).alias("ma1"),
            col("hlc3").mean().rolling(fast_n).alias("ma2"),
            col("hlc3").mean().rolling(slow_n).alias("ma3"),
            col("close").mean().rolling(length).alias("mid"),
            col("close").std(2).rolling(length).alias("band"),
            col("close").mean().rolling(length_short).alias("mid1"),
            col("close").std(2).rolling(length_short).alias("band1"),
        )
        .with_cols(
            (col("mid") + col("offset") * col("band")).alias("up"),
            (col("mid") - col("offset") * col("band")).alias("down"),
            (col("mid1") + col("offset1") * col("band1")).alias("up1"),
            (col("mid1") - col("offset1") * col("band1")).alias("down1"),
            (col("ma2") - col("ma3")).alias("macd_v"),
            (col("high") > col("ma1")).alias("dt1"),
            (col("ma1") > col("ma2")).alias("dt2"),
            (col("ma2") > col("ma3")).alias("dt3"),
            (col("low") < col("ma1")).alias("kt1"),
            (col("ma1") < col("ma2")).alias("kt2"),
            (col("ma2") < col("ma3")).alias("kt3"),
        )
        .with_cols(
            col("up", "down", "up1", "down1", "mid", "dt1", "dt2", "dt3", "kt1", "kt2", "kt3")
            .shift(1)
            .expanding()
            .add_suffix("1"),
        )
        .with_cols(
            col("macd_v").mean().rolling(macd_n).alias("macd_avg"),
        )
        .with_cols(
            (col("macd_v") - col("macd_avg")).alias("macd_diff"),
            (col("up1_1") < col("up_1")).alias("cond1"),
            (col("down1_1") > col("down_1")).alias("cond2"),
            (col("up1_1") > col("up_1")).alias("cond3"),
            (col("down1_1") < col("down_1")).alias("cond4"),
        )
        .select(
            col(
                col(
                    col("cond2", col("open") > col("mid_1")).any(axis=1),
                    col("high") >= col("up_1"),
                    col("macd_diff") > 0,
                    "dt1_1",
                    "dt2_1",
                    "dt3_1",
                ).all(axis=1),
                col("cond3", col("high") >= col("up_1")).all(axis=1),
            )
            .any(axis=1)
            .fill_null(False)
            .alias("open_long_sig"),
            col(
                col(
                    col("cond1", col("open") < col("mid_1")).any(axis=1),
                    col("low") <= col("down_1"),
                    col("macd_diff") < 0,
                    "kt1_1",
                    "kt2_1",
                    "kt3_1",
                ).all(axis=1),
                col("cond4", col("low") <= col("down_1")).all(axis=1),
            )
            .any(axis=1)
            .fill_null(False)
            .alias("open_short_sig"),
            (col("macd_diff") < 0).fill_null(False).alias("exit_long_sig"),
            (col("macd_diff") > 0).fill_null(False).alias("exit_short_sig"),
        )
    )
    return _with_meta(
        e,
        "fshgmz6",
        "按 fshgmz6 原策略的条件分支实现：Condition2/1 与 open-midline 使用 OR 关系，且 Condition3/4 分支不再强制 open-midline 同向过滤。",
    )

def build_t23_expr() -> Expr:
    length1 = pms("t23_length1", 1, 40, 1).limit(20)
    length2 = pms("t23_length2", 10, 300, 10).limit(20)
    n = pms("t23_n", 1, 80, 1).limit(20)
    stop_set = pms("t23_stop_set", 0.1, 10.0, 0.1).limit(20)
    profit = pms("t23_profit", 1.0, 60.0, 1.0).limit(20)
    trace_point = pms("t23_trace_point", 1.0, 80.0, 1.0).limit(20)
    nday3 = pms("t23_nday3", 1, 30, 1).limit(20)
    lots1 = pms("t23_lots1", 1, 20, 1).limit(20)
    lots2 = pms("t23_lots2", 1, 20, 1).limit(20)
    e = (
        _full_kline_expr()
        .select("open", "high", "low", "close")
        .with_cols(
            col("high", "low", "close").shift(1).expanding().add_suffix("1"),
            col("close").mean().rolling(500).alias("malong"),
            col("open", "high", "low", "close").stra.t23_entry_core(length1, length2, n, nday3).expanding(),
        )
        .with_cols(
            col("malong").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                col(
                    col("close_1") < col("malong_1"),
                    lots2,
                    lots1,
                )
                .if_else()
                == lots1
            )
            .fill_null(False)
            .alias("enable_long_once"),
            (
                col(
                    col("close_1") < col("malong_1"),
                    lots1,
                    lots2,
                )
                .if_else()
                == lots1
            )
            .fill_null(False)
            .alias("enable_short_once"),
        )
        .with_cols(
            col("open_long_raw", "open_short_raw", "open", "high_1", "low")
            .stra
            .exit_by_profit_drawdown_ref(profit, trace_point, False)
            .expanding()
            .alias("exit_long_draw"),
            col("open_short_raw", "open_long_raw", "open", "low_1", "high")
            .stra
            .exit_by_profit_drawdown_ref(profit, trace_point, True)
            .expanding()
            .alias("exit_short_draw"),
            col("open_long_raw", "open_short_raw", "open", "high", "enable_long_once")
            .stra
            .exit_by_entry_move_once(stop_set, False)
            .expanding()
            .alias("exit_long_once"),
            col("open_short_raw", "open_long_raw", "open", "low", "enable_short_once")
            .stra
            .exit_by_entry_move_once(stop_set, True)
            .expanding()
            .alias("exit_short_once"),
        )
        .with_cols(
            col("open_short_raw", "exit_long_draw", "exit_long_once").any(axis=1).fill_null(False).alias("exit_long_raw"),
            col("open_long_raw", "exit_short_draw", "exit_short_once").any(axis=1).fill_null(False).alias("exit_short_raw"),
        )
        .with_cols(
            _pos_from_four_raw_expr("open_long_raw", "exit_long_raw", "open_short_raw", "exit_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "t23",
        "按 T23 源码实现：均线区间状态机 + 进场后动态止盈止损 + 分批减仓逻辑。",
    )


def build_t24_expr() -> Expr:
    para1 = pms("t24_para1", 2, 120, 2).limit(20)
    para2 = pms("t24_para2", 2, 180, 2).limit(20)
    buy_ratio = pms("t24_buy_ratio", 1.0, 80.0, 1.0).limit(20)
    sell_ratio = pms("t24_sell_ratio", 1.0, 80.0, 1.0).limit(20)
    lots = pms("t24_lots", 1, 20, 1).limit(20)
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("datetime").dt.date().alias("day"),
            col("close").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("day").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (col("day") != col("day_1")).fill_null(True).alias("is_new_day"),
            col("close").ta.ema(para1).expanding().alias("ema_inner"),
            col("close_1").ta.ema(para2).expanding().alias("ema_prev_close"),
        )
        .with_cols(
            col("ema_inner").ta.ema(para1).expanding().alias("madiff"),
            col(
                col("open").first_value().alias("day_open"),
                col("close_1").first_value().alias("prev_day_close"),
            ).expanding().over("day"),
        )
        .with_cols(
            col('madiff', 'ema_prev_close').shift(1).expanding().alias('madiff_1', 'prev_ma'),
        )
        .with_cols(
            ((col("madiff") - col("madiff_1")) * 100.0).alias("diffsection"),
        )
        .with_cols(
            col("diffsection").ta.ema(para2).expanding().alias("acceleration"),
        )
        .with_cols(
            col('diffsection', 'acceleration').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col(
                col("day_open") > col("prev_ma"),
                col("prev_day_close") * (1.0 + buy_ratio * 0.001),
                col("prev_day_close") * (1.0 + sell_ratio * 0.001),
            )
            .if_else()
            .alias("buy_point"),
            col(
                col("day_open") > col("prev_ma"),
                col("prev_day_close") * (1.0 - sell_ratio * 0.001),
                col("prev_day_close") * (1.0 - buy_ratio * 0.001),
            )
            .if_else()
            .alias("sell_point"),
        )
        .with_cols(
            (
                (col("high") > col("buy_point"))
                & (col("diffsection_1") > col("acceleration_1"))
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("low") < col("sell_point"))
                & (col("diffsection_1") < col("acceleration_1"))
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "t24",
        "按 T24 源码实现：双重平滑动量加速度 + 日切换开盘/昨收阈值突破进场。",
    )


def build_t25_expr() -> Expr:
    length = pms("t25_length", 5, 120, 5).limit(20)
    m = pms("t25_m", 1, 20, 1).limit(20)
    lots = pms("t25_lots", 1, 20, 1).limit(20)
    bull_length = pms("t25_bull_length", 10, 200, 10).limit(20)
    num_devs = pms("t25_num_devs", 0.5, 6.0, 0.5).limit(20)
    noise_limen = pms("t25_noise_limen", 1.0, 80.0, 1.0).limit(20)
    win_point = pms("t25_win_point", 1.0, 80.0, 1.0).limit(20)
    loss_point = pms("t25_loss_point", 1.0, 80.0, 1.0).limit(20)
    close_lag_exprs = [col("close").shift(i).expanding().alias(f"close_lag_{i}") for i in range(2, 21)]
    corr_exprs = [col("close", "close_1").corr().rolling(length).alias("corr_1")] + [
        col("close", f"close_lag_{i}").corr().rolling(length).alias(f"corr_{i}")
        for i in range(2, 21)
    ]
    q_exprs = [
        col(
            m >= float(i),
            col(f"corr_{i}") * col(f"corr_{i}"),
            0.0,
        )
        .if_else()
        .alias(f"q_{i}")
        for i in range(1, 21)
    ]
    q_cols = [f"q_{i}" for i in range(1, 21)]
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").mean().rolling(bull_length).alias("bull_mid"),
            col("close").std().rolling(bull_length).alias("bull_std"),
            col("close").shift(1).expanding().add_suffix("1"),
            *close_lag_exprs,
        )
        .with_cols(
            (col("bull_mid") + num_devs * col("bull_std")).alias("bull_up"),
            (col("bull_mid") - num_devs * col("bull_std")).alias("bull_down"),
            *corr_exprs,
        )
        .with_cols(
            *q_exprs,
        )
        .with_cols(
            (
                col(*q_cols).sum(axis=1)
                * 100.0
            ).alias("qma"),
            col('bull_up', 'bull_down').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            col("qma").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("close_1") > col("bull_up_1"))
                & (col("qma_1") > noise_limen)
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("close_1") < col("bull_down_1"))
                & (col("qma_1") > noise_limen)
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "t25",
        "按 T25 源码实现：布林带突破 + Close 与多阶滞后序列相关性平方和（QMA）噪声过滤触发开仓。",
    )


def build_t26_expr() -> Expr:
    af_step = pms("t26_af_step", 0.001, 0.2, 0.001).limit(20)
    af_limit = pms("t26_af_limit", 0.01, 0.5, 0.01).limit(20)
    adx_len = pms("t26_adx_len", 2, 60, 2).limit(20)
    level = pms("t26_level", 10.0, 60.0, 10.0).limit(20)
    stop_l = pms("t26_stop_l", 1.0, 40.0, 1.0).limit(20)
    prof_t = pms("t26_prof_t", 1.0, 80.0, 1.0).limit(20)
    e = (
        _full_kline_expr()
        .select("high", "low", "close")
        .with_cols(
            col("high", "low", "close").stra.t26_adx(adx_len).expanding().alias("adx"),
            col("high", "low", "close").stra.t26_psar_dir(af_step, af_limit).expanding().alias("sar_dir"),
        )
        .with_cols(
            col(col("adx") > 20.0, col("adx") < level, col("sar_dir") == -1.0).all(axis=1).fill_null(False).alias("open_long_raw"),
            col(col("adx") > 20.0, col("adx") < level, col("sar_dir") == 1.0).all(axis=1).fill_null(False).alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "t26",
        "按 R01/QM_SARcADX 逻辑实现：SAR 方向在 ADX 区间过滤内触发反转开仓。",
    )


def build_t27_expr() -> Expr:
    ema_length = pms("t27_ema_length", 2, 80, 2).limit(20)
    boll_length = pms("t27_boll_length", 5, 160, 5).limit(20)
    offset = pms("t27_offset", 0.1, 5.0, 0.1).limit(20)
    rsi_length = pms("t27_rsi_length", 2, 80, 2).limit(20)
    over_sold = pms("t27_over_sold", 1.0, 50.0, 1.0).limit(20)
    over_bought = pms("t27_over_bought", 50.0, 99.0, 50.0).limit(20)
    fast_length = pms("t27_fast_length", 2, 60, 2).limit(20)
    slow_length = pms("t27_slow_length", 4, 120, 4).limit(20)
    ma_length = pms("t27_ma_length", 2, 60, 2).limit(20)
    lots = pms("t27_lots", 1, 20, 1).limit(20)

    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col.lit(offset).alias("offset"),
            col("close").ta.ema(ema_length).expanding().alias("ema_main"),
            col("close").mean().rolling(boll_length).alias("boll_mid"),
            col("close").std().rolling(boll_length).alias("boll_std"),
            col("close").ta.rsi(rsi_length).expanding().alias("rsi_v"),
            col("close").ta.ema(fast_length).expanding().alias("ema_fast"),
            col("close").ta.ema(slow_length).expanding().alias("ema_slow"),
        )
        .with_cols(
            (col("boll_mid") + col("offset") * col("boll_std")).alias("up"),
            (col("boll_mid") - col("offset") * col("boll_std")).alias("down"),
            (col("ema_fast") - col("ema_slow")).alias("md"),
        )
        .with_cols(
            col("md").ta.ema(ma_length).expanding().alias("ema_avgmd"),
        )
        .with_cols(
            (col("md") - col("ema_avgmd")).alias("maiff"),
        )
        .with_cols(
            col('ema_main', 'up', 'down', 'rsi_v', 'maiff').shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            (
                (col("ema_main_1") > col("up_1"))
                & (col("rsi_v_1") > 50.0)
                & (col("maiff_1") > 0.0)
            )
            .fill_null(False)
            .alias("open_long_raw"),
            (
                (col("ema_main_1") < col("down_1"))
                & (col("rsi_v_1") < 50.0)
                & (col("maiff_1") < 0.0)
            )
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "t27",
        "按 T27 源码实现：EMA 与布林边界、RSI 与 MACD 差值共振触发开仓。",
    )


def build_t28_expr() -> Expr:
    lots = pms("t28_lots", 1, 20, 1).limit(20)
    buy_index = pms("t28_buy_index", 10.0, 100.0, 10.0).limit(20)
    sell_index = pms("t28_sell_index", 1.0, 90.0, 1.0).limit(20)
    offset = pms("t28_offset", 0.0, 10.0, 0.0).limit(20)
    ma_windows = [
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
        11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        24, 28, 32, 36, 40, 44, 48, 52, 56, 60, 64, 68, 72, 76, 80,
    ]
    pairs = [
        (1, 4), (2, 8), (3, 12), (4, 16), (5, 20),
        (6, 24), (7, 28), (8, 32), (9, 36), (10, 40),
        (11, 44), (12, 48), (13, 52), (14, 56), (15, 60),
        (16, 64), (17, 68), (18, 72), (19, 76), (20, 80),
    ]
    score_expr = sum(
        [col(col(f"ma_{a}") > col(f"ma_{b}"), 1.0, 0.0).if_else() for a, b in pairs],
        0.0,
    )
    e = (
        _full_kline_expr()
        .select("datetime", "open", "high", "low", "close", "volume")
        .with_cols(
            col("close").shift(1).expanding().add_suffix("1"),
        )
        .with_cols(
            *[col("close_1").mean().rolling(n).alias(f"ma_{n}") for n in ma_windows],
        )
        .with_cols(
            (score_expr / 20.0).alias("ma_index"),
        )
        .with_cols(
            (col("ma_index") >= buy_index / 100.0)
            .fill_null(False)
            .alias("open_long_raw"),
            (col("ma_index") <= sell_index / 100.0)
            .fill_null(False)
            .alias("open_short_raw"),
        )
        .with_cols(
            _pos_from_open_raw_expr("open_long_raw", "open_short_raw", "pos"),
        )
        .select(_four_signals_from_pos_expr("pos"))
    )
    return _with_meta(
        e,
        "t28",
        "按 T28 源码实现：20 组均线强弱投票形成 MaIndex，达到阈值触发开仓。",
    )


def iter_strategy_expr_builders() -> list[tuple[str, Callable[[], Expr]]]:
    return [
        ("c02", build_c02_expr),
        ("c03", build_c03_expr),
        ("c04", build_c04_expr),
        ("c05", build_c05_expr),
        ("c07", build_c07_expr),
        ("c08", build_c08_expr),
        ("c09", build_c09_expr),
        ("c10", build_c10_expr),
        ("c11", build_c11_expr),
        ("c12", build_c12_expr),
        ("c13", build_c13_expr),
        ("c14", build_c14_expr),
        ("c15", build_c15_expr),
        ("c16", build_c16_expr),
        ("c17", build_c17_expr),
        ("c18", build_c18_expr),
        ("c19", build_c19_expr),
        ("c20", build_c20_expr),
        ("c21", build_c21_expr),
        ("c22", build_c22_expr),
        ("c23", build_c23_expr),
        ("c24", build_c24_expr),
        ("c25", build_c25_expr),
        ("c26", build_c26_expr),
        ("c27", build_c27_expr),
        ("c28", build_c28_expr),
        ("c29", build_c29_expr),
        ("c30", build_c30_expr),
        ("c31", build_c31_expr),
        ("c32", build_c32_expr),
        ("c33", build_c33_expr),
        ("c34", build_c34_expr),
        ("c35", build_c35_expr),
        ("c36", build_c36_expr),
        ("c37", build_c37_expr),
        ("c38", build_c38_expr),
        ("c39", build_c39_expr),
        ("c41", build_c41_expr),
        ("c42", build_c42_expr),
        ("c44", build_c44_expr),
        ("c45", build_c45_expr),
        ("c46", build_c46_expr),
        ("c47", build_c47_expr),
        ("c48", build_c48_expr),
        ("c49", build_c49_expr),
        ("c50", build_c50_expr),
        ("c51", build_c51_expr),
        ("c52", build_c52_expr),
        ("c53", build_c53_expr),
        ("c54", build_c54_expr),
        ("c55", build_c55_expr),
        ("c56", build_c56_expr),
        ("c57", build_c57_expr),
        ("c58", build_c58_expr),
        ("c59", build_c59_expr),
        ("c60", build_c60_expr),
        ("c61", build_c61_expr),
        ("c62", build_c62_expr),
        ("c63", build_c63_expr),
        ("c64", build_c64_expr),
        ("c65", build_c65_expr),
        ("c66", build_c66_expr),
        ("c67", build_c67_expr),
        ("c68", build_c68_expr),
        ("c69", build_c69_expr),
        ("c70", build_c70_expr),
        ("c71", build_c71_expr),
        ("c72", build_c72_expr),
        ("c73", build_c73_expr),
        ("c74", build_c74_expr),
        ("c75", build_c75_expr),
        ("c76", build_c76_expr),
        ("q01", build_q01_expr),
        ("q02", build_q02_expr),
        ("sf01", build_sf01_expr),
        ("sf02", build_sf02_expr),
        ("sf03", build_sf03_expr),
        ("sf04", build_sf04_expr),
        ("sf05", build_sf05_expr),
        ("sf06", build_sf06_expr),
        ("sf07", build_sf07_expr),
        ("sf08", build_sf08_expr),
        ("sf09", build_sf09_expr),
        ("sf10", build_sf10_expr),
        ("sf11", build_sf11_expr),
        ("sf12", build_sf12_expr),
        ("sf13", build_sf13_expr),
        ("sf14", build_sf14_expr),
        ("sf15", build_sf15_expr),
        ("sf16", build_sf16_expr),
        ("sf17", build_sf17_expr),
        ("sf18", build_sf18_expr),
        ("sf19", build_sf19_expr),
        ("sf20", build_sf20_expr),
        ("sf21", build_sf21_expr),
        ("t01", build_t01_expr),
        ("t02", build_t02_expr),
        ("t03", build_t03_expr),
        ("t04", build_t04_expr),
        ("t05", build_t05_expr),
        ("t06", build_t06_expr),
        ("t07", build_t07_expr),
        ("t08", build_t08_expr),
        ("t09", build_t09_expr),
        ("t10", build_t10_expr),
        ("t11", build_t11_expr),
        ("t12", build_t12_expr),
        ("t13", build_t13_expr),
        ("t14", build_t14_expr),
        ("t15", build_t15_expr),
        ("t16", build_t16_expr),
        ("t17", build_t17_expr),
        ("t18", build_t18_expr),
        ("t19", build_t19_expr),
        ("t20", build_t20_expr),
        ("t21", build_t21_expr),
        ("t22", build_t22_expr),
        ("t23", build_t23_expr),
        ("t24", build_t24_expr),
        ("t25", build_t25_expr),
        ("t26", build_t26_expr),
        ("t27", build_t27_expr),
        ("t28", build_t28_expr),
        ("t29", build_t29_expr),
        ("fshgmz1", build_fshgmz1_expr),
        ("fshgmz2", build_fshgmz2_expr),
        ("fshgmz3", build_fshgmz3_expr),
        ("fshgmz4", build_fshgmz4_expr),
        ("fshgmz5", build_fshgmz5_expr),
        ("fshgmz6", build_fshgmz6_expr),
    ]
