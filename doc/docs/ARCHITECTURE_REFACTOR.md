# Qust Architecture Boundaries

This document records the current hard boundaries for expression execution,
monitor rendering, callbacks, over-aware operators and plugins.

## 1. Core Boundary

`otters` core owns only:

- expression trees and serde
- forward/backward context planning
- basic operators
- packet execution

Core must not own monitor sessions, browser transport, slot callback UI,
Python business helpers, or domain trading logic.

Domain-specific operators such as futures, kline, stock OLS/residual, strategy
and backtest live in `otters-domain`, which is a workspace crate parallel to
`otters`. New business operators must be added to a domain crate, not to core
expression modules.

## 2. Plugin Boundary

Plugins are explicit runtime objects.

Plugin trait, plugin registry, and core plugin lifetimes are defined in
`otters`. Concrete plugins live outside core, in a crate parallel to core
such as `otters-plugin`. `DataFrame` owns the plugin registry so execution
plans can carry explicit runtime resources without hidden globals.

The first concrete plugin is `DataPool`, implemented in `otters-plugin`:

```python
pool = qs.DataPool("raw_pool")
df = qs.select(col.all.save_data_inner("raw", pool))
df.calc_data(data)
pool.get_dataframe("raw")
```

Rules:

- `save_data(key)` writes to the shared default automatic `DataPool`; `key`
  is the data name inside that pool.
- `save_data_inner(key, pool)` writes to the explicit `DataPool`.
- `col.get_data(key)` reads from the automatically created `DataPool`.
- `DataFrame` owns the plugin registry that carries the `DataPool` handle.
- `DataPool` lifetime is controlled by the caller.
- expression serde stores the stable `DataPool` plugin key, not the Python object.
- after deserialization, caller must recreate a `DataPool` with the same key
  and register it on the `DataFrame` before execution.
- if the plugin key is unavailable at execution time, execution fails loudly.

There must be no global `DataPool` registry. `Expr` may carry a transient
plugin sidecar while being assembled, but once it is inserted into a
`DataFrame`, the `DataFrame` registry owns the runtime plugin handle.

## 3. Monitor Boundary

Monitor runtime state is a concrete plugin, not an implicit property of a
`DataFrame` and not a field embedded inside monitor plot expressions.

`otters-monitor-runtime` implements `MonitorRuntimePlugin`. The core `otters`
crate only defines the generic `Plugin` trait and `DataFrame` plugin registry;
core does not know what a monitor is.

Main path:

```python
monitor = qs.Monitor(background="black").extract_from_df(df)
monitor.plot(df, data)
```

Convenience path:

```python
df.plot(data)
```

The convenience path is only sugar for:

```python
monitor = qs.Monitor().extract_from_df(df)
monitor.plot(df, data)
```

Rules:

- `Monitor.extract_from_df(df)` parses monitor plot expressions and builds the
  monitor layout explicitly.
- `Monitor.plot(df, data, ...)` owns session creation and display.
- `df.plot(...)` must stay a thin wrapper, not a second monitor runtime.
- resolved monitor plot expressions store only slot/grid/channel/plugin-key
  binding data.
- the actual `Monitor` object is carried by `MonitorRuntimePlugin` in the
  `DataFrame` plugin registry.
- monitor execution must fail loudly if the runtime plugin key is missing.
- `df.calc_data(...)` does not auto-bind monitor plots. If a calc-only path
  contains unresolved monitor plots, it should fail instead of silently opening
  or resolving monitor state.

## 4. Callback Boundary

Callback binding is structural.

```python
plot_expr.attach_to_monitor(callback_expr.scatter_select(filter_expr))
```

Rules:

- the receiver of `attach_to_monitor` must contain exactly one monitor plot
- zero plots is an error
- multiple plots is an error
- callback body slots are independent from source plot slots
- callback body plots must declare their own monitor slot if they need a stable
  target
- callback source is the input scope where `attach_to_monitor` is called
- callback source never inherits the source scatter plot's `over(...)`

`x_slider` and `scatter_select` are the same abstraction layer: both are
callback triggers that build a selection payload, then execute callback code.
They differ only in payload semantics.

## 5. Interaction Boundary

All plot frontend interaction must flow through `PlotFrontendBehavior`.

The trait has first-class extension points for:

- hover
- zoom
- drag/pan
- select
- query
- slider

Adding a new interaction must extend or implement this trait/interface instead
of patching individual chart call sites. A plot may return a no-op mode for any
capability it does not support.

## 6. Over Boundary

Over-aware operators must declare their index domain.

Rules:

- `BwCtx::has_over()` is the single Rust-side predicate for over context.
- `trade_row` in non-over context emits global row indices.
- `trade_row` in over context emits group-local row indices plus an internal
  group marker.
- `get_by_index` in over context consumes the group marker and indexes inside
  each group independently.
- `get_by_index(index_expr=...)` rejects index expressions that contain their
  own `over(...)`; the outer context must provide over.
- `near(...)` in over context expands within each group only.
- callback source filtering must be expressed with columns in `filter_expr`,
  not by inheriting an outer plot's over context.

The goal is correctness over convenience: ambiguous over behavior should error
instead of guessing.
