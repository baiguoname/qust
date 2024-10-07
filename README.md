# Qust
Qust is a Rust libraries for building live-trading and back-test systems. It has the following features:
* **Fast**: It's way to handle or to save the kline data, tick data and strategy makes the backtest and live trading fast.
* **Extensible**: It provide many ways to build a strategy, and new ways can be implemented by needs, so that you can focus what you care. You can build a simple strategy or complicated one, then backtest it on kline data(for quick scruch) or tick data, on put it on live trading directly. For example, you can build a strategy by following ways:
    1. Accept kline flow, and return a target position.
    2. Accept tick data flow, and return a target position.
    3. Accept tick data flow, and return an order action.
    4. Accept kline and tick flow, return a target positon or an order action.
    5. Accept kline flow, and return a bool.(a least two of it make a  strategy, one for open position, another for close)
    6. Add filter conditions to an existed strategy.
    7. Add algorithm method to an existed strategy.
    8. Add order matching methods when backtest a strategy.
    9. Add valitality manager to strategies.
    10. Add portoflio manager to a pool of strategies.
    and so on.


See this notebook Example for more detail.