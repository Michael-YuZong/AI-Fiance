"""Simple daily-rule backtester."""

from __future__ import annotations

from typing import Callable, Dict, List

import numpy as np
import pandas as pd


class SimpleBacktester:
    """简易规则回测引擎。"""

    def __init__(self, price_df: pd.DataFrame):
        self.prices = price_df.copy()

    def run(
        self,
        entry_rule: Callable[[pd.DataFrame, int], bool],
        exit_rule: Callable[[pd.DataFrame, int], bool],
        initial_capital: float = 100000,
    ) -> Dict[str, object]:
        capital = initial_capital
        position = 0.0
        entry_price = 0.0
        entry_date = None
        trades: List[Dict[str, object]] = []
        equity_curve: List[float] = []

        for i in range(1, len(self.prices)):
            price = float(self.prices["close"].iloc[i])
            if position == 0 and entry_rule(self.prices, i):
                position = capital / price
                entry_price = price
                entry_date = self.prices.index[i]
            elif position > 0 and exit_rule(self.prices, i):
                capital = position * price
                trades.append(
                    {
                        "entry_date": entry_date,
                        "exit_date": self.prices.index[i],
                        "entry_price": entry_price,
                        "exit_price": price,
                        "return": (price - entry_price) / entry_price,
                    }
                )
                position = 0.0
            equity_curve.append(capital if position == 0 else position * price)

        if position > 0 and len(self.prices) > 0:
            final_price = float(self.prices["close"].iloc[-1])
            capital = position * final_price
            trades.append(
                {
                    "entry_date": entry_date,
                    "exit_date": self.prices.index[-1],
                    "entry_price": entry_price,
                    "exit_price": final_price,
                    "return": (final_price - entry_price) / entry_price,
                }
            )
            equity_curve[-1] = capital

        return self._analyze(trades, equity_curve, initial_capital)

    def _analyze(
        self,
        trades: List[Dict[str, object]],
        equity_curve: List[float],
        initial_capital: float,
    ) -> Dict[str, object]:
        if not trades:
            return {"error": "无交易记录，规则可能从未触发。"}

        returns = [float(trade["return"]) for trade in trades]
        equity = pd.Series(equity_curve)
        peak = equity.expanding().max()
        drawdown = (equity - peak) / peak
        total_return = float((equity.iloc[-1] - initial_capital) / initial_capital)
        n_years = len(equity) / 252
        annual_return = (1 + total_return) ** (1 / max(n_years, 0.01)) - 1

        result = {
            "total_trades": len(trades),
            "win_rate": float(sum(1 for value in returns if value > 0) / len(returns)),
            "avg_return_per_trade": float(np.mean(returns)),
            "profit_loss_ratio": (
                float(np.mean([value for value in returns if value > 0]) / abs(np.mean([value for value in returns if value < 0])))
                if any(value < 0 for value in returns)
                else float("inf")
            ),
            "total_return": total_return,
            "annual_return": float(annual_return),
            "max_drawdown": float(drawdown.min()),
            "trades": trades,
        }
        if len(trades) < 30:
            result["warning"] = f"仅 {len(trades)} 次交易，样本不足，结果无统计意义。"
        return result
