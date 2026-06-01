import pandas as pd
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from strategies.base import BaseStrategy


class RsiReversionStrategy(BaseStrategy):
    """v2 — RSI 평균회귀. 과매도(RSI<30) 매수, 과매수(RSI>70) 청산."""

    RSI_PERIOD = 14
    OVERSOLD = 30
    OVERBOUGHT = 70
    WATCHLIST = ["AAPL", "MSFT", "NVDA", "TSLA", "GOOGL"]
    POSITION_SIZE = 0.2

    def select_symbols(self) -> list[str]:
        return self.WATCHLIST

    def on_bar(self, bar) -> None:
        symbol = bar.symbol
        buffer = self._bar_buffer.get(symbol)
        # 14봉 변화량을 구하려면 종가 15개 필요
        if buffer is None or len(buffer) < self.RSI_PERIOD + 1:
            return

        closes = pd.Series(list(buffer))
        deltas = closes.diff().iloc[-self.RSI_PERIOD:]
        avg_gain = deltas.clip(lower=0).mean()
        avg_loss = (-deltas.clip(upper=0)).mean()

        if avg_loss == 0:
            rsi = 100.0  # 하락분이 없으면 RSI = 100
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))

        # 보유 여부는 캐시로 판단 (매 bar REST 호출 금지 — rate limit 방지).
        has_position = symbol in self._positions

        if rsi < self.OVERSOLD and not has_position:
            qty = int(self.budget * self.POSITION_SIZE / float(bar.close))
            if qty > 0:
                self.trading_client.submit_order(
                    MarketOrderRequest(
                        symbol=symbol, qty=qty,
                        side=OrderSide.BUY, time_in_force=TimeInForce.DAY,
                    )
                )
                self.logger.info(f"BUY {qty} {symbol} @ {bar.close} (RSI={rsi:.1f})")

        elif rsi > self.OVERBOUGHT and has_position:
            self.trading_client.close_position(symbol)
            self.logger.info(f"SELL all {symbol} (RSI={rsi:.1f})")
