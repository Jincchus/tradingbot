import pandas as pd
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from strategies.base import BaseStrategy


class MacdStrategy(BaseStrategy):
    """v3 — MACD 시그널 크로스. MACD가 Signal을 상향/하향 돌파할 때 매매."""

    FAST = 12
    SLOW = 26
    SIGNAL = 9
    WATCHLIST = ["AAPL", "MSFT", "NVDA", "TSLA", "GOOGL"]
    POSITION_SIZE = 0.2

    def select_symbols(self) -> list[str]:
        return self.WATCHLIST

    def on_bar(self, bar) -> None:
        symbol = bar.symbol
        buffer = self._bar_buffer.get(symbol)
        # EMA26 + Signal9 안정화를 위해 최소 35봉 필요
        if buffer is None or len(buffer) < self.SLOW + self.SIGNAL:
            return

        closes = pd.Series(list(buffer))
        ema_fast = closes.ewm(span=self.FAST, adjust=False).mean()
        ema_slow = closes.ewm(span=self.SLOW, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=self.SIGNAL, adjust=False).mean()

        is_bull_cross = (macd_line.iloc[-1] > signal_line.iloc[-1] and
                         macd_line.iloc[-2] <= signal_line.iloc[-2])
        is_bear_cross = (macd_line.iloc[-1] < signal_line.iloc[-1] and
                         macd_line.iloc[-2] >= signal_line.iloc[-2])

        # 보유 여부는 캐시로 판단 (매 bar REST 호출 금지 — rate limit 방지).
        has_position = symbol in self._positions

        if is_bull_cross and not has_position:
            qty = int(self.budget * self.POSITION_SIZE / float(bar.close))
            if qty > 0:
                self.trading_client.submit_order(
                    MarketOrderRequest(
                        symbol=symbol, qty=qty,
                        side=OrderSide.BUY, time_in_force=TimeInForce.DAY,
                    )
                )
                self.logger.info(f"BUY {qty} {symbol} @ {bar.close} (MACD cross up)")

        elif is_bear_cross and has_position:
            self.trading_client.close_position(symbol)
            self.logger.info(f"SELL all {symbol} (MACD cross down)")
