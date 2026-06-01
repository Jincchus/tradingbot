import pandas as pd
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from strategies.base import BaseStrategy


class BollingerStrategy(BaseStrategy):
    """v4 — 볼린저 밴드 평균회귀. 하단 밴드 터치 시 매수, 중심선 회귀 시 청산."""

    PERIOD = 20
    NUM_STD = 2
    WATCHLIST = ["AAPL", "MSFT", "NVDA", "TSLA", "GOOGL"]
    POSITION_SIZE = 0.2

    def select_symbols(self) -> list[str]:
        return self.WATCHLIST

    def on_bar(self, bar) -> None:
        symbol = bar.symbol
        buffer = self._bar_buffer.get(symbol)
        if buffer is None or len(buffer) < self.PERIOD:
            return

        closes = pd.Series(list(buffer))
        window = closes.rolling(self.PERIOD)
        sma = window.mean().iloc[-1]
        # 볼린저는 모집단 표준편차(ddof=0)를 사용
        std = window.std(ddof=0).iloc[-1]
        lower_band = sma - self.NUM_STD * std

        # 보유 여부는 캐시로 판단 (매 bar REST 호출 금지 — rate limit 방지).
        has_position = symbol in self._positions
        close = float(bar.close)

        if close <= lower_band and not has_position:
            qty = int(self.budget * self.POSITION_SIZE / close)
            if qty > 0:
                self.trading_client.submit_order(
                    MarketOrderRequest(
                        symbol=symbol, qty=qty,
                        side=OrderSide.BUY, time_in_force=TimeInForce.DAY,
                    )
                )
                self.logger.info(f"BUY {qty} {symbol} @ {close} (lower band {lower_band:.2f})")

        elif close >= sma and has_position:
            self.trading_client.close_position(symbol)
            self.logger.info(f"SELL all {symbol} @ {close} (center {sma:.2f})")
