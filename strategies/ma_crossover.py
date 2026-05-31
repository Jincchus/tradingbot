import pandas as pd
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from strategies.base import BaseStrategy


class MACrossoverStrategy(BaseStrategy):
    SHORT_WINDOW = 10
    LONG_WINDOW = 30
    WATCHLIST = ["AAPL", "MSFT", "NVDA", "TSLA", "GOOGL"]
    POSITION_SIZE = 0.2

    def select_symbols(self) -> list[str]:
        return self.WATCHLIST

    def on_bar(self, bar) -> None:
        symbol = bar.symbol
        buffer = self._bar_buffer.get(symbol)
        if buffer is None or len(buffer) < self.LONG_WINDOW + 1:
            return

        closes = pd.Series(list(buffer))
        short_ma = closes.rolling(self.SHORT_WINDOW).mean()
        long_ma = closes.rolling(self.LONG_WINDOW).mean()

        is_golden_cross = (short_ma.iloc[-1] > long_ma.iloc[-1] and
                           short_ma.iloc[-2] <= long_ma.iloc[-2])
        is_death_cross = (short_ma.iloc[-1] < long_ma.iloc[-1] and
                          short_ma.iloc[-2] >= long_ma.iloc[-2])

        # 보유 여부는 캐시로 판단 (매 bar REST 호출 금지 — rate limit 방지).
        # 캐시는 sync_state()와 체결 이벤트(on_order_filled)로 갱신됨.
        has_position = symbol in self._positions

        if is_golden_cross and not has_position:
            qty = int(self.budget * self.POSITION_SIZE / float(bar.close))
            if qty > 0:
                self.trading_client.submit_order(
                    MarketOrderRequest(
                        symbol=symbol, qty=qty,
                        side=OrderSide.BUY, time_in_force=TimeInForce.DAY,
                    )
                )
                self.logger.info(f"BUY {qty} {symbol} @ {bar.close}")

        elif is_death_cross and has_position:
            self.trading_client.close_position(symbol)
            self.logger.info(f"SELL all {symbol}")
