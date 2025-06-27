from breeze_client import connect_to_breeze
from trade_logic import get_prev_close, on_ticks, prev_close_data, NIFTY_SYMBOL, BANKNIFTY_SYMBOL, start_streaming
from config import *
import time

if __name__ == "__main__":
    breeze = connect_to_breeze()
    prev_close_data[NIFTY_SYMBOL] = get_prev_close(breeze, NIFTY_SYMBOL)
    prev_close_data[BANKNIFTY_SYMBOL] = get_prev_close(breeze, BANKNIFTY_SYMBOL)

    breeze.ws_connect()
    time.sleep(2)
    breeze.on_ticks = lambda msg: on_ticks(breeze, msg)

    for symbol in [NIFTY_SYMBOL, BANKNIFTY_SYMBOL]:
        breeze.subscribe_feeds(
            exchange_code=EXCHANGE_CODE,
            stock_code=symbol,
            expiry_date=EXPIRY_DATE,
            product_type="futures",
            right="others",
            strike_price=0.0,
            get_market_depth=False,
            get_exchange_quotes=True
        )
    print("[WS] Tick Stream Active")
    while True:
        time.sleep(1)
