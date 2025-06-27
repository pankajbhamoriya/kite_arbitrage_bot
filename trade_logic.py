# --- trade_logic.py ---
import time
import threading
from config import *
from logger import log_order_to_text

ltp_data = {NIFTY_STOCK: None, BNF_STOCK: None}
prev_close_data = {NIFTY_SYMBOL: None, BANKNIFTY_SYMBOL: None}
last_trade_time = 0
trade_executed = False
nifty_trade_price = 0
bnf_trade_price = 0
nifty_action = ""
trade_lock = threading.Lock()


def get_prev_close(breeze, symbol):
    try:
        quote = breeze.get_quotes(
            stock_code=symbol,
            exchange_code=EXCHANGE_CODE,
            expiry_date=EXPIRY_DATE ,
            product_type="futures",
            right="others",
            strike_price="0"
        )
        prev_close = float(quote['Success'][0].get('previous_close'))
        print(f"[INIT] {symbol} Previous Close: {prev_close}")
        return prev_close
    except Exception as e:
        print(f"[ERROR] Could not fetch previous close for {symbol}: {e}")
        return None


def place_order(breeze, act, symbol,qty):
    print(f"[TRADE] Placing {action.upper()} order on {symbol}")
    try:
        ltp = ltp_data[symbol]
        # Placeholder for actual order placement
        breeze.place_order(stock_code=symbol,exchange_code="NFO",product="futures", action= act,order_type="market", stoploss="0",quantity=qty,validity="day",disclosed_quantity="0",expiry_date=EXPIRY_DATE,right="others",strike_price="0")
        log_order_to_text(action, symbol, ltp, TEXT_LOG_FILE)
    except Exception as e:
        print(f"[ERROR] Failed to place order for {symbol}: {e}")


def check_and_trade(breeze):
    global last_trade_time, trade_executed, nifty_action, nifty_trade_price, bnf_trade_price
    if trade_executed:
        return
    if None in (ltp_data[NIFTY_STOCK], ltp_data[BNF_STOCK]):
        return

    nifty_ltp = ltp_data[NIFTY_STOCK]
    banknifty_ltp = ltp_data[BNF_STOCK]
    nifty_prev = prev_close_data[NIFTY_SYMBOL]
    banknifty_prev = prev_close_data[BANKNIFTY_SYMBOL]

    nifty_change = ((nifty_ltp - nifty_prev) / nifty_prev) * 100
    banknifty_change = ((banknifty_ltp - banknifty_prev) / banknifty_prev) * 100
    diff = abs(nifty_change - banknifty_change)

    print(f"[TICK] Nifty: {nifty_change:.2f}%, BankNifty: {banknifty_change:.2f}%, Diff: {diff:.2f}%")

    if diff > THRESHOLD and time.time() - last_trade_time > COOLDOWN_SECONDS:
        with trade_lock:
            if nifty_change > banknifty_change:
                place_order(breeze, "sell", NIFTY_STOCK, NIFTY_ORDER_QTY)
                place_order(breeze, "buy", BNF_STOCK,BNF_ORDER_QTY)
                nifty_action = "sell"
            else:
                place_order(breeze, "buy", NIFTY_STOCK,NIFTY_ORDER_QTY)
                place_order(breeze, "sell", BNF_STOCK,BNF_ORDER_QTY)
                nifty_action = "buy"
            nifty_trade_price = nifty_ltp
            bnf_trade_price = banknifty_ltp
            trade_executed = True
            last_trade_time = time.time()


def profit_loss():
    global trade_executed, nifty_action, nifty_trade_price, bnf_trade_price
    diff = 0
    if trade_executed:
        nifty_ltp = ltp_data[NIFTY_STOCK]
        banknifty_ltp = ltp_data[BNF_STOCK]
        if nifty_action == "buy":
            diff = (nifty_ltp - nifty_trade_price) * 75 + (bnf_trade_price - banknifty_ltp) * 35
        else:
            diff = (nifty_trade_price - nifty_ltp) * 75 + (banknifty_ltp - bnf_trade_price) * 35
    print(f"Profit/Loss: ₹{diff:.2f}")


def on_ticks(breeze, message):
    try:
        symbol = message.get("stock_name")
        ltp = float(message.get("last", 0))
        ltp_data[symbol] = ltp
        check_and_trade(breeze)
        profit_loss()
    except Exception as e:
        print(f"[ERROR] Tick error: {e}")
