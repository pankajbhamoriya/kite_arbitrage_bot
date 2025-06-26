from breeze_connect import BreezeConnect
import threading
import time
import pandas as pd
import os

# --- Breeze Credentials ---
API_KEY = "your_api_key"
API_SECRET = "your_api_secret"
SESSION_TOKEN = "your_session_token"

# --- Contract Details ---
NIFTY_SYMBOL = "NIFTY24JULFUT"
BANKNIFTY_SYMBOL = "BANKNIFTY24JULFUT"
EXCHANGE_CODE = "NFO"
ORDER_QTY = 50
THRESHOLD = 0.5  # % change difference
COOLDOWN_SECONDS = 30  # between trades

# --- State ---
ltp_data = {NIFTY_SYMBOL: None, BANKNIFTY_SYMBOL: None}
prev_close_data = {NIFTY_SYMBOL: None, BANKNIFTY_SYMBOL: None}
last_trade_time = 0
trade_lock = threading.Lock()
excel_file = "orders.xlsx"

# --- Setup BreezeConnect ---
breeze = BreezeConnect(api_key=API_KEY)
breeze.generate_session(api_secret=API_SECRET, session_token=SESSION_TOKEN)

# --- Initialize Excel log ---
def init_excel():
    if not os.path.exists(excel_file):
        df = pd.DataFrame(columns=["Time", "Action", "Symbol", "Price"])
        df.to_excel(excel_file, index=False)

def log_order_to_excel(action, symbol, price):
    df = pd.read_excel(excel_file)
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    new_row = {"Time": now, "Action": action.upper(), "Symbol": symbol, "Price": price}
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    df.to_excel(excel_file, index=False)

# --- Fetch Previous Close ---
def get_prev_close(symbol):
    try:
        quote = breeze.get_quotes(stock_code=symbol, exchange_code=EXCHANGE_CODE, product_type="futures")
        prev_close = float(quote['previous_close'])
        print(f"[INIT] {symbol} Previous Close: {prev_close}")
        return prev_close
    except Exception as e:
        print(f"[ERROR] Could not fetch previous close for {symbol}: {e}")
        return None

# --- Place Order and Log ---

def place_order(action, symbol):
    print(f"[TRADE] Placing {action.upper()} order on {symbol}")
    try:
        response = breeze.place_order(
            stock_code=symbol,
            exchange_code=EXCHANGE_CODE,
            product="futures",
            action=action,
            order_type="market",
            quantity=ORDER_QTY,
            price=0,
            validity="day"
        )
        # Log last traded price at time of order
        ltp = ltp_data[symbol]
        log_order_to_excel(action, symbol, ltp)
    except Exception as e:
        print(f"[ERROR] Failed to place order for {symbol}: {e}")

# --- Strategy Logic ---
trade_executed = False
def check_and_trade():
    global last_trade_time
    if trade_executed:
        return
    if None in (ltp_data[NIFTY_SYMBOL], ltp_data[BANKNIFTY_SYMBOL]):
        return

    nifty_ltp = ltp_data[NIFTY_SYMBOL]
    banknifty_ltp = ltp_data[BANKNIFTY_SYMBOL]
    nifty_prev = prev_close_data[NIFTY_SYMBOL]
    banknifty_prev = prev_close_data[BANKNIFTY_SYMBOL]

    nifty_change = ((nifty_ltp - nifty_prev) / nifty_prev) * 100
    banknifty_change = ((banknifty_ltp - banknifty_prev) / banknifty_prev) * 100
    diff = abs(nifty_change - banknifty_change)

    print(f"[TICK] Nifty: {nifty_change:.2f}%, BankNifty: {banknifty_change:.2f}%, Diff: {diff:.2f}%")

    if diff > THRESHOLD and time.time() - last_trade_time > COOLDOWN_SECONDS:
        with trade_lock:
            if nifty_change > banknifty_change:
                place_order("sell", NIFTY_SYMBOL)
                place_order("buy", BANKNIFTY_SYMBOL)
            else:
                place_order("buy", NIFTY_SYMBOL)
                place_order("sell", BANKNIFTY_SYMBOL)
            trade_executed = True 
            last_trade_time = time.time()

# --- WebSocket Handler ---
def on_ticks(message):
    try:
        for tick in message['data']:
            symbol = tick['stock_code']
            ltp = float(tick['last_traded_price'])
            ltp_data[symbol] = ltp
        check_and_trade()
    except Exception as e:
        print(f"[ERROR] Tick Error: {e}")

# --- WebSocket Start ---
def start_streaming():
    print("[WS] Connecting...")
    breeze.ws_connect()
    breeze.on_ticks = on_ticks

    for symbol in [NIFTY_SYMBOL, BANKNIFTY_SYMBOL]:
        breeze.subscribe_feeds(stock_code=symbol, exchange_code=EXCHANGE_CODE, product_type="futures")

    print("[WS] Tick Stream Active")

# --- Main ---
if __name__ == "__main__":
    init_excel()
    prev_close_data[NIFTY_SYMBOL] = get_prev_close(NIFTY_SYMBOL)
    prev_close_data[BANKNIFTY_SYMBOL] = get_prev_close(BANKNIFTY_SYMBOL)

    start_streaming()
    while True:
        time.sleep(1)
