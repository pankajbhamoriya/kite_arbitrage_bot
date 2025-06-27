from breeze_connect import BreezeConnect
import threading
import time
import pandas as pd
import os

API_KEY = "6Q2(s324f7=Y75@74171m9J66O6D0%88"
API_SECRET = "61q2106591287LR3295153JE128%p7@6"
SESSION_TOKEN = "52008022"

# --- Contract Details ---
NIFTY_SYMBOL = "NIFTY"
BANKNIFTY_SYMBOL = "CNXBAN"
NIFTY_STOCK = "NIFTY 50"
BNF_STOCK = "NIFTY BANK"
EXCHANGE_CODE = "NFO"
ORDER_QTY = 50
THRESHOLD = 0.1  # % change difference
COOLDOWN_SECONDS = 30  # between trades

# --- State ---
ltp_data = {NIFTY_STOCK: None, BNF_STOCK: None}
prev_close_data = {NIFTY_SYMBOL: None, BANKNIFTY_SYMBOL: None}
last_trade_time = 0
trade_lock = threading.Lock()
nifty_trade_price = 0
bnf_trade_price = 0
nifty_action = ""
excel_file = "orders.xlsx"
trade_executed = False
# --- Setup BreezeConnect ---
breeze = BreezeConnect(api_key=API_KEY)
breeze.generate_session(api_secret=API_SECRET, session_token=SESSION_TOKEN)
# --- Setup BreezeConnect ---
breeze = BreezeConnect(api_key=API_KEY)
breeze.generate_session(api_secret=API_SECRET, session_token=SESSION_TOKEN)

# --- Initialize Excel log ---

def log_order_to_text(action, symbol, price, file_path="orders_log.txt"):
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{now} | {action.upper():<6} | {symbol:<15} | Price: {price}\n"
    with open(file_path, "a") as f:
        f.write(line)

# --- Fetch Previous Close ---
def get_prev_close(symbol):
    try:
        quote=breeze.get_quotes(stock_code=symbol,exchange_code="NFO",expiry_date="2025-07-31",product_type="futures",right="others",strike_price="0")
        prev_close = float(quote['Success'][0].get('previous_close'))
        print(f"[INIT] {symbol} Previous Close: {prev_close}")
        return prev_close
    except Exception as e:
        print(f"[ERROR] Could not fetch previous close for {symbol}: {e}")
        return None

# --- Place Order and Log ---

def place_order(action, symbol):
    print(f"[TRADE] Placing {action.upper()} order on {symbol}")
    try:
#        response = breeze.place_order(stock_code=symbol,exchange_code=EXCHANGE_CODE,product="futures",action=action,order_type="market",quantity=ORDER_QTY,price=0,validity="day")
        # Log last traded price at time of order
        ltp = ltp_data[symbol]
        log_order_to_text(action, symbol, ltp)
    except Exception as e:
        print(f"[ERROR] Failed to place order for {symbol}: {e}")



# --- show profit/loss
def profit_loss():
    global trade_executed
    global nifty_action,bnf_trade_price,nifty_trade_price
    diff=0
    if trade_executed:
        nifty_ltp = ltp_data[NIFTY_STOCK]
        banknifty_ltp = ltp_data[BNF_STOCK]
        if nifty_action == "BUY":
            nifty_change = (nifty_ltp - nifty_trade_price)*75
            banknifty_change = (bnf_trade_price - banknifty_ltp)*35
            diff = nifty_change + banknifty_change
        else:
            nifty_change = (nifty_trade_price - nifty_ltp)*75
            banknifty_change = (banknifty_ltp - bnf_trade_price)*35
            diff = nifty_change + banknifty_change
    print(f"Profit/Loss : {diff}")

# --- Strategy Logic ---

def check_and_trade():
    global last_trade_time
    global trade_executed
    global nifty_action,nifty_trade_price,bnf_trade_price
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
                place_order("sell", NIFTY_STOCK)
                place_order("buy", BNF_STOCK)
                nifty_action = "sell"
                nifty_trade_price = ltp_data[NIFTY_STOCK]
                bnf_trade_price = ltp_data[BNF_STOCK]
            else:
                place_order("buy", NIFTY_STOCK)
                place_order("sell", BNF_STOCK)
                nifty_action = "buy"
                nifty_trade_price = ltp_data[NIFTY_STOCK]
                bnf_trade_price = ltp_data[BNF_STOCK]
            trade_executed = True
            last_trade_time = time.time()

# Search futures instruments
def search_futures_instruments(stock_code="NIFTY", exchange_code="NFO"):
    try:
        results = breeze.get_names("NFO","NIFTY")
        for contract in results:
            print(f"Symbol: {contract['stock_code']} | "
                  f"Expiry: {contract['expiry_date']} | "
                  f"Lot Size: {contract['lot_size']}")

        print(results)
        return results

    except Exception as e:
        print(f"[ERROR] Failed to search instruments: {e}")
        return []
                                                                                                                                                                                                 137,15        75%
# --- WebSocket Handler ---
def on_ticks(message):
    #print(message)
    symbol = message.get("stock_name", "Unknown")
    ltp = float(message.get("last", "N/A"))
    #print(f"[{symbol}] Last Price: {ltp}")
    ltp_data[symbol] = ltp
    print(ltp_data)
    check_and_trade()
    profit_loss()


# --- WebSocket Start ---
def start_streaming():
    print("[WS] Connecting...")
    breeze.ws_connect()
    time.sleep(2)
    breeze.on_ticks = on_ticks

    #breeze.subscribe_feeds(exchange_code="NSE",stock_code="NIFTY",product_type="cash",get_market_depth=False,get_exchange_quotes=True)
    breeze.subscribe_feeds(exchange_code= "NFO",stock_code="NIFTY",expiry_date="31-Jul-2025", strike_price="0", right="others", product_type="futures", get_market_depth=False ,get_exchange_quotes=True)
    for symbol in [NIFTY_SYMBOL, BANKNIFTY_SYMBOL]:
        breeze.subscribe_feeds(exchange_code= "NFO",stock_code=symbol,expiry_date="31-Jul-2025", strike_price="0", right="others", product_type="futures", get_market_depth=False ,get_exchange_quotes=True)
    print("[WS] Tick Stream Active")

# --- Main ---
if __name__ == "__main__":
    prev_close_data[NIFTY_SYMBOL] = get_prev_close(NIFTY_SYMBOL)
    prev_close_data[BANKNIFTY_SYMBOL] = get_prev_close(BANKNIFTY_SYMBOL)
    start_streaming()
    while True:
        time.sleep(1)
 
