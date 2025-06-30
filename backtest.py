from breeze_connect import BreezeConnect
import pandas as pd
import time
import threading
NIFTY_ORDER_QTY = 75
BNF_ORDER_QTY = 35
THRESHOLD = 0.5  # % change difference
COOLDOWN_SECONDS = 0
NIFTY_SYMBOL = "NIFTY"
BANKNIFTY_SYMBOL = "CNXBAN"
NIFTY_STOCK = "NIFTY 50"
BNF_STOCK = "NIFTY BANK"
ltp_data = {NIFTY_STOCK: None, BNF_STOCK: None}
prev_close_data = {NIFTY_SYMBOL: None, BANKNIFTY_SYMBOL: None}
last_trade_time = 0
trade_executed = False
nifty_trade_price = 0
bnf_trade_price = 0
nifty_action = ""
backtest_date = "2025-06-10"
prev_close_date = "2025-06-09"
expiry_dt = "2025-06-26"
trade_lock = threading.Lock()

# Step 1: Initialize and authenticate
api_key = "6Q2(s324f7=Y75@74171m9J66O6D0%88"
secret_key = "61q2106591287LR3295153JE128%p7@6"
session_token = "52033315"

breeze = BreezeConnect(api_key=api_key)
breeze.generate_session(api_secret=secret_key, session_token=session_token)


def check_and_trade(nifty_ltp,banknifty_ltp):
    global last_trade_time, trade_executed, nifty_action, nifty_trade_price, bnf_trade_price
    if trade_executed:
        return

    nifty_prev = prev_close_data[NIFTY_SYMBOL]
    banknifty_prev = prev_close_data[BANKNIFTY_SYMBOL]

    nifty_change = ((nifty_ltp - nifty_prev) / nifty_prev) * 100
    banknifty_change = ((banknifty_ltp - banknifty_prev) / banknifty_prev) * 100
    diff = abs(nifty_change - banknifty_change)

    print(f"[TICK] Nifty: {nifty_change:.2f}%, BankNifty: {banknifty_change:.2f}%, Diff: {diff:.2f}%")

    if diff > THRESHOLD:
        with trade_lock:
            if nifty_change > banknifty_change:
               # place_order(breeze, "sell", NIFTY_STOCK, NIFTY_ORDER_QTY)
               # place_order(breeze, "buy", BNF_STOCK,BNF_ORDER_QTY)
                nifty_action = "sell"
            else:
               # place_order(breeze, "buy", NIFTY_STOCK,NIFTY_ORDER_QTY)
               # place_order(breeze, "sell", BNF_STOCK,BNF_ORDER_QTY)
                nifty_action = "buy"
            nifty_trade_price = nifty_ltp
            bnf_trade_price = banknifty_ltp
            trade_executed = True
            #last_trade_time = time.time()


def profit_loss(nifty_ltp,banknifty_ltp):
    global trade_executed, nifty_action, nifty_trade_price, bnf_trade_price
    diff = 0
    if trade_executed:
        if nifty_action == "buy":
            diff = ((nifty_ltp - nifty_trade_price) * NIFTY_ORDER_QTY) + ((bnf_trade_price - banknifty_ltp) * BNF_ORDER_QTY)
        else:
            diff = ((nifty_trade_price - nifty_ltp) * NIFTY_ORDER_QTY) + ((banknifty_ltp - bnf_trade_price) * BNF_ORDER_QTY)
    print(f"Profit/Loss: ₹{diff:.2f}")
# Step 2: Parameters for historical fetch
def fetch_futures(symbol_code):
    global expiry_dt,backtest_date
    return breeze.get_historical_data_v2(
        interval="5minute",           # can also use "1second", "5minute", etc.
        from_date=f"{backtest_date}T09:30:00.000Z",
        to_date=f"{backtest_date}T15:15:00.000Z",
        stock_code=symbol_code,
        exchange_code="NFO",
        product_type="futures",
        expiry_date=expiry_dt,  # adjust expiry accordingly
        right="others",
        strike_price="0"
    )

def fetch_futures_prev(symbol_code):
    global expiry_dt,prev_close_date
    response = breeze.get_historical_data_v2(
        interval="1minute",           # can also use "1second", "5minute", etc.
        from_date=f"{prev_close_date}T15:29:00.000Z",
        to_date=f"{prev_close_date}T15:30:00.000Z",
        stock_code=symbol_code,
        exchange_code="NFO",
        product_type="futures",
        expiry_date=expiry_dt,  # adjust expiry accordingly
        right="others",
        strike_price="0"
    )
    df = pd.DataFrame(response)
    if not df.empty:
        close_price = df['Success'].apply(lambda x: x['close'])
        print(f"{symbol_code} Futures Close Price: {close_price}")
        return float(close_price)
    else:
        print("No data returned.")
        return None

# Step 3: Fetch for NIFTY and BANKNIFTY
nifty = pd.DataFrame(fetch_futures("NIFTY"))
banknifty = pd.DataFrame(fetch_futures("CNXBAN"))
#print(nifty)
# Step 4: Normalize timestamp and inspect
for df,name in [(banknifty,"BankNifty")]:
    df['close'] = df['Success'].apply(lambda x: x['close'])
    df['datetime'] = pd.to_datetime(df['Success'].apply(lambda x: x['datetime']))
    # Drop the original response column if you no longer need it
    df.drop(columns='Success', inplace=True)
    nifty_df=df
    #print(df.head())
for df,name in [(nifty,"Nifty")]:
    df['close'] = df['Success'].apply(lambda x: x['close'])
    df['datetime'] = pd.to_datetime(df['Success'].apply(lambda x: x['datetime']))
    # Drop the original response column if you no longer need it
    df.drop(columns='Success', inplace=True)
    banknifty_df=df
    #print(df.head())

merged_df = pd.merge(nifty_df, banknifty_df, on='datetime', how='inner')  # use 'outer', 'left', or 'right' as needed

#print(merged_df.head())
prev_close_data[NIFTY_SYMBOL] = fetch_futures_prev(NIFTY_SYMBOL)
prev_close_data[BANKNIFTY_SYMBOL] = fetch_futures_prev(BANKNIFTY_SYMBOL)
for index, row in merged_df.iterrows():
    merged_df.at[index, '% nifty'] = row['Status_x'] + row['Status_y']
    check_and_trade(row['close_y'], row['close_x'])
    time.sleep(COOLDOWN_SECONDS)
    profit_loss(row['close_y'], row['close_x'])
