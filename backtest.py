# ----- Backtest can be done for a month------
# change the value of start_date,end_date (line no 270,271) for any month and expiry dt (line no 32)
# In case of change of month expiry date need to be changed
from breeze_connect import BreezeConnect
import pandas as pd
import time
import threading
from datetime import datetime, timedelta
import requests
import os
import logging
logging.disable(logging.CRITICAL)

NIFTY_ORDER_QTY = 75
BNF_ORDER_QTY = 35
THRESHOLD = 0.1  # % change difference
thershold_delta = 0.1
COOLDOWN_SECONDS = 0
NIFTY_SYMBOL = "NIFTY"
BANKNIFTY_SYMBOL = "CNXBAN"
NIFTY_STOCK = "NIFTY 50" # Changed from "NIFTY 50"
BNF_STOCK = "NIFTY BANK" # Changed from "NIFTY BANK"
ltp_data = {NIFTY_STOCK: None, BNF_STOCK: None}
prev_close_data = {NIFTY_SYMBOL: None, BANKNIFTY_SYMBOL: None}
last_trade_time = 0
trade_executed = False
nifty_trade_price = 0
bnf_trade_price = 0
nifty_action = ""
backtest_date = ""
prev_close_date=""
expiry_dt = "2025-05-29"
trade_lock = threading.Lock()
max_pnl = 0
last_pnl = 0

# Step 1: Initialize and authenticate
api_key = "6Q2(s324f7=Y75@74171m9J66O6D0%88"
secret_key = "61q2106591287LR3295153JE128%p7@6"
session_token = "52064941"

breeze = BreezeConnect(api_key=api_key)
breeze.generate_session(api_secret=secret_key, session_token=session_token)

def  percnt_change(symbol_code,backtest_date,):
    global expiry_dt
    # Fetch 1-day interval data
    response =   breeze.get_historical_data_v2(
        interval="5minute",           # can also use "1second", "5minute", etc.
        from_date=f"{backtest_date}T09:25:00.000Z",
        to_date=f"{backtest_date}T09:25:00.000Z",
        stock_code=symbol_code,
        exchange_code="NFO",
        product_type="futures",
        expiry_date=expiry_dt,  # adjust expiry accordingly
        right="others",
        strike_price="0"
    )
    #print(f"Historical data response for {symbol_code} on {backtest_date}: {response}")
    df = pd.DataFrame(response)
    if not df.empty and 'Success' in df and len(df['Success']) > 0:
        close_price = df['Success'].iloc[0]['close']
        #print(f"{symbol_code} Futures Open Price: {close_price}")
    else:
        print("No data returned for open price.")
        return None


    close_prev = prev_close_data[symbol_code]
    if close_prev is not None:
        perc_change = ((close_price - close_prev) / close_prev) * 100
        return perc_change
    else:
        print(f"Previous close data not available for {symbol_code}")
        return None


def previous_close_date(backtest_date):
    today = datetime.strptime(backtest_date, "%Y-%m-%d")
    # Fetch historical data for the last 10 days (or more to be safe)
    start_date = today - timedelta(days=10)
    end_date = today - timedelta(days=1)  # yesterday or earlier

    historical_data = breeze.get_historical_data(
    interval="1day",
    from_date=start_date.strftime("%Y-%m-%d"),
    to_date=end_date.strftime("%Y-%m-%d"),
    stock_code="NIFTY",
    exchange_code="NSE",
    product_type="cash"
    )
    #print(f"Previous close historical data response for NIFTY: {historical_data}")
# Extract the latest available trading date
    if 'Success' in historical_data and historical_data['Success']:
        trading_dates = [entry['datetime'][:10] for entry in historical_data['Success']]
        previous_trading_date = trading_dates[-1]  # last trading day before today
        #print("Previous trading date:", previous_trading_date)
        return previous_trading_date
    else:
        print("Failed to fetch historical data or no trading data available.")
        return None



def check_and_trade(nifty_ltp,banknifty_ltp):
    global last_trade_time, trade_executed, nifty_action, nifty_trade_price, bnf_trade_price, THRESHOLD
    if trade_executed:
        return

    nifty_prev = prev_close_data[NIFTY_SYMBOL]
    banknifty_prev = prev_close_data[BANKNIFTY_SYMBOL]

    if nifty_prev is None or banknifty_prev is None:
        print("Previous close data not available for trading decision.")
        return
    #print("Change  : " + str(round(nifty_ltp,2)) + " " + str(round(nifty_prev,2)))
    nifty_change = ((nifty_ltp - nifty_prev) / nifty_prev) * 100
    banknifty_change = ((banknifty_ltp - banknifty_prev) / banknifty_prev) * 100
    diff = abs(nifty_change - banknifty_change)

    #print(f"[TICK] Nifty: {nifty_change:.2f}%, BankNifty: {banknifty_change:.2f}%, Diff: {diff:.2f}%, Thershold : {THRESHOLD}")

    if diff > THRESHOLD:
        with trade_lock:
            if nifty_change > banknifty_change:
               # place_order(breeze, "sell", NIFTY_STOCK, NIFTY_ORDER_QTY)
               # place_order(breeze, "buy", BNF_STOCK,BNF_ORDER_QTY)
                nifty_action = "sell"
                print("Action taken : nifty sell, bnf buy")
            else:
               # place_order(breeze, "buy", NIFTY_STOCK,NIFTY_ORDER_QTY)
               # place_order(border(breeze, "sell", BNF_STOCK,BNF_ORDER_QTY)
                nifty_action = "buy"
                print("Action taken : nifty buy, bnf sell")
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
    #print(f"Profit/Loss: ₹{diff:.2f}")
    return diff
# Step 2: Parameters for historical fetch
def fetch_futures(symbol_code):
    global expiry_dt,backtest_date
    response = breeze.get_historical_data_v2(
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
    #print(f"Futures historical data response for {symbol_code} on {backtest_date}: {response}")
    return response

def fetch_futures_prev(symbol_code):
    global expiry_dt
    #print("Back test date : " + backtest_date)
    prev_close_date = previous_close_date(backtest_date)
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
    #print(f"Previous close futures {prev_close_date} historical data response for {symbol_code}: {response}")

    df = pd.DataFrame(response)
    if not df.empty and 'Success' in df and len(df['Success']) > 0:
        close_price = df['Success'].iloc[-1]['close']
        #print(f"{symbol_code} Futures Close Price: {close_price}")
        return float(close_price)
    else:
        print("No data returned for previous close.")
        return None

# Step 3: Fetch previous close data first
def backtestdata_for_day(backtest_date):
    global  trade_executed, nifty_action, nifty_trade_price, bnf_trade_price, THRESHOLD, max_pnl,last_pnl

    # Reset trade status for the new day
    trade_executed = False
    nifty_action = ""
    nifty_trade_price = 0
    bnf_trade_price = 0

    if prev_close_date is None:
        print(f"Skipping backtest for {backtest_date} due to missing previous close date.")
        return

    prev_close_data[NIFTY_SYMBOL] = fetch_futures_prev(NIFTY_SYMBOL)
    prev_close_data[BANKNIFTY_SYMBOL] = fetch_futures_prev(BANKNIFTY_SYMBOL)

    if prev_close_data[NIFTY_SYMBOL] is None or prev_close_data[BANKNIFTY_SYMBOL] is None:
        print(f"Skipping backtest for {backtest_date} due to missing previous close data.")
        return

    # Now calculate percentage change using the fetched previous close data
    nifty_change_pct = percnt_change(NIFTY_SYMBOL,backtest_date)
    banknifty_change_pct = percnt_change(BANKNIFTY_SYMBOL,backtest_date)

    if nifty_change_pct is not None and banknifty_change_pct is not None:
        THRESHOLD = abs(nifty_change_pct - banknifty_change_pct) + thershold_delta
        #print(f"Revised Thershold : {THRESHOLD}")
        #print(f"New Threshold: {abs(nifty_change_pct - banknifty_change_pct)}")
    else:
        print("Could not calculate threshold due to data fetching issues.")
        return


    # Step 4: Fetch for NIFTY and BANKNIFTY
    nifty_response = fetch_futures(NIFTY_SYMBOL)
    banknifty_response = fetch_futures(BANKNIFTY_SYMBOL)

    if 'Success' not in nifty_response or 'Success' not in banknifty_response:
         print(f"Skipping backtest for {backtest_date} due to missing futures data.")
         return


    nifty = pd.DataFrame(nifty_response['Success'])
    banknifty = pd.DataFrame(banknifty_response['Success'])


    # Step 5: Normalize timestamp and inspect
    for df in [nifty, banknifty]:
        df['datetime'] = pd.to_datetime(df['datetime'])


    merged_df = pd.merge(nifty, banknifty, on='datetime', how='inner')  # use 'outer', 'left', or 'right' as needed


    data = []
    for index, row in merged_df.iterrows():
        check_and_trade(float(row['close_x']), float(row['close_y']))
        time.sleep(COOLDOWN_SECONDS) # Removed sleep for faster backtesting
        pnl = round(profit_loss(float(row['close_x']), float(row['close_y'])),2)
        data.append({"Profit":pnl})
        
    pnls = pd.DataFrame(data)
    if not pnls.empty:
        print(f"{backtest_date} : Last Close:", pnls['Profit'].tail(1).values[0])
        # Max close value
        print(f"{backtest_date} : Max Close:", pnls['Profit'].max())
        max_pnl = pnls['Profit'].max() + max_pnl
        # Last close value
        last_pnl = pnls['Profit'].tail(1).values[0] + last_pnl
    else:
        print(f"No PnL data generated for {backtest_date}")



start_date = datetime(2025, 5, 1)
end_date = datetime(2025, 5, 29)

#backtestdata_for_day(backtest_date)
# Loop through each day in the month
current_date = end_date
while current_date >= start_date:
    if current_date.weekday() < 5:
      date_str = current_date.strftime("%Y-%m-%d")
      backtest_date = date_str
      print(f"\n--- Backtesting for {date_str} ---")
      prev_close_date = previous_close_date(backtest_date)
      #print("Prev Close Date : " + prev_close_date)
      backtestdata_for_day(date_str)
    current_date -= timedelta(days=1)

print("Final for month : Max : " + str(max_pnl))
print("Final for month : last : " + str(last_pnl))
