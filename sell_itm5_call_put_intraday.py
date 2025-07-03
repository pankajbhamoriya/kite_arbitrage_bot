!pip install breeze-connect
!pip install requests
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
NIFTY_SYMBOL = "CNXBAN"
BANKNIFTY_SYMBOL = ""
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
max_profit = 5000
max_loss = 3000
# Step 1: Initialize and authenticate
api_key = "6Q2(s324f7=Y75@74171m9J66O6D0%88"
secret_key = "61q2106591287LR3295153JE128%p7@6"
session_token = "52080397"
delta = 1000
strike_diff = 100
breeze = BreezeConnect(api_key=api_key)
breeze.generate_session(api_secret=secret_key, session_token=session_token)

def atm_strike_price(backtest_date,symbol_code):
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
        spot_price = df['Success'].iloc[0]['close']
        #print(f"{symbol_code} Futures Open Price: {close_price}")
    else:
        print("No data returned for open price.")
        return None

    if spot_price:
        # ---------- ROUND TO NEAREST 50 FOR NIFTY ----------
        atm_strike = round(spot_price / strike_diff) * strike_diff 
        #print(f"✅ ATM Strike for {symbol_code} at 09:30 on {backtest_date}: {atm_strike}")
        return atm_strike
    else:
        print("❌ 09:30 price not found.")
        return None


def check_and_trade(backtest_date,symbol_code):
    global nifty_trade_price,bnf_trade_price,trade_executed,delta
    if trade_executed:
        return
    strike_p_call = atm_strike_price(backtest_date,symbol_code) - delta   
    strike_p_put  = atm_strike_price(backtest_date,symbol_code)  + delta   
    #print(f"Strike price : {strike_p}")
   #print(f"[TICK] Nifty: {nifty_change:.2f}%, BankNifty: {banknifty_change:.2f}%, Diff: {diff:.2f}%, Thershold : {THRESHOLD}")
    nifty_trade_price = fetch_options_0930(backtest_date,symbol_code,strike_p_call,"call")
    print(f"Nifty trade price : {nifty_trade_price}")
    bnf_trade_price = fetch_options_0930(backtest_date,symbol_code,strike_p_put,"put")
    print(f"BankNifty trade price : {bnf_trade_price}")
    trade_executed = True
    

def profit_loss(nifty_ltp,banknifty_ltp):
    global trade_executed, nifty_action, nifty_trade_price, bnf_trade_price
    diff = 0

    if trade_executed:
        diff = ((nifty_trade_price - nifty_ltp) * NIFTY_ORDER_QTY) + ((bnf_trade_price - banknifty_ltp) * NIFTY_ORDER_QTY)
    #print(f"Profit/Loss: ₹{diff:.2f}")
    return diff
# Step 2: Parameters for historical fetch
def fetch_options(backtest_date,symbol_code,strike_p,call_put):
    global expiry_dt
    response = breeze.get_historical_data_v2(
        interval="5minute",           # can also use "1second", "5minute", etc.
        from_date=f"{backtest_date}T10:00:00.000Z",
        to_date=f"{backtest_date}T15:00:00.000Z",
        stock_code=symbol_code,
        exchange_code="NFO",
        product_type="options",
        expiry_date=expiry_dt,  # adjust expiry accordingly
        right=call_put,
        strike_price=strike_p
      )
    #print(f"Futures historical data response for {symbol_code} on {backtest_date}: {response}")
    return response

def fetch_options_0930(backtest_date,symbol_code,strike_p,call_put):
    global expiry_dt
    #print("Back test date : " + backtest_date)
    response = breeze.get_historical_data_v2(
        interval="5minute",           # can also use "1second", "5minute", etc.
        from_date=f"{backtest_date}T09:25:00.000Z",
        to_date=f"{backtest_date}T09:30:00.000Z",
        stock_code=symbol_code,
        exchange_code="NFO",
        product_type="options",
        expiry_date=expiry_dt,  # adjust expiry accordingly
        right=call_put,
        strike_price=strike_p
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
    global  trade_executed, nifty_action, nifty_trade_price, bnf_trade_price, THRESHOLD, max_pnl,last_pnl,delta

    # Reset trade status for the new day
    trade_executed = False
    nifty_trade_price = 0
    bnf_trade_price = 0

    check_and_trade(backtest_date,NIFTY_SYMBOL)
    strike_p_call = atm_strike_price(backtest_date,NIFTY_SYMBOL) - delta   
    strike_p_put  = atm_strike_price(backtest_date,NIFTY_SYMBOL)  + delta    
    # Step 4: Fetch for NIFTY and BANKNIFTY
    nifty_response = fetch_options(backtest_date,NIFTY_SYMBOL,strike_p_call,"call")
    banknifty_response = fetch_options(backtest_date,NIFTY_SYMBOL,strike_p_put,"put")

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
        #check_and_trade(float(row['close_x']), float(row['close_y']))
        time.sleep(COOLDOWN_SECONDS) # Removed sleep for faster backtesting
        pnl = round(profit_loss(float(row['close_x']), float(row['close_y'])),2)
        data.append({"Profit":pnl})
        if(pnl > max_profit ):
            break
        if(pnl < -max_loss):
            break
          
    

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



start_date = datetime(2025, 5, 2)
end_date = datetime(2025, 5, 29)

#backtestdata_for_day(backtest_date)
# Loop through each day in the month
current_date = end_date
while current_date >= start_date:
    if current_date.weekday() < 5:
      date_str = current_date.strftime("%Y-%m-%d")
      backtest_date = date_str
      print(f"\n--- Backtesting for {date_str} ---")
      backtestdata_for_day(date_str)
    current_date -= timedelta(days=1)

print("Final for month : Max : " + str(max_pnl))
print("Final for month : last : " + str(last_pnl))
