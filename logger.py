# --- logger.py ---
import time
import os

def log_order_to_text(action, symbol, price, file_path="orders_log.txt"):
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{now} | {action.upper():<6} | {symbol:<15} | Price: {price}\n"
    with open(file_path, "a") as f:
        f.write(line)

def ensure_log_dir(path):
    folder = os.path.dirname(path)
    if folder and not os.path.exists(folder):
        os.makedirs(folder)
