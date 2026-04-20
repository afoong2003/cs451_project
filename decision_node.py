import json
import argparse
from collections import deque
import pandas as pd
import numpy as np

INGESTION_NODE_IP = "192.168.1.1" 
EXECUTION_NODE_IP = "192.168.1.12" 


FAST_WINDOW = 10
SLOW_WINDOW = 50
RSI_WINDOW = 14
WARMUP_BARS = 200



def _normalize_prices(price_data):
    if isinstance(price_data, pd.DataFrame):
        if "Close" not in price_data:
            raise ValueError("DataFrame input must contain a 'Close' column.")
        close_prices = price_data["Close"]
        if isinstance(close_prices, pd.DataFrame):
            close_prices = close_prices.iloc[:, 0]
    elif isinstance(price_data, pd.Series):
        close_prices = price_data
    else:
        close_prices = pd.Series(list(price_data), dtype="float64")

    close_prices = close_prices.dropna().astype(float)
    if close_prices.empty:
        raise ValueError("Price data is empty after normalization.")

    return close_prices


def compute_sma(price_data, window):
    close_prices = _normalize_prices(price_data)
    if len(close_prices) < window:
        raise ValueError(f"Need at least {window} prices to compute SMA-{window}.")

    sma_value = float(close_prices.rolling(window=window).mean().iloc[-1])
    if pd.isna(sma_value):
        raise ValueError(f"Could not compute SMA-{window}; result is NaN.")

    return sma_value


def compute_rsi(price_data, window=14):
    close_prices = _normalize_prices(price_data)
    if len(close_prices) < window + 1:
        raise ValueError(f"Need at least {window + 1} prices to compute RSI-{window}.")

    delta = close_prices.diff()
    gains = np.where(delta > 0, delta, 0)
    losses = np.where(delta < 0, -delta, 0)
    avg_gain = float(pd.Series(gains).rolling(window=window).mean().iloc[-1])
    avg_loss = float(pd.Series(losses).rolling(window=window).mean().iloc[-1])

    if pd.isna(avg_gain) or pd.isna(avg_loss):
        raise ValueError(f"Could not compute RSI-{window}; averages are NaN.")

    if avg_loss == 0 and avg_gain == 0:
        return 50.0
    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return float(100 - (100 / (1 + rs)))


def calculate_indicators(price_data, fast_window=10, slow_window=50, rsi_window=14):
    sma_fast = compute_sma(price_data, fast_window)
    sma_slow = compute_sma(price_data, slow_window)
    rsi_value = compute_rsi(price_data, rsi_window)
    return sma_fast, sma_slow, rsi_value


def decide_action(previous_price, live_price, sma_10, sma_50, rsi_14, position):
    if isinstance(position, int):
        position_size = max(position, 0)

        if position_size > 0 and previous_price >= sma_10 and live_price < sma_10:
            return "SELL", 0

        if previous_price <= sma_50 and live_price > sma_50 and rsi_14 < 70:
            return "BUY", position_size + 1

        return None, position_size

    if position == "FLAT":
        if previous_price <= sma_50 and live_price > sma_50 and rsi_14 < 70:
            return "BUY", "LONG"

    elif position == "LONG":
        if previous_price >= sma_10 and live_price < sma_10:
            return "SELL", "FLAT"

    return None, position


def warmup_close_history(symbol, min_points):
    import yfinance as yf

    data = yf.download(symbol, period="7d", interval="1m", progress=False)

    if data.empty:
        raise ValueError(f"Could not fetch warm-up bar data for {symbol}.")

    close_prices = _normalize_prices(data)
    if len(close_prices) < min_points:
        raise ValueError(
            f"Need at least {min_points} warm-up bars for {symbol}, got {len(close_prices)}."
        )

    return [float(price) for price in close_prices.tail(min_points).tolist()]

def main():
    import zmq

    parser = argparse.ArgumentParser(description="Worker Node for Trading Bot")
    parser.add_argument("symbol", type=str)
    args = parser.parse_args()
    ASSIGNED_SYMBOL = args.symbol.upper()

    print(f"Initializing Decision Node for {ASSIGNED_SYMBOL}...")

    minimum_required_bars = max(SLOW_WINDOW, RSI_WINDOW + 1)

    try:
        close_history = warmup_close_history(ASSIGNED_SYMBOL, WARMUP_BARS)
        rolling_closes = deque(close_history, maxlen=WARMUP_BARS)
        baseline_sma10, baseline_sma50, baseline_rsi = calculate_indicators(
            rolling_closes,
            fast_window=FAST_WINDOW,
            slow_window=SLOW_WINDOW,
            rsi_window=RSI_WINDOW,
        )
        print(f"[{ASSIGNED_SYMBOL}] BASELINE SET:")
        print(f"  Warm-up bars: {len(rolling_closes)} (1-minute bars)")
        print(f"  SMA-{FAST_WINDOW}: ${baseline_sma10:.2f}")
        print(f"  SMA-{SLOW_WINDOW}: ${baseline_sma50:.2f}")
        print(f"  RSI-{RSI_WINDOW}: {baseline_rsi:.2f}")
    except Exception as e:
        print(f"Warm-up failed: {e}")
        return

    context = zmq.Context()
    
    subscriber = context.socket(zmq.SUB)
    subscriber.connect(f"tcp://{INGESTION_NODE_IP}:5555")
    subscriber.setsockopt_string(zmq.SUBSCRIBE, ASSIGNED_SYMBOL) 

    pusher = context.socket(zmq.PUSH)
    pusher.connect(f"tcp://{EXECUTION_NODE_IP}:5556")

    position_size = 0
    previous_price = float(rolling_closes[-1])
    print(f"[{ASSIGNED_SYMBOL}] Listening for live market ticks...")

    try:
        while True:
            message = subscriber.recv_string()
            
            try:
                topic, payload_str = message.split(" ", 1)
                if topic != ASSIGNED_SYMBOL:
                    continue

                payload = json.loads(payload_str)
                if payload.get("event") != "trade":
                    continue

                live_price = float(payload["price"])
                tick_timestamp = str(payload.get("timestamp", "unknown"))
            except (ValueError, TypeError, KeyError, json.JSONDecodeError):
                continue 

            rolling_closes.append(live_price)
            if len(rolling_closes) < minimum_required_bars:
                continue

            try:
                sma_10, sma_50, rsi_14 = calculate_indicators(
                    rolling_closes,
                    fast_window=FAST_WINDOW,
                    slow_window=SLOW_WINDOW,
                    rsi_window=RSI_WINDOW,
                )
            except ValueError:
                continue
            
            current_size = position_size
            action, next_position = decide_action(
                previous_price=previous_price,
                live_price=live_price,
                sma_10=sma_10,
                sma_50=sma_50,
                rsi_14=rsi_14,
                position=current_size,
            )
            position_size = next_position

            print(
                f"[TICK] {ASSIGNED_SYMBOL} {tick_timestamp} price=${live_price:.2f} "
                f"SMA{FAST_WINDOW}=${sma_10:.2f} SMA{SLOW_WINDOW}=${sma_50:.2f} "
                f"RSI{RSI_WINDOW}={rsi_14:.2f} shares={position_size}"
            )
            
            if action:
                signal_qty = 1 if action == "BUY" else max(current_size, 1)
                signal = {
                    "symbol": ASSIGNED_SYMBOL, 
                    "action": action, 
                    "qty": signal_qty,
                    "trigger_price": live_price,
                    "timestamp": tick_timestamp,
                    "sma_10": round(sma_10, 4),
                    "sma_50": round(sma_50, 4),
                    "rsi_14": round(rsi_14, 4),
                }
                pusher.send_string(json.dumps(signal))
                print(
                    f"[DECISION] {ASSIGNED_SYMBOL} @ ${live_price:.2f} | "
                    f"Action: {action} qty={signal_qty} shares={position_size} | ts={tick_timestamp}"
                )

            previous_price = live_price

    except KeyboardInterrupt:
        print(f"\nShutting down {ASSIGNED_SYMBOL} Decision Node...")
    finally:
        subscriber.close()
        pusher.close()
        context.term()

if __name__ == "__main__":
    main()