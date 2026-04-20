import json
import zmq

ZMQ_PULL_PORT = 5556
QTY_DECIMALS = 8


def main():
    print(f"Starting execution:")
    context = zmq.Context()
    receiver = context.socket(zmq.PULL)
    receiver.bind(f"tcp://*:{ZMQ_PULL_PORT}")

    positions = {}

    print("Waiting for decisions")

    try:
        while True:
            message = receiver.recv_string()

            try:
                signal = json.loads(message)
                symbol = str(signal.get("symbol", "")).upper()
                action = str(signal.get("action", "")).upper()
                qty = float(signal.get("qty", 0.0))
                trigger_price = float(signal.get("trigger_price", 0.0))
            except (json.JSONDecodeError, ValueError, TypeError):
                print(f"ERROR: Invalid signal payload received: {message}")
                continue

            if not symbol or action not in {"BUY", "SELL"} or qty <= 0:
                print(f"ERROR: Invalid trading signal fields: {signal}")
                continue

            current_qty = float(positions.get(symbol, 0.0))

            if action == "BUY":
                new_qty = current_qty + qty
                positions[symbol] = new_qty
                print(
                    f"BOUGHT ({symbol}) qty={qty:.{QTY_DECIMALS}f} "
                    f"@ ${trigger_price:.6f} | position={new_qty:.{QTY_DECIMALS}f}"
                )

            elif action == "SELL":
                if current_qty <= 0:
                    print(f"SKIP: No open crypto position for {symbol}; SELL ignored.")
                    continue

                sell_qty = min(qty, current_qty)
                remaining_qty = max(current_qty - sell_qty, 0.0)
                if remaining_qty > 0:
                    positions[symbol] = remaining_qty
                else:
                    positions.pop(symbol, None)

                print(
                    f"SOLD ({symbol}) qty={sell_qty:.{QTY_DECIMALS}f} "
                    f"@ ${trigger_price:.6f} | position={remaining_qty:.{QTY_DECIMALS}f}"
                )

    except KeyboardInterrupt:
        print("\nShutting down decision logger node...")
    finally:
        receiver.close()
        context.term()


if __name__ == "__main__":
    main()
