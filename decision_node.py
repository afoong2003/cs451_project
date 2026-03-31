import zmq
import json
import argparse

INGESTION_NODE_IP = "192.168.1.1" 
EXECUTION_NODE_IP = "192.168.1.3" 

def main():
    parser = argparse.ArgumentParser(description="Worker Node for Trading Bot")
    parser.add_argument("symbol", type=str)
    args = parser.parse_args()
    ASSIGNED_SYMBOL = args.symbol.upper()

    print(f"Decision Node for {ASSIGNED_SYMBOL}...")

    context = zmq.Context()
    
    subscriber = context.socket(zmq.SUB)
    subscriber.connect(f"tcp://{INGESTION_NODE_IP}:5555")
    subscriber.setsockopt_string(zmq.SUBSCRIBE, ASSIGNED_SYMBOL) 

    pusher = context.socket(zmq.PUSH)
    pusher.connect(f"tcp://{EXECUTION_NODE_IP}:5556")

    position = "FLAT" 

    print(f"Connected, trades on every tick for {ASSIGNED_SYMBOL}")

    try:
        while True:
            
            message = subscriber.recv_string()
            
            try:
                topic, payload_str = message.split(" ", 1)
            except ValueError:
                continue 
            if position == "FLAT":
                action = "BUY"
                position = "LONG"
            else:
                action = "SELL"
                position = "FLAT"
            
            signal = {
                "symbol": ASSIGNED_SYMBOL, 
                "action": action, 
                "qty": 1
            }
            
            # 4. Push to Execution Node
            pusher.send_string(json.dumps(signal))

            # 5. Visual Confirmation
            print(f"[DECISION] {ASSIGNED_SYMBOL} tick received, Action: {action}")

    except KeyboardInterrupt:
        print(f"\nShutting down {ASSIGNED_SYMBOL} Decision Node...")
    finally:
        subscriber.close()
        pusher.close()
        context.term()

if __name__ == "__main__":
    main()
