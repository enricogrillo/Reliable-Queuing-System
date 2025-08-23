#!/usr/bin/env python3
"""
Simple script to start three brokers in a cluster.
"""

import time
import sys
import os
from threading import Thread

# Add code directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'code'))
from broker import Broker


def start_broker_thread(broker_id: str, port: int, seed_brokers: list = None):
    """Start a broker instance in a separate thread."""
    def run_broker():
        broker = Broker(
            broker_id=broker_id,
            cluster_id="G-START",
            listen_host="localhost",
            listen_port=port,
            seed_brokers=seed_brokers or [],
            db_path=f"data/broker_{broker_id}.db"
        )
        
        print(f"[{broker_id}] Starting on localhost:{port}")
        broker.start()
    
    thread = Thread(target=run_broker, daemon=True)
    thread.start()
    return thread


def main():
    """Start a 3-broker cluster."""
    print("=== Starting 3-Broker Cluster ===")
    print("Cluster ID: G-START")
    print("Ports: 9001, 9002, 9003")
    print()
    
    # Start first broker (will become initial leader)
    print("Starting B-LEAD1 (initial leader)...")
    broker1_thread = start_broker_thread("B-LEAD1", 9001)
    time.sleep(2)
    
    # Start second broker (will join as replica)
    print("Starting B-REPL1 (replica)...")
    broker2_thread = start_broker_thread("B-REPL1", 9002, ["localhost:9001"])
    time.sleep(2)
    
    # Start third broker (will join as replica)
    print("Starting B-REPL2 (replica)...")
    broker3_thread = start_broker_thread("B-REPL2", 9003, ["localhost:9001"])
    time.sleep(2)
    
    print()
    print("✓ All brokers started successfully!")
    print("Cluster topology:")
    print("  Leader:  B-LEAD1 (localhost:9001)")
    print("  Replica: B-REPL1 (localhost:9002)")
    print("  Replica: B-REPL2 (localhost:9003)")
    print()
    print("You can now use the client CLI:")
    print("  python3 client_cli.py -b localhost:9001,localhost:9002,localhost:9003")
    print()
    print("Press Ctrl+C to stop all brokers.")
    
    try:
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\nShutting down brokers...")
        print("Goodbye!")


if __name__ == '__main__':
    main()
