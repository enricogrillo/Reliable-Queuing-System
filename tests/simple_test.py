#!/usr/bin/env python3
"""
Simple test with single broker to verify position tracking works correctly.
"""

import time
import sys
import os

# Add code directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from code.broker import Broker
from code.client import Client


def test_single_broker():
    """Test with single broker to verify position tracking."""
    print("=== Single Broker Position Tracking Test ===")
    
    # Start single broker
    broker = Broker(
        broker_id="test-broker",
        cluster_id="test-cluster",
        listen_host="localhost",
        listen_port=9030,
        seed_brokers=[]
    )
    broker.start()
    time.sleep(1)
    
    # Create client that will only talk to one broker
    client = Client(
        cluster_id="test-cluster",
        seed_brokers=["localhost:9030"]
    )
    
    if not client.connect_to_cluster():
        print("Failed to connect")
        return
    
    try:
        # Create queue and add messages
        client.create_queue("position-test")
        for i in range(5):
            result = client.append_message("position-test", i * 100)
            print(f"Append {i}: {result}")
        
        print("\n--- Reading with position tracking ---")
        # Read messages - should advance through queue
        for i in range(5):
            result = client.read_message("position-test")
            print(f"Read {i}: {result}")
        
        # Try reading when empty
        result = client.read_message("position-test")
        print(f"Read (empty): {result}")
        
    finally:
        client.disconnect()
        broker.stop()


if __name__ == "__main__":
    test_single_broker()
