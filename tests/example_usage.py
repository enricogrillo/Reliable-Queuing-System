#!/usr/bin/env python3
"""
Example usage of the distributed queuing platform.
Demonstrates how to start brokers and use the client library.
"""

import time
import threading
import sys
import os

# Add code directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from code.broker import Broker
from code.client import Client


def start_broker(broker_id: str, port: int, seed_brokers: list = None):
    """Start a broker instance."""
    broker = Broker(
        broker_id=broker_id,
        cluster_id="G-EXMPL",
        listen_host="localhost",
        listen_port=port,
        seed_brokers=seed_brokers or [],
        db_path=f"broker_{broker_id}.db"
    )
    
    broker.start()
    return broker

def three_broker_cluster():
    """Example with a 3-broker cluster (1 leader, 2 replicas)."""
    print("=== Starting 3-Broker Cluster Example ===")
    
    # Start first broker (will become initial leader)
    print("Starting broker-1 (initial leader)...")
    broker1 = start_broker("broker-1", 9011)
    time.sleep(1)
    
    # Start second broker (will join as replica)
    print("Starting broker-2 (replica)...")
    broker2 = start_broker("broker-2", 9012, ["localhost:9011"])
    time.sleep(1)
    
    # Start third broker (will join as replica)
    print("Starting broker-3 (replica)...")
    broker3 = start_broker("broker-3", 9013, ["localhost:9011"])
    time.sleep(1)

    while True:
        pass

def example_three_broker_cluster():
    """Example with a 3-broker cluster (1 leader, 2 replicas)."""
    print("=== Starting 3-Broker Cluster Example ===")
    
    # Start first broker (will become initial leader)
    print("Starting broker-1 (initial leader)...")
    broker1 = start_broker("broker-1", 9011)
    time.sleep(1)
    
    # Start second broker (will join as replica)
    print("Starting broker-2 (replica)...")
    broker2 = start_broker("broker-2", 9012, ["localhost:9011"])
    time.sleep(1)
    
    # Start third broker (will join as replica)
    print("Starting broker-3 (replica)...")
    broker3 = start_broker("broker-3", 9013, ["localhost:9011"])
    time.sleep(1)
    
    # Create client
    print("Creating client...")
    client = Client(
        cluster_id="G-EXMPL",
        seed_brokers=["localhost:9011", "localhost:9012", "localhost:9013"]
    )
    
    if not client.connect_to_cluster():
        print("Failed to connect to cluster")
        return
    
    try:
        # Create a queue
        print("\n=== Queue Operations ===")
        result = client.create_queue()
        print(f"Create queue result: {result}")
        queue_id = result.get('queue_id')
        
        if queue_id:
            # Send some messages
            for i in range(5):
                result = client.append_message(queue_id, i * 10)
                print(f"Append message {i}: {result}")
            
            # Read messages
            print("\nReading messages:")
            for i in range(5):
                result = client.read_message(queue_id)
                print(f"Read message {i}: {result}")
            
            # Try to read when no more messages
            result = client.read_message(queue_id)
            print(f"Read (empty): {result}")
        else:
            print("Failed to create queue")
        
        # Show cluster info
        print(f"\nCluster info: {client.get_cluster_info()}")
        
    finally:
        # Cleanup
        print("\n=== Cleanup ===")
        client.disconnect()
        
        # Wait a moment for graceful shutdown
        time.sleep(1)
        
        broker1.stop()
        broker2.stop()
        broker3.stop()


def example_leader_failover():
    """Example demonstrating leader failover."""
    print("\n=== Leader Failover Example ===")
    
    # Start two brokers
    broker1 = start_broker("broker-1", 9021)
    time.sleep(1)
    broker2 = start_broker("broker-2", 9022, ["localhost:9021"])
    time.sleep(1)
    
    # Create client
    client = Client(
        cluster_id="G-EXMPL",
        seed_brokers=["localhost:9021", "localhost:9022"]
    )
    client.connect_to_cluster()
    
    try:
        # Create queue and send message
        result = client.create_queue()
        queue_id = result.get('queue_id')
        print(f"Created queue: {queue_id}")
        
        if queue_id:
            client.append_message(queue_id, 42)
            
            print("Initial state: broker-1 is leader")
            print(f"Cluster info: {client.get_cluster_info()}")
            
            # Stop the leader (broker-1)
            print("\nStopping leader broker-1...")
            broker1.stop()
            print("Waiting for failure detection and leader election (this takes ~18 seconds)...")
            time.sleep(18)  # Wait for election timeout (15s) + buffer
            
            # broker-2 should become new leader
            print("After failover - broker-2 should be new leader")
            
            # Client should automatically discover new topology
            time.sleep(1)
            result = client.append_message(queue_id, 99)
            print(f"Append after failover: {result}")
            
            result = client.read_message(queue_id)
            print(f"Read after failover: {result}")
            
            print(f"Final cluster info: {client.get_cluster_info()}")
        else:
            print("Failed to create queue")
        
    finally:
        client.disconnect()
        broker2.stop()


def example_client_only():
    """Example for connecting to existing cluster."""
    print("\n=== Client-Only Example ===")
    print("This example assumes brokers are already running on ports 9011-9013")
    
    client = Client(
        cluster_id="G-EXMPL",
        seed_brokers=["localhost:9011", "localhost:9012", "localhost:9013"]
    )
    
    try:
        if client.connect_to_cluster():
            print("Connected to existing cluster")
            
            # Show cluster status
            status = client.get_status()
            print(f"Client status: {status}")
            
            # Try basic operations
            result = client.create_queue()
            queue_id = result.get('queue_id')
            print(f"Created queue: {queue_id}")
            
            if queue_id:
                client.append_message(queue_id, 123)
                result = client.read_message(queue_id)
                print(f"Read result: {result}")
            else:
                print("Failed to create queue")
        else:
            print("Could not connect to cluster (make sure brokers are running)")
    
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        client.disconnect()


if __name__ == "__main__":
    print("Distributed Queuing Platform - Example Usage")
    print("=" * 50)
    
    # Run the three-broker example
    three_broker_cluster()
    
    # Wait between examples
    time.sleep(2)
    
    # Run leader failover example
    #example_leader_failover()
    
    # Uncomment to run client-only example
    # example_client_only()
    
    print("\nExample completed!")
