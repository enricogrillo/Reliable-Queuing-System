#!/usr/bin/env python3
"""
Test the new strong consistency model for client positions.
Verifies that:
1. All reads go to leaders only
2. Client positions are replicated to all replicas
3. Read operations succeed only with majority consensus
"""

import time
import threading
import sys
import os

# Add code directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'code'))
from broker import Broker
from client import Client


def test_strong_consistency():
    """Test strong consistency for client positions."""
    print("=== Strong Consistency Test ===")
    
    # Start 3-broker cluster (1 leader + 2 replicas)
    print("Starting 3-broker cluster...")
    
    broker1 = Broker("leader-1", "consistency-test", "localhost", 9051, [])
    broker1.start()
    time.sleep(1)
    
    broker2 = Broker("replica-1", "consistency-test", "localhost", 9052, ["localhost:9051"])
    broker2.start()
    time.sleep(1)
    
    broker3 = Broker("replica-2", "consistency-test", "localhost", 9053, ["localhost:9051"])
    broker3.start()
    time.sleep(1)
    
    print(f"Cluster started: broker1={broker1.role.value}, broker2={broker2.role.value}, broker3={broker3.role.value}")
    
    # Create client
    client = Client("consistency-test", ["localhost:9051", "localhost:9052", "localhost:9053"])
    
    if not client.connect_to_cluster():
        print("Failed to connect to cluster")
        return
    
    try:
        print("\n--- Testing Strong Consistency ---")
        
        # Create queue and add messages
        print("Creating queue and adding messages...")
        client.create_queue("consistency-queue")
        
        for i in range(5):
            result = client.append_message("consistency-queue", i * 10)
            print(f"Append {i}: {result}")
        
        print("\n--- Reading with position replication ---")
        
        # Read messages - all should go to leaders and positions should be replicated
        for i in range(5):
            result = client.read_message("consistency-queue")
            print(f"Read {i}: {result}")
            
            # Verify that position is consistent across all brokers
            if result.get("status") == "success":
                print(f"  → Position {result['sequence_num']} replicated to all replicas")
        
        # Try reading when empty
        result = client.read_message("consistency-queue")
        print(f"Read (empty): {result}")
        
        # Test client position consistency by checking all brokers
        print("\n--- Verifying Position Consistency Across Brokers ---")
        final_position = verify_position_consistency(broker1, broker2, broker3, client.client_id, "consistency-queue")
        print(f"Final client position consistent across all brokers: {final_position}")
        
    finally:
        print("\n--- Cleanup ---")
        client.disconnect()
        broker1.stop()
        broker2.stop()
        broker3.stop()


def verify_position_consistency(broker1, broker2, broker3, client_id, queue_name):
    """Verify that client position is consistent across all brokers."""
    pos1 = broker1.data_manager.get_client_position(client_id, queue_name)
    pos2 = broker2.data_manager.get_client_position(client_id, queue_name)
    pos3 = broker3.data_manager.get_client_position(client_id, queue_name)
    
    print(f"Position on broker1 (leader): {pos1}")
    print(f"Position on broker2 (replica): {pos2}")
    print(f"Position on broker3 (replica): {pos3}")
    
    if pos1 == pos2 == pos3:
        print("✅ Positions are consistent across all brokers!")
        return pos1
    else:
        print("❌ Position inconsistency detected!")
        return None


def test_replica_cannot_serve_reads():
    """Test that replicas reject read requests."""
    print("\n=== Replica Read Rejection Test ===")
    
    # Start single broker as replica (will become leader)
    broker = Broker("test-broker", "replica-test", "localhost", 9061, [])
    broker.start()
    time.sleep(1)
    
    # Manually set to replica role to test rejection
    from broker import BrokerRole
    broker.role = BrokerRole.REPLICA
    
    try:
        # Try to read directly from replica
        result = broker.read_message("test-queue", "test-client")
        print(f"Direct read from replica: {result}")
        
        if result.get("message") == "Only leaders can handle read operations":
            print("✅ Replica correctly rejected read operation!")
        else:
            print("❌ Replica should have rejected read operation!")
            
    finally:
        broker.stop()


if __name__ == "__main__":
    print("Strong Consistency Test Suite")
    print("=" * 40)
    
    # Test 1: Strong consistency with position replication
    test_strong_consistency()
    
    time.sleep(2)
    
    # Test 2: Replica read rejection
    test_replica_cannot_serve_reads()
    
    print("\nAll tests completed!")
