#!/usr/bin/env python3
"""Demo script showing BrokerManager functionality."""

from __future__ import annotations

import os
import sys
import time

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from code.broker_manager import BrokerManager
from code.proxy import Proxy
from code.client import Client


def reset_dir(path: str) -> None:
    if os.path.isdir(path):
        import shutil
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)


def main() -> None:
    # Clean up data directory
    base = os.path.join(os.path.dirname(__file__), "..", "data")
    reset_dir(base)

    print("🚀 BrokerManager Demo")
    print("=" * 50)

    # Create broker manager
    manager = BrokerManager()

    try:
        # Spawn a group of brokers
        print("\n1. Spawning broker group...")
        broker_names = manager.spawn_group("test_group", [
            ("127.0.0.1", 8001, True),   # leader
            ("127.0.0.1", 8002, False),  # follower
            ("127.0.0.1", 8003, False),  # follower
        ])

        # List brokers
        print("\n2. Listing brokers...")
        manager.list_brokers()

        # Start proxy and connect brokers
        print("\n3. Starting proxy and connecting brokers...")
        proxy = Proxy()
        proxy.start_http_server("127.0.0.1", 8000)
        
        # Connect brokers to proxy
        broker_configs = [(broker.ip, broker.port, broker.is_leader) 
                         for name, broker in manager._brokers.items()]
        proxy.setup_replication("test_group", broker_configs)
        proxy.start_cluster_tasks("test_group", "http://127.0.0.1:8000")

        # Create client and test
        print("\n4. Testing with client...")
        client = Client("test_client", ("127.0.0.1", 8000))
        
        queue_id = client.create_queue()
        print(f"Created queue: {queue_id}")
        
        message_id1 = client.append(queue_id, 100)
        message_id2 = client.append(queue_id, 200)
        print(f"Appended messages: {message_id1}, {message_id2}")
        
        message = client.read(queue_id)
        print(f"Read message: {message}")

        # Show detailed broker status
        print("\n5. Detailed broker status...")
        for name in broker_names:
            manager.get_broker_details(name)

        # Simulate crash and recovery
        print("\n6. Simulating broker crash...")
        manager.simulate_crash("broker-2")
        manager.list_brokers()

        print("\n7. Spawning replacement broker...")
        manager.spawn_broker("127.0.0.1", 8004, False, "test_group")
        manager.setup_replication("test_group")
        manager.list_brokers()

        # Test system still works
        print("\n8. Testing system after crash recovery...")
        message = client.read(queue_id)
        print(f"Read another message: {message}")

        print("\n✅ Demo completed successfully!")

    except Exception as e:
        print(f"❌ Error during demo: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # Cleanup
        print("\n🧹 Cleaning up...")
        if 'proxy' in locals():
            proxy.close()
        manager.stop_all_brokers()


if __name__ == "__main__":
    main()
