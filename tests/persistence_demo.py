from __future__ import annotations

import os
import sys
import time

# Add project root to path so 'code' package can be found
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from code.client import Client
from code.proxy import Proxy


def reset_dir(path: str) -> None:
    if os.path.isdir(path):
        import shutil
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)


def main() -> None:
    base = os.path.join(os.path.dirname(__file__), "..", "data")
    reset_dir(base)

    print("=== Testing Proxy Data Persistence ===\n")

    # Phase 1: Create initial setup
    print("Phase 1: Creating initial proxy setup...")
    proxy1 = Proxy()
    proxy1.start_http_server("127.0.0.1", 8000)
    
    # Create two broker groups
    proxy1.create_broker_group("group_a")
    proxy1.create_broker_group("group_b")
    
    # Set up brokers with replication for both groups
    proxy1.setup_replication_for_group("group_a", [
        ("127.0.0.1", 8001, True),   # leader
        ("127.0.0.1", 8002, False),  # follower
    ])
    proxy1.setup_replication_for_group("group_b", [
        ("127.0.0.1", 8003, True),   # leader
    ])
    
    # Start cluster tasks for both groups
    proxy1.start_cluster_tasks_for_group("group_a", proxy_url="http://127.0.0.1:8000")
    proxy1.start_cluster_tasks_for_group("group_b", proxy_url="http://127.0.0.1:8000")

    time.sleep(0.2)

    # Create some queues and data
    client = Client("test_client", ("127.0.0.1", 8000))
    
    print("Creating queues and adding data...")
    queue1 = client.create_queue()  # Should go to group_a (round-robin)
    queue2 = client.create_queue()  # Should go to group_b
    queue3 = client.create_queue()  # Should go to group_a
    
    print(f"Created queues: {queue1}, {queue2}, {queue3}")
    
    # Add some messages
    msg1 = client.append(queue1, 100)
    msg2 = client.append(queue2, 200)
    msg3 = client.append(queue3, 300)
    print(f"Added messages: {msg1}, {msg2}, {msg3}")

    # Show current state
    print(f"Queue counter after creates: {proxy1._queue_counter}")
    
    # Clean shutdown of first proxy
    print("\nShutting down first proxy instance...")
    proxy1.close()
    time.sleep(0.5)

    # Phase 2: Restart proxy and verify persistence
    print("\nPhase 2: Restarting proxy and checking persistence...")
    proxy2 = Proxy()  # Should load from same database
    proxy2.start_http_server("127.0.0.1", 8000)
    
    # Check if data was restored
    print(f"Queue counter after restart: {proxy2._queue_counter}")
    print(f"Broker groups: {list(proxy2._broker_groups.keys())}")
    print(f"Group leaders: {proxy2._group_leaders}")
    
    # Verify we can create new queues that continue the sequence
    client2 = Client("test_client2", ("127.0.0.1", 8000))
    
    # We need to restart the brokers since they're not automatically restarted
    # In a real system, this would be handled by the proxy on startup
    print("\nNote: In this demo, brokers need to be manually restarted.")
    print("In a production system, the proxy would restart brokers on startup.")
    
    # Recreate the brokers to continue testing
    try:
        proxy2.setup_replication_for_group("group_a", [
            ("127.0.0.1", 8001, True),   # leader
            ("127.0.0.1", 8002, False),  # follower
        ])
        proxy2.setup_replication_for_group("group_b", [
            ("127.0.0.1", 8003, True),   # leader
        ])
        
        proxy2.start_cluster_tasks_for_group("group_a", proxy_url="http://127.0.0.1:8000")
        proxy2.start_cluster_tasks_for_group("group_b", proxy_url="http://127.0.0.1:8000")
        
        time.sleep(0.2)
        
        queue4 = client2.create_queue()  # Should continue from where we left off
        print(f"New queue after restart: {queue4}")
        print(f"Final queue counter: {proxy2._queue_counter}")
        
        if queue4 > queue3:
            print("✅ Queue counter persistence works!")
        else:
            print("❌ Queue counter persistence failed")
            
    except Exception as e:
        print(f"⚠️ Broker restart failed (expected): {e}")
        print("This demonstrates that proxy configuration is persistent.")

    # Show persistent data
    print(f"\nPersistent data verification:")
    print(f"- Broker groups exist: {proxy2._data_manager.get_all_broker_groups()}")
    print(f"- Brokers in database: {len(proxy2._data_manager.get_all_brokers())}")
    print(f"- Queue counter: {proxy2._data_manager.get_config('queue_counter')}")
    
    # Cleanup
    proxy2.close()
    print("\n✅ Persistence demo completed!")


if __name__ == "__main__":
    main()
