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

    # Create proxy and broker groups
    print("Setting up proxy with broker groups...")
    proxy = Proxy()
    proxy.start_http_server("127.0.0.1", 8000)
    
    # Create broker groups and spawn brokers with replication setup
    proxy.create_broker_group("group_a")
    proxy.create_broker_group("group_b")
    
    # Set up Group A with replication
    print("\nSetting up Group A...")
    proxy.setup_replication_for_group("group_a", [
        ("127.0.0.1", 8001, True),   # leader
        ("127.0.0.1", 8002, False),  # follower
    ])
    
    # Set up Group B with replication
    print("\nSetting up Group B...")
    proxy.setup_replication_for_group("group_b", [
        ("127.0.0.1", 8003, True),   # leader
        ("127.0.0.1", 8004, False),  # follower
    ])
    
    # Start cluster tasks for both groups
    proxy.start_cluster_tasks_for_group("group_a", proxy_url="http://127.0.0.1:8000")
    proxy.start_cluster_tasks_for_group("group_b", proxy_url="http://127.0.0.1:8000")

    time.sleep(0.2)

    client = Client("alice", ("127.0.0.1", 8000))

    # Create multiple queues to see distribution
    print("\nCreating multiple queues...")
    queues = []
    for i in range(5):
        qid = client.create_queue()
        queues.append(qid)
        print(f"Created queue {i+1}: {qid}")

    # Append to each queue
    print("\nAppending messages to queues...")
    for i, qid in enumerate(queues):
        mid = client.append(qid, 100 + i)
        print(f"Appended {100 + i} to queue {qid} (message_id: {mid})")

    # Read from each queue
    print("\nReading messages from queues...")
    for i, qid in enumerate(queues):
        msg = client.read(qid)
        print(f"Read from queue {qid}: {msg}")

    # Cleanup
    proxy.stop_http_server()
    proxy.stop_all_brokers()


if __name__ == "__main__":
    main()
