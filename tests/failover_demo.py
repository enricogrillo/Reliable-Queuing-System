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

    # Create proxy and brokers
    proxy = Proxy()
    proxy.start_http_server("127.0.0.1", 8000)
    
    # Create default group and spawn brokers with replication setup
    proxy.create_broker_group("default")
    proxy.setup_replication_for_group("default", [
        ("127.0.0.1", 8001, True),   # leader
        ("127.0.0.1", 8002, False),  # follower1
        ("127.0.0.1", 8003, False),  # follower2
    ])
    proxy.start_cluster_tasks_for_group("default", proxy_url="http://127.0.0.1:8000")

    time.sleep(0.2)

    client = Client("alice", ("127.0.0.1", 8000))

    # Create queue and append some messages
    qid = client.create_queue()
    print(f"Created queue: {qid}")
    mid1 = client.append(qid, 100)
    mid2 = client.append(qid, 200)
    print(f"Appended messages: {mid1}, {mid2}")

    # Simulate leader crash and failover
    print("Simulating leader crash...")
    proxy.remove_broker("broker-1")  # Remove the leader broker
    # Give time for heartbeat timeout and election
    time.sleep(3.0)
    # Try another append via proxy and read it
    new_mid = None
    for _ in range(5):
        try:
            new_mid = client.append(qid, 300)
            break
        except Exception:
            time.sleep(0.8)
    if new_mid is None:
        raise RuntimeError("append after failover failed")
    print(f"Appended after failover: {new_mid}")
    msg_after = client.read(qid)
    print(f"Alice reads after failover: {msg_after}")

    # Cleanup
    proxy.stop_http_server()
    proxy.stop_all_brokers()


if __name__ == "__main__":
    main()
