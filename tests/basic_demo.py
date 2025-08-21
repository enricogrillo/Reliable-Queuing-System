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

    # Create queue
    qid = client.create_queue()
    print(f"Created queue: {qid}")

    # Append messages
    mid1 = client.append(qid, 100)
    mid2 = client.append(qid, 200)
    print(f"Appended messages: {mid1}, {mid2}")

    # Read (peek) next message
    next_msg = client.read(qid)
    print(f"Next message on queue {qid}: {next_msg}")

    # Verify pointer advancement and isolation across clients
    next_msg_same_client = client.read(qid)
    print(f"Alice next message on queue {qid}: {next_msg_same_client}")
    
    bob = Client("bob", ("127.0.0.1", 8000))
    next_msg_other_client = bob.read(qid)
    print(f"Bob first message on queue {qid}: {next_msg_other_client}")

    # Cleanup
    proxy.stop_http_server()
    proxy.stop_all_brokers()


if __name__ == "__main__":
    main()
