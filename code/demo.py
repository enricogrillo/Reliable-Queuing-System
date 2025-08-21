from __future__ import annotations

import os
import shutil

from broker import Broker
from client import Client
from proxy import Proxy
import time


def reset_dir(path: str) -> None:
    if os.path.isdir(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)


def main() -> None:
    base = os.path.join(os.path.dirname(__file__), "data")
    reset_dir(base)

    # Create brokers
    leader = Broker(1, "127.0.0.1", 8001, os.path.join(base, "leader.db"), is_leader=True, name="leader")
    follower1 = Broker(2, "127.0.0.1", 8002, os.path.join(base, "f1.db"), name="follower1")
    follower2 = Broker(3, "127.0.0.1", 8003, os.path.join(base, "f2.db"), name="follower2")

    # Start HTTP servers
    leader.start_http_server()
    follower1.start_http_server()
    follower2.start_http_server()

    # Wire followers for network replication
    leader.set_follower_urls(["http://127.0.0.1:8002", "http://127.0.0.1:8003"])

    # Proxy and client
    proxy = Proxy(leader, leader_url="http://127.0.0.1:8001")
    proxy.start_http_server("127.0.0.1", 8000)
    client = Client("alice", proxy, proxy_url="http://127.0.0.1:8000")

    time.sleep(0.2)

    # Create queue
    qid = client.create_queue()
    print(f"Created queue: {qid}")

    # Append messages
    mid1 = client.append(qid, "hello")
    mid2 = client.append(qid, "world")
    print(f"Appended messages: {mid1}, {mid2}")

    # Read (peek) next message
    # Read for different clients advances independent pointers
    next_msg = client.read(qid)
    print(f"Next message on queue {qid}: {next_msg}")

    # Verify pointer advancement and isolation across clients
    next_msg_same_client = client.read(qid)
    print(f"Alice next message on queue {qid}: {next_msg_same_client}")
    bob = Client("bob", proxy, proxy_url="http://127.0.0.1:8000")
    next_msg_other_client = bob.read(qid)
    print(f"Bob first message on queue {qid}: {next_msg_other_client}")

    # Cleanup
    proxy.stop_http_server()
    leader.stop_http_server()
    follower1.stop_http_server()
    follower2.stop_http_server()
    leader.close()
    follower1.close()
    follower2.close()


if __name__ == "__main__":
    main()


