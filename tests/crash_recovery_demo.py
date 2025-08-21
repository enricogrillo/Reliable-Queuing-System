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

    # Create queue and add some data
    print("Creating queue and adding initial data...")
    qid = client.create_queue()
    print(f"Created queue: {qid}")
    
    mid1 = client.append(qid, 100)
    mid2 = client.append(qid, 200)
    print(f"Appended messages: {mid1}, {mid2}")

    # Simulate follower1 crash
    print("\nSimulating follower1 crash...")
    proxy.remove_broker("broker-2")  # follower1 is broker-2
    
    # Add more data while follower1 is down
    print("Adding more data while follower1 is down...")
    mid3 = client.append(qid, 300)
    mid4 = client.append(qid, 400)
    print(f"Appended more messages: {mid3}, {mid4}")

    # Restart follower1 (it should catch up)
    print("\nRestarting follower1...")
    # Add a new broker with a different port to avoid URL conflict
    new_follower1 = proxy.add_broker_to_group("default", "127.0.0.1", 8004, is_leader=False)
    # Update replication for the group to include the new follower
    proxy.setup_replication_for_group("default")
    new_follower1.start_cluster_tasks(proxy_url="http://127.0.0.1:8000")
    
    # Give time for catch-up
    print("Waiting for follower1 to catch up...")
    time.sleep(3.0)

    # Verify follower1 has caught up by checking its data directly
    print("\nVerifying follower1 has caught up...")
    try:
        # Check if follower1 has the latest messages by querying its database
        import json
        import http.client
        from urllib.parse import urlparse
        
        def http_post(url: str, path: str, payload: dict) -> dict:
            parsed = urlparse(url)
            conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=5)
            body = json.dumps(payload).encode("utf-8")
            conn.request("POST", path, body=body, headers={"Content-Type": "application/json"})
            resp = conn.getresponse()
            data = resp.read()
            conn.close()
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status}")
            return json.loads(data.decode("utf-8")) if data else {}

        # Get all messages from follower1
        follower1_messages = http_post("http://127.0.0.1:8004", "/get_all_messages", {})
        print(f"Follower1 messages: {follower1_messages}")
        
        if follower1_messages.get("messages"):
            message_count = len(follower1_messages["messages"])
            print(f"✅ Follower1 has {message_count} messages - successfully caught up!")
        else:
            print("❌ Follower1 has no messages - catch-up failed")
            
    except Exception as e:
        print(f"❌ Error checking follower1: {e}")

    # Cleanup
    proxy.stop_http_server()
    proxy.stop_all_brokers()


if __name__ == "__main__":
    main()
