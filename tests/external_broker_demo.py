from __future__ import annotations

import os
import sys
import time
import threading

# Add project root to path so 'code' package can be found
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from code.client import Client
from code.proxy import Proxy
from code.broker_manager import BrokerManager


def reset_dir(path: str) -> None:
    if os.path.isdir(path):
        import shutil
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)


def main() -> None:
    base = os.path.join(os.path.dirname(__file__), "..", "data")
    reset_dir(base)

    # Use BrokerManager to spawn brokers
    print("Starting external brokers...")
    broker_manager = BrokerManager()
    
    # Spawn brokers and set up replication
    broker_names = broker_manager.spawn_group("external_group", [
        ("127.0.0.1", 9501, True),   # leader
        ("127.0.0.1", 9502, False),  # follower
    ])
    
    time.sleep(0.1)  # Give brokers time to start

    # Create proxy with broker URLs
    print("\nCreating proxy that connects to external brokers...")
    broker_urls = broker_manager.get_broker_urls("external_group")
    proxy = Proxy(broker_urls)
    proxy.start_http_server("127.0.0.1", 8000)

    time.sleep(0.2)

    # Test the system
    print("\nTesting external broker communication...")
    client = Client("test_user", ("127.0.0.1", 8000))

    # Create queue
    qid = client.create_queue()
    print(f"Created queue: {qid}")

    # Append messages
    mid1 = client.append(qid, 500)
    mid2 = client.append(qid, 600)
    print(f"Appended messages: {mid1}, {mid2}")

    # Read messages
    next_msg = client.read(qid)
    print(f"Next message on queue {qid}: {next_msg}")

    # Cleanup
    print("\nCleaning up...")
    proxy.close()
    broker_manager.stop_all_brokers()
    
    print("Demo completed successfully!")


if __name__ == "__main__":
    main()
