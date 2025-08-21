#!/usr/bin/env python3
"""
Test script for the interactive CLI.
"""

import os
import sys
import time
import subprocess

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "."))

from code.proxy import Proxy


def main():
    # Clean data
    if os.path.isdir("data"):
        import shutil
        shutil.rmtree("data")
    os.makedirs("data", exist_ok=True)
    
    # Start proxy
    proxy = Proxy()
    proxy.create_broker_group("default")
    
    # Set up brokers with replication
    proxy.setup_replication_for_group("default", [
        ("127.0.0.1", 8001, True),   # leader
        ("127.0.0.1", 8002, False),  # follower
    ])
    proxy.start_cluster_tasks_for_group("default")
    
    # Start proxy HTTP server
    proxy.start_http_server("127.0.0.1", 8000)
    print("Proxy started on http://127.0.0.1:8000")
    
    while True:
        pass
    # Wait a moment for everything to start
    # time.sleep(2)
    
    print("\n=== Testing Interactive CLI ===")
    
    # Test interactive CLI with predefined commands
    commands = [
        "create_queue",
        "append 1 100", 
        "append 1 200",
        "read 1",
        "read 1 as bob",
        "status",
        "quit"
    ]
    
    # Create input for the interactive CLI
    input_text = "\n".join(commands) + "\n"
    
    # Run the interactive CLI with the commands
    result = subprocess.run([
        "python3", "-m", "code.client_cli", 
        "--proxy", "127.0.0.1:8000", 
        "--client", "alice"
    ], input=input_text, capture_output=True, text=True)
    
    print("Interactive CLI Output:")
    print(result.stdout)
    
    if result.stderr:
        print("Errors:")
        print(result.stderr)
    
    print("\n=== Testing Single Command Mode ===")
    
    # Test single command mode
    result = subprocess.run([
        "python3", "-m", "code.client_cli", 
        "--proxy", "127.0.0.1:8000", 
        "--client", "bob",
        "create-queue"
    ], capture_output=True, text=True)
    
    print("Single command output:")
    print(result.stdout.strip())
    
    if result.stderr:
        print("Errors:")
        print(result.stderr)
    
    # Clean up
    proxy.close()
    print("\nTest completed!")


if __name__ == "__main__":
    main()
