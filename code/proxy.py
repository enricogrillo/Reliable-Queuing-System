from __future__ import annotations

import threading
from typing import Optional, Tuple, List
import json
from urllib.parse import urlparse
import http.client

from .broker import Broker
from .proxy_data_manager import ProxyDataManager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from .http_utils import HttpJsonClient, HttpJsonServer


class Proxy:
    """Proxy forwards client calls and owns broker creation/runtime with load balancing across groups."""

    def __init__(self, db_path: str = "data/proxy.db") -> None:
        self._lock = threading.RLock()
        self._data_manager = ProxyDataManager(db_path)
        self._brokers: dict[str, Broker] = {}
        self._queue_counter = 0
        
        # Load existing data from database
        self._load_from_database()

    def _load_from_database(self) -> None:
        """Load existing proxy data from database."""
        with self._lock:
            # Get queue counter from config
            queue_counter = self._data_manager.get_config("queue_counter", "0")
            self._queue_counter = int(queue_counter)

    @property
    def _broker_groups(self) -> dict[str, list[str]]:
        """Get broker groups from database."""
        return self._data_manager.get_all_brokers()

    @property
    def _broker_urls(self) -> dict[str, str]:
        """Get broker URLs from database."""
        return self._data_manager.get_broker_urls()

    @property
    def _group_leaders(self) -> dict[str, str]:
        """Get group leaders from database."""
        return self._data_manager.get_all_group_leaders()

    def create_broker_group(self, group_name: str) -> None:
        """Create a new broker group."""
        with self._lock:
            if self._data_manager.broker_group_exists(group_name):
                raise ValueError(f"Broker group '{group_name}' already exists")
            self._data_manager.create_broker_group(group_name)
            print(f"Created broker group: {group_name}")

    def add_broker_to_group(self, group_name: str, address: str, port: int, is_leader: bool = False) -> Broker:
        """Create and add a broker to a specific group."""
        with self._lock:
            if not self._data_manager.broker_group_exists(group_name):
                raise ValueError(f"Broker group '{group_name}' does not exist")
            
            url = f"http://{address}:{port}"
            if url in self._broker_urls.values():
                raise ValueError("Broker address already registered")
            
            broker_id = self._data_manager.get_next_broker_id()
            
            broker = Broker(broker_id, address, port, is_leader)
            broker.start_http_server()
            name = broker.name
            self._brokers[name] = broker
            
            # Save broker to database
            self._data_manager.add_broker(name, broker_id, group_name, address, port, is_leader, url)
            
            if is_leader:
                self._data_manager.set_group_leader(group_name, name)
                print(f"Added leader '{name}' to group '{group_name}'")
            else:
                print(f"Added follower '{name}' to group '{group_name}'")
            
            return broker

    def add_broker(self, address: str, port: int, is_leader: bool = False) -> Broker:
        """Legacy method - adds broker to default group."""
        # Ensure default group exists
        if not self._data_manager.broker_group_exists("default"):
            self.create_broker_group("default")
        return self.add_broker_to_group("default", address, port, is_leader)

    def setup_replication_for_group(self, group_name: str, broker_config: Optional[List[Tuple[str, int, bool]]] = None) -> None:
        """
        Set up replication relationships within a broker group.
        
        Args:
            group_name: Name of the broker group
            broker_config: List of (address, port, is_leader) tuples for brokers to spawn.
                         If None, uses existing brokers in the group.
        """
        with self._lock:
            if not self._data_manager.broker_group_exists(group_name):
                raise ValueError(f"Broker group '{group_name}' does not exist")
            
            # If broker_config is provided, spawn brokers first
            if broker_config:
                for address, port, is_leader in broker_config:
                    self.add_broker_to_group(group_name, address, port, is_leader)
            
            broker_names = self._broker_groups.get(group_name, [])
            if not broker_names:
                return
            
            # Get leader and followers
            leader_name = self._group_leaders.get(group_name)
            if not leader_name:
                return
            
            leader_broker = self._brokers[leader_name]
            follower_urls = []
            peer_urls = []
            
            # Collect URLs for all brokers in group
            for name in broker_names:
                url = self._broker_urls[name]
                peer_urls.append(url)
                if name != leader_name:
                    follower_urls.append(url)
            
            # Set up leader's follower URLs
            leader_broker.set_follower_urls(follower_urls)
            
            # Set peer URLs for all brokers (for election)
            for name in broker_names:
                broker = self._brokers[name]
                broker.set_peer_urls(peer_urls)
    
    def start_cluster_tasks_for_group(self, group_name: str, proxy_url: Optional[str] = None) -> None:
        """Start cluster tasks (heartbeat, monitoring, etc.) for all brokers in a group."""
        with self._lock:
            if not self._data_manager.broker_group_exists(group_name):
                raise ValueError(f"Broker group '{group_name}' does not exist")
            
            broker_names = self._broker_groups.get(group_name, [])
            for broker_name in broker_names:
                broker = self._brokers[broker_name]
                broker.start_cluster_tasks(proxy_url)
    
    def remove_broker(self, broker_name: str) -> None:
        """Remove a broker from management (for crash simulation)."""
        with self._lock:
            if broker_name not in self._brokers:
                return
            
            # Stop and clean up the broker
            broker = self._brokers[broker_name]
            broker.stop_cluster_tasks()
            broker.stop_http_server()
            broker.close()
            
            # Remove from in-memory tracking
            del self._brokers[broker_name]
            
            # Get broker info to check if it's a leader
            broker_info = self._data_manager.get_broker_info(broker_name)
            if broker_info and broker_info["is_leader"]:
                self._data_manager.clear_group_leader(broker_info["group_name"])
            
            # Remove from database
            self._data_manager.remove_broker(broker_name)

    def stop_all_brokers(self) -> None:
        """Stop and clean up all managed brokers."""
        with self._lock:
            for broker in self._brokers.values():
                broker.stop_cluster_tasks()
                broker.stop_http_server()
                broker.close()
            self._brokers.clear()
            # Note: We don't clear database data, only in-memory broker instances

    def create_queue(self) -> int:
        with self._lock:
            group_leaders = self._group_leaders
            if not group_leaders:
                raise RuntimeError("No broker groups configured. Use add_broker() or add_broker_to_group() first.")
            
            # Round-robin load balancing across groups
            group_names = list(group_leaders.keys())
            selected_group = group_names[self._queue_counter % len(group_names)]
            leader_name = group_leaders[selected_group]
            leader_url = self._broker_urls[leader_name]
            
            self._queue_counter += 1
            # Persist queue counter
            self._data_manager.set_config("queue_counter", str(self._queue_counter))
            
            print(f"Creating queue in group '{selected_group}' (leader: {leader_name})")
            
            return int(self._post_json(leader_url, "/create_queue", {})["queue_id"])

    def append(self, queue_id: int, value: int) -> int:
        with self._lock:
            group_leaders = self._group_leaders
            if not group_leaders:
                raise RuntimeError("No broker groups configured. Use add_broker() or add_broker_to_group() first.")
            
            # Find which group owns this queue by trying all group leaders
            broker_urls = self._broker_urls
            for group_name, leader_name in group_leaders.items():
                leader_url = broker_urls[leader_name]
                try:
                    result = self._post_json(
                        leader_url, "/append", {"queue_id": queue_id, "value": value}
                    )
                    print(f"Appended to queue {queue_id} in group '{group_name}'")
                    return int(result["message_id"])
                except Exception:
                    continue
            raise RuntimeError(f"Queue {queue_id} not found in any group")

    def read(self, queue_id: int, client_id: str) -> Optional[Tuple[int, int]]:
        with self._lock:
            group_leaders = self._group_leaders
            if not group_leaders:
                raise RuntimeError("No broker groups configured. Use add_broker() or add_broker_to_group() first.")
            
            # Find which group owns this queue by trying all group leaders
            broker_urls = self._broker_urls
            for group_name, leader_name in group_leaders.items():
                leader_url = broker_urls[leader_name]
                try:
                    res = self._post_json(
                        leader_url, "/read", {"queue_id": queue_id, "client_id": client_id}
                    )
                    if res.get("message_id") is None:
                        return None
                    print(f"Read from queue {queue_id} in group '{group_name}'")
                    return int(res["message_id"]), int(res["value"])
                except Exception:
                    continue
            raise RuntimeError(f"Queue {queue_id} not found in any group")

    # Networking helper
    def _post_json(self, base_url: str, path: str, payload: dict) -> dict:
        client = HttpJsonClient(timeout=5)
        return client.post_json(base_url, path, payload)

    # HTTP server
    def start_http_server(self, host: str, port: int) -> None:
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            def _read_json(self) -> dict:
                return HttpJsonServer.read_json(self)

            def _write_json(self, status: int, payload: dict) -> None:
                HttpJsonServer.write_json(self, status, payload)

            # Per-command handlers
            def handle_create_queue(self, body: dict) -> None:
                qid = proxy.create_queue()
                self._write_json(200, {"queue_id": qid})

            def handle_append(self, body: dict) -> None:
                qid = int(body.get("queue_id"))
                value = int(body.get("value"))
                mid = proxy.append(qid, value)
                self._write_json(200, {"message_id": mid})

            def handle_read(self, body: dict) -> None:
                qid = int(body.get("queue_id"))
                client_id = str(body.get("client_id"))
                res = proxy.read(qid, client_id)
                if res is None:
                    self._write_json(200, {})
                else:
                    mid, value = res
                    self._write_json(200, {"message_id": mid, "value": value})

            def handle_set_leader(self, body: dict) -> None:
                leader_url = str(body.get("leader_url"))
                # Find which broker became the new leader and update group leadership
                with proxy._lock:  # noqa: SLF001
                    broker_groups = proxy._broker_groups
                    broker_urls = proxy._broker_urls
                    for group_name, broker_names in broker_groups.items():
                        for broker_name in broker_names:
                            if broker_urls[broker_name] == leader_url:
                                proxy._data_manager.set_group_leader(group_name, broker_name)
                                print(f"Updated leader for group '{group_name}' to {broker_name}")
                                break
                self._write_json(200, {"ok": True})

            def do_POST(self) -> None:  # noqa: N802
                try:
                    body = self._read_json()
                    routes = {
                        "/create_queue": self.handle_create_queue,
                        "/append": self.handle_append,
                        "/read": self.handle_read,
                        "/set_leader": self.handle_set_leader,
                    }
                    handler = routes.get(self.path)
                    if handler is None:
                        self._write_json(404, {"error": "not found"})
                        return
                    handler(body)
                except Exception as exc:  # pylint: disable=broad-except
                    self._write_json(500, {"error": str(exc)})

            def log_message(self, fmt: str, *args) -> None:  # noqa: D401
                return

        self._httpd = ThreadingHTTPServer((host, port), Handler)
        self._server_thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._server_thread.start()

    def stop_http_server(self) -> None:
        httpd = getattr(self, "_httpd", None)
        if httpd is not None:
            httpd.shutdown()
            httpd.server_close()
        thr = getattr(self, "_server_thread", None)
        if thr is not None:
            thr.join(timeout=2)

    def close(self) -> None:
        """Close the proxy and clean up resources."""
        self.stop_http_server()
        self.stop_all_brokers()
        self._data_manager.close()