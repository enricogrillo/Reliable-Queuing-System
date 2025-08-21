from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple

from .http_client import HttpJsonClient
from .http_server_base import HttpServerBase, JsonRequestHandler
from .broker_data_manager import SqliteManager


@dataclass
class ReplicationResult:
    """Result of a replication operation."""
    ok: bool
    error: Optional[str] = None


class Broker(HttpServerBase):
    """
    Distributed broker node with leader-follower replication.
    
    Leaders execute operations and replicate to followers.
    Followers apply replicated operations from leaders.
    """

    _used_ids: set[int] = set()
    _used_addresses: set[Tuple[str, int]] = set()

    def __init__(self, broker_id: int, ip: str, port: int, is_leader: bool = False) -> None:
        super().__init__()
        
        # Ensure uniqueness of broker ID and address
        self._ensure_unique_broker(broker_id, ip, port)
        
        # Core broker properties
        self.broker_id = broker_id
        self.ip = ip
        self.port = port
        self.name = f"broker-{broker_id}"
        
        # State management
        self._sqlite = SqliteManager(f"data/broker-{broker_id}.db")
        self._is_leader = is_leader
        self._lock = threading.RLock()
        
        # Cluster configuration
        self._follower_urls: List[str] = []
        self._peer_urls: List[str] = []
        self._proxy_url: Optional[str] = None
        
        # Heartbeat and monitoring
        self._last_heartbeat_ts: float = 0.0
        self._heartbeat_interval_sec: float = 0.5
        self._heartbeat_timeout_sec: float = 2.0
        self._hb_stop = threading.Event()
        self._monitor_stop = threading.Event()

    def _ensure_unique_broker(self, broker_id: int, ip: str, port: int) -> None:
        """Ensure broker ID and address are unique."""
        # Allow reuse if previous broker is no longer active
        Broker._used_ids.discard(broker_id)
        Broker._used_addresses.discard((ip, port))
        
        Broker._used_ids.add(broker_id)
        Broker._used_addresses.add((ip, port))

    # =============================================================================
    # Properties and Configuration
    # =============================================================================

    @property
    def is_leader(self) -> bool:
        """Check if this broker is the current leader."""
        return self._is_leader

    @property
    def self_url(self) -> str:
        """Get this broker's HTTP URL."""
        return f"http://{self.ip}:{self.port}"

    def set_follower_urls(self, follower_urls: List[str]) -> None:
        """Configure follower URLs for replication."""
        with self._lock:
            self._follower_urls = follower_urls[:]

    def set_peer_urls(self, peer_urls: List[str]) -> None:
        """Configure peer URLs for leader election."""
        with self._lock:
            self._peer_urls = [url for url in peer_urls if url != self.self_url]

    # =============================================================================
    # Cluster Management
    # =============================================================================

    def start_cluster_tasks(self, proxy_url: Optional[str] = None) -> None:
        """Start background cluster management tasks."""
        with self._lock:
            self._proxy_url = proxy_url
            self._hb_stop.clear()
            self._monitor_stop.clear()
            
        # Start background threads
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        threading.Thread(target=self._monitor_loop, daemon=True).start()
        
        if not self._is_leader:
            threading.Thread(target=self._catchup_loop, daemon=True).start()

    def stop_cluster_tasks(self) -> None:
        """Stop all cluster management tasks."""
        self._hb_stop.set()
        self._monitor_stop.set()

    def close(self) -> None:
        """Clean up and close the broker."""
        self.stop_cluster_tasks()
        self._sqlite.close()

    # =============================================================================
    # Client Operations (Leader Only)
    # =============================================================================

    def create_queue(self) -> int:
        """Create a new queue (leader only)."""
        self._ensure_leader("create_queue")
        
        with self._lock:
            queue_id = self._sqlite.create_queue()
            self._replicate("/apply_create_queue", {"queue_id": queue_id})
            return queue_id

    def append(self, queue_id: int, value: int) -> int:
        """Append a message to a queue (leader only)."""
        self._ensure_leader("append")
        
        with self._lock:
            message_id = self._sqlite.append_message(queue_id, value)
            self._replicate("/apply_append", {
                "message_id": message_id,
                "queue_id": queue_id,
                "value": value
            })
            return message_id

    def read_for_client(self, queue_id: int, client_id: str) -> Optional[Tuple[int, int]]:
        """Read next message for client and advance pointer (leader only)."""
        self._ensure_leader("read_for_client")
        
        with self._lock:
            last_read = self._sqlite.get_last_read_message_id(queue_id, client_id)
            next_msg = self._sqlite.fetch_next_after(queue_id, last_read)
            
            if next_msg is None:
                return None
            
            message_id, value = next_msg
            
            # Update pointer locally and replicate
            self._sqlite.set_last_read_message_id(queue_id, client_id, message_id)
            self._replicate("/apply_set_pointer", {
                "queue_id": queue_id,
                "client_id": client_id,
                "message_id": message_id
            })
            
            return message_id, value

    def _ensure_leader(self, operation: str) -> None:
        """Ensure this broker is the leader for the given operation."""
        if not self._is_leader:
            raise RuntimeError(f"{operation} must be invoked on the leader")

    # =============================================================================
    # Replication Operations (Apply Commands)
    # =============================================================================

    def apply_create_queue(self, queue_id: int) -> ReplicationResult:
        """Apply queue creation (used by followers)."""
        try:
            with self._lock:
                if not self._sqlite.queue_exists(queue_id):
                    self._sqlite.create_queue_with_id(queue_id)
            return ReplicationResult(ok=True)
        except Exception as exc:
            return ReplicationResult(ok=False, error=str(exc))

    def apply_append(self, message_id: int, queue_id: int, value: int) -> ReplicationResult:
        """Apply message append (used by followers)."""
        try:
            with self._lock:
                try:
                    self._sqlite.append_message_with_id(message_id, queue_id, value)
                except Exception:
                    # Idempotency: ignore if message already exists
                    pass
            return ReplicationResult(ok=True)
        except Exception as exc:
            return ReplicationResult(ok=False, error=str(exc))

    def apply_set_pointer(self, queue_id: int, client_id: str, message_id: int) -> ReplicationResult:
        """Apply pointer update (used by followers)."""
        try:
            with self._lock:
                self._sqlite.set_last_read_message_id(queue_id, client_id, message_id)
            return ReplicationResult(ok=True)
        except Exception as exc:
            return ReplicationResult(ok=False, error=str(exc))

    # =============================================================================
    # Replication and Synchronization
    # =============================================================================

    def _replicate(self, path: str, payload: dict) -> None:
        """Replicate operation to all followers."""
        failures = []
        
        for url in list(self._follower_urls):
            try:
                self._post_json(url, path, payload)
            except Exception as exc:
                failures.append(f"{url}: {exc}")
                print(f"Replication failed to {url}: {exc}")
        
        # Allow partial failures but fail if ALL followers fail
        if failures and len(failures) == len(self._follower_urls):
            raise RuntimeError(f"All replication targets failed: {'; '.join(failures)}")
        elif failures:
            print(f"Partial replication failures (continuing): {failures}")

    def _catchup_loop(self) -> None:
        """Background task for followers to sync with leader."""
        while not self._monitor_stop.is_set():
            try:
                with self._lock:
                    last_hb = self._last_heartbeat_ts
                
                # Only sync if leader is alive (recent heartbeat)
                if time.time() - last_hb <= self._heartbeat_timeout_sec:
                    self._sync_with_leader()
                
                time.sleep(5.0)
            except Exception:
                time.sleep(5.0)

    def _sync_with_leader(self) -> None:
        """Synchronize local state with current leader."""
        try:
            leader_url = self._find_current_leader()
            if not leader_url:
                return
            
            self._sync_queues_from_leader(leader_url)
            self._sync_messages_from_leader(leader_url)
            self._sync_pointers_from_leader(leader_url)
            
        except Exception as exc:
            print(f"Sync failed for {self.name}: {exc}")

    def _find_current_leader(self) -> Optional[str]:
        """Find the current leader among peers."""
        for url in self._peer_urls:
            try:
                info = self._post_json(url, "/broker_info", {})
                if info.get("is_leader", False):
                    return url
            except Exception:
                continue
        return None

    def _sync_queues_from_leader(self, leader_url: str) -> None:
        """Sync queue data from leader."""
        try:
            response = self._post_json(leader_url, "/get_all_queues", {})
            for queue_id in response.get("queue_ids", []):
                if not self._sqlite.queue_exists(queue_id):
                    self._sqlite.create_queue_with_id(queue_id)
        except Exception:
            pass

    def _sync_messages_from_leader(self, leader_url: str) -> None:
        """Sync message data from leader."""
        try:
            response = self._post_json(leader_url, "/get_all_messages", {})
            for msg in response.get("messages", []):
                try:
                    self._sqlite.append_message_with_id(
                        msg["message_id"], msg["queue_id"], msg["value"]
                    )
                except Exception:
                    # Message might already exist
                    pass
        except Exception:
            pass

    def _sync_pointers_from_leader(self, leader_url: str) -> None:
        """Sync client pointer data from leader."""
        try:
            response = self._post_json(leader_url, "/get_all_pointers", {})
            for ptr in response.get("pointers", []):
                self._sqlite.set_last_read_message_id(
                    ptr["queue_id"], ptr["client_id"], ptr["message_id"]
                )
        except Exception:
            pass

    # =============================================================================
    # Leader Election and Heartbeat
    # =============================================================================

    def _heartbeat_loop(self) -> None:
        """Send heartbeats to peers if leader."""
        while not self._hb_stop.is_set():
            try:
                with self._lock:
                    is_leader = self._is_leader
                    peers = list(self._peer_urls)
                
                if is_leader:
                    self._send_heartbeats(peers)
                
                time.sleep(self._heartbeat_interval_sec)
            except Exception:
                time.sleep(self._heartbeat_interval_sec)

    def _send_heartbeats(self, peers: List[str]) -> None:
        """Send heartbeat messages to all peers."""
        heartbeat_payload = {
            "leader_id": self.broker_id,
            "leader_url": self.self_url
        }
        
        for url in peers:
            try:
                self._post_json(url, "/heartbeat", heartbeat_payload)
            except Exception:
                pass

    def _monitor_loop(self) -> None:
        """Monitor for leader failures and trigger elections."""
        while not self._monitor_stop.is_set():
            time.sleep(self._heartbeat_interval_sec)
            
            with self._lock:
                is_leader = self._is_leader
                last_hb = self._last_heartbeat_ts
                peers = list(self._peer_urls)
                proxy_url = self._proxy_url
            
            if is_leader:
                continue
            
            # Check if leader has failed
            if time.time() - last_hb > self._heartbeat_timeout_sec:
                self._run_leader_election(peers, proxy_url)

    def _run_leader_election(self, peers: List[str], proxy_url: Optional[str]) -> None:
        """Run leader election among available peers."""
        candidates = []
        
        # Check all peers (including self) for availability
        for url in peers + [self.self_url]:
            try:
                info = self._post_json(url, "/broker_info", {})
                candidates.append((int(info["broker_id"]), str(info["url"])))
            except Exception:
                pass
        
        if not candidates:
            return
        
        # Choose leader with lowest broker ID
        candidates.sort(key=lambda x: x[0])
        winner_id, winner_url = candidates[0]
        
        if winner_url == self.self_url:
            self._become_leader(candidates, proxy_url)

    def _become_leader(self, candidates: List[Tuple[int, str]], proxy_url: Optional[str]) -> None:
        """Transition this broker to leader role."""
        with self._lock:
            self._is_leader = True
            self._last_heartbeat_ts = time.time()
            # Update followers to all other reachable peers
            self._follower_urls = [url for (_id, url) in candidates if url != self.self_url]
        
        # Notify proxy of new leadership
        if proxy_url:
            try:
                self._post_json(proxy_url, "/set_leader", {"leader_url": self.self_url})
            except Exception:
                pass

    # =============================================================================
    # HTTP Server and Request Handling
    # =============================================================================

    def start_http_server(self, host: Optional[str] = None, port: Optional[int] = None) -> None:
        """Start the HTTP server."""
        host = host or self.ip
        port = port or self.port
        super().start_http_server(host, port)

    def _create_http_handler(self):
        """Create HTTP request handler class."""
        broker = self

        class BrokerHandler(JsonRequestHandler):
            def __init__(self, *args, **kwargs):
                super().__init__(broker, *args, **kwargs)

            def _handle_request(self, path: str, body: dict) -> None:
                """Route requests to appropriate handlers with leader checking."""
                routes = {
                    # Client operations (require leadership)
                    "/create_queue": (self._handle_create_queue, True),
                    "/append": (self._handle_append, True),
                    "/read": (self._handle_read, True),
                    
                    # Replication operations (no leadership required)
                    "/apply_create_queue": (self._handle_apply_create_queue, False),
                    "/apply_append": (self._handle_apply_append, False),
                    "/apply_set_pointer": (self._handle_apply_set_pointer, False),
                    
                    # Cluster management (no leadership required)
                    "/heartbeat": (self._handle_heartbeat, False),
                    "/broker_info": (self._handle_broker_info, False),
                    "/set_follower_urls": (self._handle_set_follower_urls, False),
                    "/set_peer_urls": (self._handle_set_peer_urls, False),
                    "/start_cluster_tasks": (self._handle_start_cluster_tasks, False),
                    "/stop_cluster_tasks": (self._handle_stop_cluster_tasks, False),
                    "/stop_http_server": (self._handle_stop_http_server, False),
                    
                    # Data access (no leadership required)
                    "/get_all_queues": (self._handle_get_all_queues, False),
                    "/get_all_messages": (self._handle_get_all_messages, False),
                    "/get_all_pointers": (self._handle_get_all_pointers, False),
                }
                
                route_info = routes.get(path)
                if route_info is None:
                    self._write_json(404, {"error": "not found"})
                    return
                
                handler, requires_leader = route_info
                if requires_leader and not broker.is_leader:
                    self._write_json(400, {"error": "not leader"})
                    return
                
                handler(body)

            # Client operation handlers
            def _handle_create_queue(self, body: dict) -> None:
                queue_id = broker.create_queue()
                self._write_json(200, {"queue_id": queue_id})

            def _handle_append(self, body: dict) -> None:
                queue_id = int(body.get("queue_id"))
                value = int(body.get("value"))
                message_id = broker.append(queue_id, value)
                self._write_json(200, {"message_id": message_id})

            def _handle_read(self, body: dict) -> None:
                queue_id = int(body.get("queue_id"))
                client_id = str(body.get("client_id"))
                result = broker.read_for_client(queue_id, client_id)
                
                if result is None:
                    self._write_json(200, {})
                else:
                    message_id, value = result
                    self._write_json(200, {"message_id": message_id, "value": value})

            # Replication operation handlers
            def _handle_apply_create_queue(self, body: dict) -> None:
                queue_id = int(body.get("queue_id"))
                result = broker.apply_create_queue(queue_id)
                self._write_replication_result(result)

            def _handle_apply_append(self, body: dict) -> None:
                message_id = int(body.get("message_id"))
                queue_id = int(body.get("queue_id"))
                value = int(body.get("value"))
                result = broker.apply_append(message_id, queue_id, value)
                self._write_replication_result(result)

            def _handle_apply_set_pointer(self, body: dict) -> None:
                queue_id = int(body.get("queue_id"))
                client_id = str(body.get("client_id"))
                message_id = int(body.get("message_id"))
                result = broker.apply_set_pointer(queue_id, client_id, message_id)
                self._write_replication_result(result)

            # Cluster management handlers
            def _handle_heartbeat(self, body: dict) -> None:
                leader_id = int(body.get("leader_id"))
                with broker._lock:
                    broker._last_heartbeat_ts = time.time()
                    # Demote if another broker claims leadership
                    if broker._is_leader and leader_id != broker.broker_id:
                        broker._is_leader = False
                self._write_json(200, {"ok": True})

            def _handle_broker_info(self, body: dict) -> None:
                self._write_json(200, {
                    "broker_id": broker.broker_id,
                    "url": broker.self_url,
                    "is_leader": broker.is_leader
                })

            def _handle_set_follower_urls(self, body: dict) -> None:
                follower_urls = body.get("follower_urls", [])
                broker.set_follower_urls(follower_urls)
                self._write_json(200, {"ok": True})

            def _handle_set_peer_urls(self, body: dict) -> None:
                peer_urls = body.get("peer_urls", [])
                broker.set_peer_urls(peer_urls)
                self._write_json(200, {"ok": True})

            def _handle_start_cluster_tasks(self, body: dict) -> None:
                proxy_url = body.get("proxy_url")
                broker.start_cluster_tasks(proxy_url)
                self._write_json(200, {"ok": True})

            def _handle_stop_cluster_tasks(self, body: dict) -> None:
                broker.stop_cluster_tasks()
                self._write_json(200, {"ok": True})

            def _handle_stop_http_server(self, body: dict) -> None:
                broker.stop_http_server()
                self._write_json(200, {"ok": True})

            # Data access handlers
            def _handle_get_all_queues(self, body: dict) -> None:
                queue_ids = broker._sqlite.get_all_queue_ids()
                self._write_json(200, {"queue_ids": queue_ids})

            def _handle_get_all_messages(self, body: dict) -> None:
                messages = broker._sqlite.get_all_messages()
                self._write_json(200, {"messages": messages})

            def _handle_get_all_pointers(self, body: dict) -> None:
                pointers = broker._sqlite.get_all_pointers()
                self._write_json(200, {"pointers": pointers})

            # Helper methods
            def _write_replication_result(self, result: ReplicationResult) -> None:
                """Write replication result as JSON response."""
                if result.ok:
                    self._write_json(200, {"ok": True})
                else:
                    self._write_json(500, {"ok": False, "error": result.error or "error"})

        return BrokerHandler

    # =============================================================================
    # Utility Methods
    # =============================================================================

    def _post_json(self, url: str, path: str, payload: dict) -> dict:
        """Make HTTP JSON request to another broker."""
        client = HttpJsonClient(timeout=5)
        return client.post_json(url, path, payload)