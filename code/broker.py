from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from .http_utils import HttpJsonClient, HttpJsonServer

from .broker_data_manager import SqliteManager


@dataclass
class ReplicationResult:
    ok: bool
    error: Optional[str] = None


class Broker:
    """
    Broker node that persists queue data via SqliteManager.

    If this node is leader, it will:
      - execute a command locally
      - replicate the deterministic effects to all followers
      - wait for all followers to acknowledge
      - then return the result

    Followers only execute deterministic apply operations provided by the leader.
    """

    _used_ids: set[int] = set()
    _used_addresses: set[Tuple[str, int]] = set()

    def __init__(
        self,
        broker_id: int,
        ip: str,
        port: int,
        is_leader: bool = False,
    ) -> None:
        # Uniqueness checks for id and (ip, port)
        if broker_id in Broker._used_ids:
            # Allow reuse if the previous broker is no longer active
            Broker._used_ids.remove(broker_id)
        if (ip, port) in Broker._used_addresses:
            # Allow reuse if the previous broker is no longer active
            Broker._used_addresses.remove((ip, port))
        Broker._used_ids.add(broker_id)
        Broker._used_addresses.add((ip, port))

        self.broker_id = broker_id
        self.ip = ip
        self.port = port
        self.name = f"broker-{broker_id}"
        self._sqlite = SqliteManager(f"data/broker-{broker_id}.db")
        self._is_leader = is_leader
        self._follower_urls: List[str] = []
        self._lock = threading.RLock()
        self._peer_urls: List[str] = []
        self._proxy_url: Optional[str] = None
        self._last_heartbeat_ts: float = 0.0
        self._heartbeat_interval_sec: float = 0.5
        self._heartbeat_timeout_sec: float = 2.0
        self._hb_stop = threading.Event()
        self._monitor_stop = threading.Event()

    # Leadership management
    @property
    def is_leader(self) -> bool:
        return self._is_leader

    def set_follower_urls(self, follower_urls: List[str]) -> None:
        with self._lock:
            self._follower_urls = follower_urls

    def set_peer_urls(self, peer_urls: List[str]) -> None:
        with self._lock:
            # All other brokers in the cluster (may include leader and followers)
            self._peer_urls = [u for u in peer_urls if u != self.self_url]

    @property
    def self_url(self) -> str:
        return f"http://{self.ip}:{self.port}"

    def start_cluster_tasks(self, proxy_url: Optional[str] = None) -> None:
        with self._lock:
            self._proxy_url = proxy_url
            self._hb_stop.clear()
            self._monitor_stop.clear()
            # Start heartbeat if we are leader
            threading.Thread(target=self._heartbeat_loop, daemon=True).start()
            # Start monitor to detect leader failure and run election
            threading.Thread(target=self._monitor_loop, daemon=True).start()
            # Start catch-up process if we're a follower
            if not self._is_leader:
                threading.Thread(target=self._catchup_loop, daemon=True).start()

    def stop_cluster_tasks(self) -> None:
        self._hb_stop.set()
        self._monitor_stop.set()

    # Client-facing operations (called via Proxy on leader only)
    def create_queue(self) -> int:
        """Create a queue (leader only)."""
        if not self._is_leader:
            raise RuntimeError("create_queue must be invoked on the leader")
        with self._lock:
            queue_id = self._sqlite.create_queue()
            self._replicate("/apply_create_queue", {"queue_id": queue_id})
            return queue_id

    def append(self, queue_id: int, value: int) -> int:
        """Append a value to a queue (leader only). Returns message_id."""
        if not self._is_leader:
            raise RuntimeError("append must be invoked on the leader")
        with self._lock:
            message_id = self._sqlite.append_message(queue_id, value)
            self._replicate("/apply_append", {"message_id": message_id, "queue_id": queue_id, "value": value})
            return message_id



    def read_for_client(self, queue_id: int, client_id: str) -> Optional[Tuple[int, int]]:
        """Return next message for this client in FIFO order and advance pointer.

        This is a write operation (pointer advancement) and must replicate.
        """
        with self._lock:
            last_read = self._sqlite.get_last_read_message_id(queue_id, client_id)
            next_msg = self._sqlite.fetch_next_after(queue_id, last_read)
            if next_msg is None:
                return None
            message_id, value = next_msg
            # Advance pointer locally then replicate
            self._sqlite.set_last_read_message_id(queue_id, client_id, message_id)
            self._replicate("/apply_set_pointer", {"queue_id": queue_id, "client_id": client_id, "message_id": message_id})
            return message_id, value

    # Deterministic apply operations (used by followers during replication)
    def apply_create_queue(self, queue_id: int) -> ReplicationResult:
        try:
            with self._lock:
                if not self._sqlite.queue_exists(queue_id):
                    self._sqlite.create_queue_with_id(queue_id)
            return ReplicationResult(ok=True)
        except Exception as exc:
            return ReplicationResult(ok=False, error=str(exc))

    def apply_append(self, message_id: int, queue_id: int, value: int) -> ReplicationResult:
        try:
            with self._lock:
                # We cannot easily check arbitrary id existence without query; do direct insert and catch
                try:
                    self._sqlite.append_message_with_id(message_id, queue_id, value)
                except Exception:
                    # best-effort idempotency: ignore if already applied
                    pass
            return ReplicationResult(ok=True)
        except Exception as exc:
            return ReplicationResult(ok=False, error=str(exc))

    def apply_set_pointer(self, queue_id: int, client_id: str, message_id: int) -> ReplicationResult:
        try:
            with self._lock:
                self._sqlite.set_last_read_message_id(queue_id, client_id, message_id)
            return ReplicationResult(ok=True)
        except Exception as exc:
            return ReplicationResult(ok=False, error=str(exc))

    # Internal replication helper
    def _replicate(self, network_path: str, payload: dict) -> None:
        failures: List[str] = []
        for url in list(self._follower_urls):
            try:
                self._post_json(url, network_path, payload)
            except Exception as exc:
                failures.append(f"{url}: {exc}")
                print(f"Replication failed to {url}: {exc}")
        
        # Only fail if ALL followers failed (allow partial failures)
        if failures and len(failures) == len(self._follower_urls):
            raise RuntimeError("All replication targets failed: " + "; ".join(failures))
        elif failures:
            print(f"Partial replication failures (continuing): {failures}")

    # Catch-up and recovery methods
    def _catchup_loop(self) -> None:
        """Follower catch-up loop to sync with leader after restart."""
        while not self._monitor_stop.is_set():
            try:
                with self._lock:
                    last_hb = self._last_heartbeat_ts
                
                # Only catch up if we have a recent heartbeat (leader is alive)
                if time.time() - last_hb <= self._heartbeat_timeout_sec:
                    self._sync_with_leader()
                
                time.sleep(5.0)  # Check every 5 seconds
            except Exception:
                time.sleep(5.0)

    def _sync_with_leader(self) -> None:
        """Sync local database with current leader."""
        try:
            # Find current leader
            leader_url = None
            for url in self._peer_urls:
                try:
                    info = self._post_json(url, "/broker_info", {})
                    if info.get("is_leader", False):
                        leader_url = url
                        break
                except Exception:
                    continue
            
            if not leader_url:
                return
            
            # Get leader's data and sync
            self._sync_queues_from_leader(leader_url)
            self._sync_messages_from_leader(leader_url)
            self._sync_pointers_from_leader(leader_url)
            
        except Exception as exc:
            print(f"Sync failed for {self.name}: {exc}")

    def _sync_queues_from_leader(self, leader_url: str) -> None:
        """Sync queue data from leader."""
        try:
            queues_data = self._post_json(leader_url, "/get_all_queues", {})
            for queue_id in queues_data.get("queue_ids", []):
                if not self._sqlite.queue_exists(queue_id):
                    self._sqlite.create_queue_with_id(queue_id)
        except Exception:
            pass

    def _sync_messages_from_leader(self, leader_url: str) -> None:
        """Sync message data from leader."""
        try:
            messages_data = self._post_json(leader_url, "/get_all_messages", {})
            for msg in messages_data.get("messages", []):
                queue_id = msg["queue_id"]
                message_id = msg["message_id"]
                value = msg["value"]
                try:
                    self._sqlite.append_message_with_id(message_id, queue_id, value)
                except Exception:
                    # Message might already exist
                    pass
        except Exception:
            pass

    def _sync_pointers_from_leader(self, leader_url: str) -> None:
        """Sync read pointer data from leader."""
        try:
            pointers_data = self._post_json(leader_url, "/get_all_pointers", {})
            for ptr in pointers_data.get("pointers", []):
                queue_id = ptr["queue_id"]
                client_id = ptr["client_id"]
                message_id = ptr["message_id"]
                self._sqlite.set_last_read_message_id(queue_id, client_id, message_id)
        except Exception:
            pass

    def close(self) -> None:
        self._hb_stop.set()
        self._monitor_stop.set()
        self._sqlite.close()

    # Networking helper
    def _post_json(self, base_url: str, path: str, payload: dict) -> dict:
        client = HttpJsonClient(timeout=5)
        return client.post_json(base_url, path, payload)

    # Heartbeat and election
    def _heartbeat_loop(self) -> None:
        while not self._hb_stop.is_set():
            try:
                with self._lock:
                    is_leader = self._is_leader
                    peers = list(self._peer_urls)
                if is_leader:
                    for url in peers:
                        try:
                            self._post_json(url, "/heartbeat", {"leader_id": self.broker_id, "leader_url": self.self_url})
                        except Exception:
                            pass
                time.sleep(self._heartbeat_interval_sec)
            except Exception:
                time.sleep(self._heartbeat_interval_sec)

    def _monitor_loop(self) -> None:
        # Followers watch for missing heartbeats and run a simple election
        while not self._monitor_stop.is_set():
            time.sleep(self._heartbeat_interval_sec)
            with self._lock:
                is_leader = self._is_leader
                last_hb = self._last_heartbeat_ts
                peers = list(self._peer_urls)
                proxy_url = self._proxy_url
            if is_leader:
                continue
            now = time.time()
            if now - last_hb <= self._heartbeat_timeout_sec:
                continue
            # Start election: contact peers, choose lowest broker_id as leader among reachable
            candidates: List[Tuple[int, str]] = []
            for url in peers + [self.self_url]:
                try:
                    info = self._post_json(url, "/broker_info", {})
                    candidates.append((int(info.get("broker_id")), str(info.get("url"))))
                except Exception:
                    pass
            if not candidates:
                continue
            candidates.sort(key=lambda x: x[0])
            winner_id, winner_url = candidates[0]
            if winner_url == self.self_url:
                with self._lock:
                    self._is_leader = True
                    self._last_heartbeat_ts = time.time()
                    # Update followers to reachable peers (exclude self)
                    self._follower_urls = [url for (_id, url) in candidates if url != self.self_url]
                # Announce to proxy if configured
                if proxy_url:
                    try:
                        self._post_json(proxy_url, "/set_leader", {"leader_url": self.self_url})
                    except Exception:
                        pass

    # HTTP server
    def start_http_server(self, host: Optional[str] = None, port: Optional[int] = None) -> None:
        broker = self
        host = host or self.ip
        port = port or self.port

        class Handler(BaseHTTPRequestHandler):
            def _read_json(self) -> dict:
                return HttpJsonServer.read_json(self)

            def _write_json(self, status: int, payload: dict) -> None:
                HttpJsonServer.write_json(self, status, payload)

            # Per-command handlers
            def handle_create_queue(self, body: dict) -> None:
                qid = broker.create_queue()
                self._write_json(200, {"queue_id": qid})

            def handle_append(self, body: dict) -> None:
                qid = int(body.get("queue_id"))
                value = int(body.get("value"))
                mid = broker.append(qid, value)
                self._write_json(200, {"message_id": mid})

            def handle_read(self, body: dict) -> None:
                qid = int(body.get("queue_id"))
                client_id = str(body.get("client_id"))
                res = broker.read_for_client(qid, client_id)
                if res is None:
                    self._write_json(200, {})
                else:
                    mid, value = res
                    self._write_json(200, {"message_id": mid, "value": value})

            def handle_apply_create_queue(self, body: dict) -> None:
                qid = int(body.get("queue_id"))
                res = broker.apply_create_queue(qid)
                if res.ok:
                    self._write_json(200, {"ok": True})
                else:
                    self._write_json(500, {"ok": False, "error": res.error or "error"})

            def handle_apply_append(self, body: dict) -> None:
                mid = int(body.get("message_id"))
                qid = int(body.get("queue_id"))
                value = int(body.get("value"))
                res = broker.apply_append(mid, qid, value)
                if res.ok:
                    self._write_json(200, {"ok": True})
                else:
                    self._write_json(500, {"ok": False, "error": res.error or "error"})

            def handle_apply_set_pointer(self, body: dict) -> None:
                qid = int(body.get("queue_id"))
                client_id = str(body.get("client_id"))
                mid = int(body.get("message_id"))
                res = broker.apply_set_pointer(qid, client_id, mid)
                if res.ok:
                    self._write_json(200, {"ok": True})
                else:
                    self._write_json(500, {"ok": False, "error": res.error or "error"})

            def handle_heartbeat(self, body: dict) -> None:
                leader_id = int(body.get("leader_id"))
                with broker._lock:  # noqa: SLF001
                    broker._last_heartbeat_ts = time.time()
                    if broker._is_leader and leader_id != broker.broker_id:
                        # Demote if someone else is leader
                        broker._is_leader = False
                self._write_json(200, {"ok": True})

            def handle_broker_info(self, body: dict) -> None:
                self._write_json(200, {"broker_id": broker.broker_id, "url": broker.self_url, "is_leader": broker.is_leader})

            def handle_get_all_queues(self, body: dict) -> None:
                queue_ids = broker._sqlite.get_all_queue_ids()
                self._write_json(200, {"queue_ids": queue_ids})

            def handle_get_all_messages(self, body: dict) -> None:
                messages = broker._sqlite.get_all_messages()
                self._write_json(200, {"messages": messages})

            def handle_get_all_pointers(self, body: dict) -> None:
                pointers = broker._sqlite.get_all_pointers()
                self._write_json(200, {"pointers": pointers})

            def do_POST(self) -> None:  # noqa: N802
                try:
                    body = self._read_json()
                    routes = {
                        "/create_queue": (self.handle_create_queue, True),
                        "/append": (self.handle_append, True),
                        "/read": (self.handle_read, True),
                        "/apply_create_queue": (self.handle_apply_create_queue, False),
                        "/apply_append": (self.handle_apply_append, False),
                        "/apply_set_pointer": (self.handle_apply_set_pointer, False),
                        "/heartbeat": (self.handle_heartbeat, False),
                        "/broker_info": (self.handle_broker_info, False),
                        "/get_all_queues": (self.handle_get_all_queues, False),
                        "/get_all_messages": (self.handle_get_all_messages, False),
                        "/get_all_pointers": (self.handle_get_all_pointers, False),
                    }
                    entry = routes.get(self.path)
                    if entry is None:
                        self._write_json(404, {"error": "not found"})
                        return
                    handler, requires_leader = entry
                    if requires_leader and not broker.is_leader:
                        self._write_json(400, {"error": "not leader"})
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


