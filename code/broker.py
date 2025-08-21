from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import List, Optional, Tuple
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from http_utils import HttpJsonClient, HttpJsonServer

from sqlite_manager import SqliteManager


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
        db_path: str,
        is_leader: bool = False,
        followers: Optional[List["Broker"]] = None,
        follower_urls: Optional[List[str]] = None,
        name: Optional[str] = None,
    ) -> None:
        # Uniqueness checks for id and (ip, port)
        if broker_id in Broker._used_ids:
            raise ValueError(f"Broker id already used: {broker_id}")
        if (ip, port) in Broker._used_addresses:
            raise ValueError(f"Broker address already used: {ip}:{port}")
        Broker._used_ids.add(broker_id)
        Broker._used_addresses.add((ip, port))

        self.broker_id = broker_id
        self.ip = ip
        self.port = port
        self.name = name or f"broker-{broker_id}"
        self._sqlite = SqliteManager(db_path)
        self._is_leader = is_leader
        self._followers: List[Broker] = followers or []
        self._follower_urls: List[str] = follower_urls or []
        self._lock = threading.RLock()

    # Leadership management
    @property
    def is_leader(self) -> bool:
        return self._is_leader

    def set_follower_urls(self, follower_urls: List[str]) -> None:
        with self._lock:
            self._follower_urls = follower_urls

    # Client-facing operations (called via Proxy on leader only)
    def create_queue(self) -> int:
        """Create a queue (leader only)."""
        if not self._is_leader:
            raise RuntimeError("create_queue must be invoked on the leader")
        with self._lock:
            queue_id = self._sqlite.create_queue()
            self._replicate(
                network_path="/apply_create_queue",
                payload={"queue_id": queue_id},
                local_method="apply_create_queue",
            )
            return queue_id

    def append(self, queue_id: int, value: str) -> int:
        """Append a value to a queue (leader only). Returns message_id."""
        if not self._is_leader:
            raise RuntimeError("append must be invoked on the leader")
        with self._lock:
            message_id = self._sqlite.append_message(queue_id, value)
            self._replicate(
                network_path="/apply_append",
                payload={"message_id": message_id, "queue_id": queue_id, "value": value},
                local_method="apply_append",
            )
            return message_id

    def read(self, queue_id: int) -> Optional[Tuple[int, str]]:
        """Deprecated: use read_for_client with client_id."""
        with self._lock:
            return self._sqlite.fetch_next_message(queue_id)

    def read_for_client(self, queue_id: int, client_id: str) -> Optional[Tuple[int, str]]:
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
            self._replicate(
                network_path="/apply_set_pointer",
                payload={"queue_id": queue_id, "client_id": client_id, "message_id": message_id},
                local_method="apply_set_pointer",
            )
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

    def apply_append(self, message_id: int, queue_id: int, value: str) -> ReplicationResult:
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
    def _replicate(self, network_path: str, payload: dict, local_method: str) -> None:
        failures: List[str] = []
        if self._follower_urls:
            for url in list(self._follower_urls):
                try:
                    self._post_json(url, network_path, payload)
                except Exception as exc:
                    failures.append(f"{url}: {exc}")
        else:
            for follower in list(self._followers):
                method = getattr(follower, local_method)
                res = method(**payload)
                if not res.ok:
                    failures.append(f"{follower.name}: {res.error}")
        if failures:
            raise RuntimeError("Replication failed: " + "; ".join(failures))

    def close(self) -> None:
        self._sqlite.close()

    # Networking helper
    def _post_json(self, base_url: str, path: str, payload: dict) -> dict:
        client = HttpJsonClient(timeout=5)
        return client.post_json(base_url, path, payload)

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
                value = str(body.get("value"))
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
                value = str(body.get("value"))
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


