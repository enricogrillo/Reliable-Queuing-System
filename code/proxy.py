from __future__ import annotations

import threading
from typing import Optional, Tuple
import json
from urllib.parse import urlparse
import http.client

from broker import Broker
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from http_utils import HttpJsonClient, HttpJsonServer


class Proxy:
    """
    Proxy forwards client calls to the current leader Broker.
    """

    def __init__(self, leader: Broker, leader_url: Optional[str] = None) -> None:
        self._leader = leader
        self._leader_url = leader_url
        self._lock = threading.RLock()

    # Note: setters removed as they're unused in this demo

    def create_queue(self) -> int:
        with self._lock:
            if self._leader_url:
                return int(self._post_json(self._leader_url, "/create_queue", {})["queue_id"])
            return self._leader.create_queue()

    def append(self, queue_id: int, value: str) -> int:
        with self._lock:
            if self._leader_url:
                return int(
                    self._post_json(
                        self._leader_url, "/append", {"queue_id": queue_id, "value": value}
                    )["message_id"]
                )
            return self._leader.append(queue_id, value)

    def read(self, queue_id: int, client_id: str) -> Optional[Tuple[int, str]]:
        with self._lock:
            if self._leader_url:
                res = self._post_json(
                    self._leader_url, "/read", {"queue_id": queue_id, "client_id": client_id}
                )
                if res.get("message_id") is None:
                    return None
                return int(res["message_id"]), str(res["value"])
            return self._leader.read_for_client(queue_id, client_id)

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
                value = str(body.get("value"))
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

            def do_POST(self) -> None:  # noqa: N802
                try:
                    body = self._read_json()
                    routes = {
                        "/create_queue": self.handle_create_queue,
                        "/append": self.handle_append,
                        "/read": self.handle_read,
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


