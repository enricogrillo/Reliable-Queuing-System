from __future__ import annotations

from typing import Optional, Tuple
import json
from urllib.parse import urlparse
import http.client
from http_utils import HttpJsonClient

from proxy import Proxy


class Client:
    """Simple client that talks only to the Proxy."""

    def __init__(self, client_id: str, proxy: Proxy, proxy_url: Optional[str] = None) -> None:
        self.client_id = client_id
        self._proxy = proxy
        self._proxy_url = proxy_url

    def create_queue(self) -> int:
        if self._proxy_url:
            res = self._post_json(self._proxy_url, "/create_queue", {})
            return int(res["queue_id"])  # type: ignore[index]
        return self._proxy.create_queue()

    def append(self, queue_id: int, value: str) -> int:
        if self._proxy_url:
            res = self._post_json(
                self._proxy_url, "/append", {"queue_id": queue_id, "value": value}
            )
            return int(res["message_id"])  # type: ignore[index]
        return self._proxy.append(queue_id, value)

    def read(self, queue_id: int, client_id: Optional[str] = None) -> Optional[Tuple[int, str]]:
        client_id = client_id or self.client_id
        if self._proxy_url:
            res = self._post_json(
                self._proxy_url, "/read", {"queue_id": queue_id, "client_id": client_id}
            )
            if res.get("message_id") is None:
                return None
            return int(res["message_id"]), str(res["value"])
        return self._proxy.read(queue_id, client_id)

    # Networking helper
    def _post_json(self, base_url: str, path: str, payload: dict) -> dict:
        client = HttpJsonClient(timeout=5)
        return client.post_json(base_url, path, payload)


