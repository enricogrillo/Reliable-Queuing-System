from __future__ import annotations

from typing import Optional, Tuple
from .http_utils import HttpJsonClient


class Client:
    """Simple client that talks only to the Proxy."""

    def __init__(self, name: str, proxy_address: Tuple[str, int]) -> None:
        self.client_id = name
        self._proxy_url = f"http://{proxy_address[0]}:{proxy_address[1]}"

    def create_queue(self) -> int:
        res = self._post_json(self._proxy_url, "/create_queue", {})
        return int(res["queue_id"])  # type: ignore[index]

    def append(self, queue_id: int, value: int) -> int:
        res = self._post_json(
            self._proxy_url, "/append", {"queue_id": queue_id, "value": value}
        )
        return int(res["message_id"])  # type: ignore[index]

    def read(self, queue_id: int, client_id: Optional[str] = None) -> Optional[Tuple[int, int]]:
        client_id = client_id or self.client_id
        res = self._post_json(
            self._proxy_url, "/read", {"queue_id": queue_id, "client_id": client_id}
        )
        if res.get("message_id") is None:
            return None
        return int(res["message_id"]), int(res["value"])  # type: ignore[index]

    # Networking helper
    def _post_json(self, base_url: str, path: str, payload: dict) -> dict:
        client = HttpJsonClient(timeout=5)
        return client.post_json(base_url, path, payload)


