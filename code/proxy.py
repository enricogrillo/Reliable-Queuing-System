from __future__ import annotations

import threading
from typing import Optional, Tuple, List

from .http_client import HttpJsonClient
from .http_server_base import HttpServerBase, JsonRequestHandler


class Proxy(HttpServerBase):
    """Simple HTTP proxy that forwards client requests to broker URLs."""

    def __init__(self, broker_urls: List[str]) -> None:
        """
        Initialize proxy with a list of broker URLs.
        
        Args:
            broker_urls: List of broker URLs (e.g., ["http://127.0.0.1:8001", "http://127.0.0.1:8002"])
        """
        super().__init__()
        self._lock = threading.RLock()
        self._broker_urls = broker_urls[:]  # Copy to avoid external modification
        self._current_broker_index = 0

    def update_broker_urls(self, broker_urls: List[str]) -> None:
        """Update the list of broker URLs."""
        with self._lock:
            self._broker_urls = broker_urls[:]
            self._current_broker_index = 0

    def add_broker_url(self, broker_url: str) -> None:
        """Add a broker URL to the list."""
        with self._lock:
            if broker_url not in self._broker_urls:
                self._broker_urls.append(broker_url)

    def remove_broker_url(self, broker_url: str) -> None:
        """Remove a broker URL from the list."""
        with self._lock:
            if broker_url in self._broker_urls:
                self._broker_urls.remove(broker_url)
                # Reset index if needed
                if self._current_broker_index >= len(self._broker_urls):
                    self._current_broker_index = 0

    def get_broker_urls(self) -> List[str]:
        """Get a copy of the current broker URLs."""
        with self._lock:
            return self._broker_urls[:]

    def _get_next_broker_url(self) -> str:
        """Get next broker URL using round-robin."""
        with self._lock:
            if not self._broker_urls:
                raise RuntimeError("No brokers configured")
            url = self._broker_urls[self._current_broker_index]
            self._current_broker_index = (self._current_broker_index + 1) % len(self._broker_urls)
            return url

    def _try_all_brokers(self, path: str, payload: dict) -> dict:
        """Try operation on all brokers until one succeeds."""
        with self._lock:
            broker_urls = self._broker_urls[:]
        
        last_error = None
        for url in broker_urls:
            try:
                client = HttpJsonClient(timeout=5)
                return client.post_json(url, path, payload)
            except Exception as e:
                last_error = e
                continue
        
        raise RuntimeError(f"All brokers failed. Last error: {last_error}")

    # =============================================================================
    # Queue Operations
    # =============================================================================

    def create_queue(self) -> int:
        """Create a new queue on the next available broker."""
        url = self._get_next_broker_url()
        client = HttpJsonClient(timeout=5)
        result = client.post_json(url, "/create_queue", {})
        return int(result["queue_id"])

    def append(self, queue_id: int, value: int) -> int:
        """Append a value to a queue."""
        result = self._try_all_brokers("/append", {"queue_id": queue_id, "value": value})
        return int(result["message_id"])

    def read(self, queue_id: int, client_id: str) -> Optional[Tuple[int, int]]:
        """Read next message from a queue for a client."""
        result = self._try_all_brokers("/read", {"queue_id": queue_id, "client_id": client_id})
        if result.get("message_id") is None:
            return None
        return int(result["message_id"]), int(result["value"])

    def close(self) -> None:
        """Close the proxy and clean up resources."""
        self.stop_http_server()

    # =============================================================================
    # Private Helper Methods
    # =============================================================================

    def _create_http_handler(self):
        """Create HTTP request handler class."""
        proxy = self

        class ProxyHandler(JsonRequestHandler):
            def __init__(self, *args, **kwargs):
                super().__init__(proxy, *args, **kwargs)

            def _handle_request(self, path: str, body: dict) -> None:
                """Handle incoming requests by routing to appropriate handlers."""
                routes = {
                    "/create_queue": self._handle_create_queue,
                    "/append": self._handle_append,
                    "/read": self._handle_read,
                }
                
                handler = routes.get(path)
                if handler is None:
                    self._write_json(404, {"error": "not found"})
                    return
                
                handler(body)

            def _handle_create_queue(self, body: dict) -> None:
                queue_id = proxy.create_queue()
                self._write_json(200, {"queue_id": queue_id})

            def _handle_append(self, body: dict) -> None:
                queue_id = int(body.get("queue_id"))
                value = int(body.get("value"))
                message_id = proxy.append(queue_id, value)
                self._write_json(200, {"message_id": message_id})

            def _handle_read(self, body: dict) -> None:
                queue_id = int(body.get("queue_id"))
                client_id = str(body.get("client_id"))
                result = proxy.read(queue_id, client_id)
                
                if result is None:
                    self._write_json(200, {})
                else:
                    message_id, value = result
                    self._write_json(200, {"message_id": message_id, "value": value})

        return ProxyHandler