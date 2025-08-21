from __future__ import annotations

import json
import http.client
from urllib.parse import urlparse
from typing import Any, Dict


class HttpJsonClient:
    """HTTP client for making JSON requests."""

    def __init__(self, timeout: float = 5.0) -> None:
        self._timeout = timeout

    def post_json(self, base_url: str, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Make a POST request with JSON payload and return JSON response."""
        parsed = urlparse(base_url)
        conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=self._timeout)
        body = json.dumps(payload).encode("utf-8")
        try:
            conn.request(
                "POST",
                path,
                body=body,
                headers={"Content-Type": "application/json", "Content-Length": str(len(body))},
            )
            resp = conn.getresponse()
            data = resp.read()
            if resp.status != 200:
                raise RuntimeError(f"HTTP {resp.status} {resp.reason}")
            return json.loads(data.decode("utf-8")) if data else {}
        finally:
            conn.close()
