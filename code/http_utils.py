from __future__ import annotations

import json
import http.client
from urllib.parse import urlparse
from http.server import BaseHTTPRequestHandler
from typing import Any, Dict


class HttpJsonClient:
    def __init__(self, timeout: float = 5.0) -> None:
        self._timeout = timeout

    def post_json(self, base_url: str, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
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


class HttpJsonServer:
    @staticmethod
    def read_json(handler: BaseHTTPRequestHandler) -> Dict[str, Any]:
        length = int(handler.headers.get("Content-Length", "0"))
        raw = handler.rfile.read(length) if length else b"{}"
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    @staticmethod
    def write_json(handler: BaseHTTPRequestHandler, status: int, payload: Dict[str, Any]) -> None:
        data = json.dumps(payload).encode("utf-8")
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(data)))
        handler.end_headers()
        handler.wfile.write(data)


