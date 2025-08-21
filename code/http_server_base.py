from __future__ import annotations

import json
import threading
from typing import Dict, Callable, Optional, Any
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class HttpServerBase:
    """Base class for HTTP servers with JSON request/response handling."""

    def __init__(self) -> None:
        self._httpd: Optional[ThreadingHTTPServer] = None
        self._server_thread: Optional[threading.Thread] = None

    def start_http_server(self, host: str, port: int) -> None:
        """Start the HTTP server."""
        if self._httpd is not None:
            raise RuntimeError("HTTP server is already running")
            
        handler_class = self._create_http_handler()
        self._httpd = ThreadingHTTPServer((host, port), handler_class)
        self._server_thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._server_thread.start()

    def stop_http_server(self) -> None:
        """Stop the HTTP server."""
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
        if self._server_thread is not None:
            self._server_thread.join(timeout=2)
            self._server_thread = None

    def _create_http_handler(self):
        """Create HTTP request handler class. Must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement _create_http_handler")


class JsonRequestHandler(BaseHTTPRequestHandler):
    """Base request handler with JSON utilities and common routing logic."""

    def __init__(self, server_instance, *args, **kwargs):
        self.server_instance = server_instance
        super().__init__(*args, **kwargs)

    def _read_json(self) -> Dict[str, Any]:
        """Read JSON request body."""
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    def _write_json(self, status: int, payload: Dict[str, Any]) -> None:
        """Write JSON response."""
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self) -> None:  # noqa: N802
        """Handle POST requests with JSON routing."""
        try:
            body = self._read_json()
            self._handle_request(self.path, body)
        except Exception as exc:
            self._write_json(500, {"error": str(exc)})

    def _handle_request(self, path: str, body: dict) -> None:
        """Handle a request. Must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement _handle_request")

    def log_message(self, fmt: str, *args) -> None:
        """Suppress HTTP server log messages."""
        pass
