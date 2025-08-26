#!/usr/bin/env python3
"""
NetworkHandler - Handles all network-related operations for the broker.
"""

import json
import socket
import struct
import threading
from typing import Dict, List, Optional, Tuple, Callable


class NetworkHandler:
    """
    Handles TCP server, client connections, and message sending/receiving.
    """
    
    def __init__(self, broker_id: str, listen_host: str, listen_port: int, message_processor: Callable[[Dict, str], Optional[Dict]]):
        """Initialize network handler."""
        self.broker_id = broker_id
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.message_processor = message_processor
        
        # Network components
        self.server_socket = None
        self.server_thread = None
        self.running = False
        
        # Connection tracking
        self.client_connections: Dict[str, socket.socket] = {}
        self.broker_connections: Dict[str, socket.socket] = {}
        
        print(f"[{self.broker_id}] NetworkHandler initialized on {listen_host}:{listen_port}")
    
    def start(self):
        """Start the network server."""
        if self.running:
            return
            
        print(f"[{self.broker_id}] Starting network server...")
        self.running = True
        self._start_network_server()
    
    def stop(self):
        """Stop the network server and cleanup resources."""
        if not self.running:
            return
            
        print(f"[{self.broker_id}] Stopping network server...")
        self.running = False
        
        # Close server socket
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
        
        # Close all connections
        for conn in list(self.client_connections.values()):
            try:
                conn.close()
            except:
                pass
        
        for conn in list(self.broker_connections.values()):
            try:
                conn.close()
            except:
                pass
        
        self.client_connections.clear()
        self.broker_connections.clear()
    
    def _start_network_server(self):
        """Start TCP server for client and broker connections."""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.listen_host, self.listen_port))
            self.server_socket.listen(10)
            
            # Start server thread
            self.server_thread = threading.Thread(target=self._server_loop, daemon=True)
            self.server_thread.start()
            
            print(f"[{self.broker_id}] Network server started on {self.listen_host}:{self.listen_port}")
            
        except Exception as e:
            print(f"[{self.broker_id}] Failed to start network server: {e}")
            raise
    
    def _server_loop(self):
        """Main server loop accepting connections."""
        while self.running:
            try:
                if not self.server_socket:
                    break
                
                client_socket, client_address = self.server_socket.accept()
                
                # Handle connection in separate thread
                handler_thread = threading.Thread(
                    target=self._handle_connection,
                    args=(client_socket, client_address),
                    daemon=True
                )
                handler_thread.start()
                
            except OSError:
                # Socket was closed
                break
            except Exception as e:
                if self.running:
                    print(f"[{self.broker_id}] Server loop error: {e}")
                break
    
    def _handle_connection(self, client_socket: socket.socket, client_address: Tuple[str, int]):
        """Handle individual client/broker connection."""
        connection_id = f"{client_address[0]}:{client_address[1]}"
        
        try:
            while self.running:
                # Receive message
                message = self.receive_message(client_socket)
                if not message:
                    break
                
                # Process message and send response
                response = self.message_processor(message, connection_id)
                if response:
                    self.send_message(client_socket, response)
        
        except Exception as e:
            if self.running:
                print(f"[{self.broker_id}] Connection {connection_id} error: {e}")
        
        finally:
            try:
                client_socket.close()
            except:
                pass
            
            # Remove from connection tracking
            if connection_id in self.client_connections:
                del self.client_connections[connection_id]
            if connection_id in self.broker_connections:
                del self.broker_connections[connection_id]
    
    def receive_message(self, sock: socket.socket) -> Optional[Dict]:
        """Receive length-prefixed JSON message."""
        try:
            # Receive 4-byte length prefix
            length_bytes = self._receive_exact(sock, 4)
            if not length_bytes:
                return None
            
            length = struct.unpack('>I', length_bytes)[0]
            
            # Receive JSON payload
            json_bytes = self._receive_exact(sock, length)
            if not json_bytes:
                return None
            
            return json.loads(json_bytes.decode('utf-8'))
        
        except Exception as e:
            print(f"[{self.broker_id}] Failed to receive message: {e}")
            return None
    
    def _receive_exact(self, sock: socket.socket, length: int) -> Optional[bytes]:
        """Receive exact number of bytes."""
        data = b''
        while len(data) < length:
            chunk = sock.recv(length - len(data))
            if not chunk:
                return None
            data += chunk
        return data
    
    def send_message(self, sock: socket.socket, message: Dict) -> bool:
        """Send length-prefixed JSON message."""
        try:
            json_bytes = json.dumps(message).encode('utf-8')
            length = struct.pack('>I', len(json_bytes))
            sock.sendall(length + json_bytes)
            return True
        except Exception as e:
            print(f"[{self.broker_id}] Failed to send message: {e}")
            return False
    
    def send_to_broker(self, host: str, port: int, message: Dict, timeout: float = 5.0) -> Optional[Dict]:
        """Send message to specific broker and return response."""
        try:
            # Create connection to broker
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect((host, port))
            
            # Send message
            if not self.send_message(sock, message):
                sock.close()
                return None
            
            # Receive response
            response = self.receive_message(sock)
            sock.close()
            
            return response
        
        except Exception as e:
            print(f"[{self.broker_id}] Failed to send to {host}:{port}: {e}")
            return None
    
    def get_connection_count(self) -> Dict[str, int]:
        """Get count of active connections."""
        return {
            "client_connections": len(self.client_connections),
            "broker_connections": len(self.broker_connections)
        }
