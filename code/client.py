import socket
import threading
import time
import json
import sys
import os
from typing import Dict, List, Optional, Any
from enum import Enum

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(__file__))
from id_generator import generate_client_id, extract_cluster_from_queue_id


class BrokerRole(Enum):
    LEADER = "leader"
    REPLICA = "replica"


class BrokerInfo:
    """Information about a broker discovered by the client."""
    def __init__(self, broker_id: str, host: str, port: int, role: BrokerRole, status: str, cluster_id: str = None):
        self.broker_id = broker_id
        self.host = host
        self.port = port
        self.role = role
        self.status = status
        self.cluster_id = cluster_id
        self.last_seen = time.time()
        self.is_healthy = True


class SocketUtils:
    """Shared socket communication utilities."""
    
    @staticmethod
    def send_message(sock: socket.socket, message: Dict[str, Any]):
        """Send JSON message over socket."""
        data = json.dumps(message).encode('utf-8')
        length = len(data)
        sock.sendall(length.to_bytes(4, byteorder='big') + data)
    
    @staticmethod
    def receive_message(sock: socket.socket) -> Optional[Dict[str, Any]]:
        """Receive JSON message from socket."""
        try:
            # Read message length
            length_data = sock.recv(4)
            if len(length_data) != 4:
                return None
            
            message_length = int.from_bytes(length_data, byteorder='big')
            
            # Read message data
            data = b''
            while len(data) < message_length:
                chunk = sock.recv(message_length - len(data))
                if not chunk:
                    return None
                data += chunk
            
            return json.loads(data.decode('utf-8'))
        except Exception:
            return None
    
    @staticmethod
    def send_request(sock: socket.socket, request: Dict[str, Any], timeout: float = 10.0) -> Dict[str, Any]:
        """Send request and receive response with timeout."""
        sock.settimeout(timeout)
        SocketUtils.send_message(sock, request)
        response = SocketUtils.receive_message(sock)
        if response is None:
            raise Exception("Failed to receive response")
        return response


class ClusterTopology:
    """Manages cluster topology information for the client."""
    
    def __init__(self, seed_brokers: List[str], cluster_id: str):
        self.seed_brokers = seed_brokers
        self.cluster_id = cluster_id
        self.leaders: List[BrokerInfo] = []
        self.replicas: List[BrokerInfo] = []
        self.cluster_version = 0
        self.last_update = None
        self.topology_lock = threading.Lock()
    
    def discover_cluster(self) -> bool:
        """Discover cluster topology from any available broker."""
        for seed_broker in self.seed_brokers:
            try:
                host, port = seed_broker.split(':')
                port = int(port)
                
                response = self._query_broker_topology(host, port)
                if response:
                    self.update_topology(response)
                    return True
                    
            except Exception:
                continue
        
        raise Exception("Cannot connect to any seed brokers")
    
    def update_topology(self, cluster_info: Dict[str, Any]):
        """Update local view of cluster topology."""
        if cluster_info.get("cluster_version", 0) <= self.cluster_version:
            return  # No update needed
        
        with self.topology_lock:
            self.leaders.clear()
            self.replicas.clear()
            
            for broker_data in cluster_info.get("brokers", []):
                broker_info = BrokerInfo(
                    broker_data["broker_id"],
                    broker_data["host"],
                    broker_data["port"],
                    BrokerRole(broker_data["role"]),
                    broker_data["status"],
                    cluster_info.get("cluster_id")
                )
                
                if broker_info.role == BrokerRole.LEADER:
                    self.leaders.append(broker_info)
                else:
                    self.replicas.append(broker_info)
            
            self.cluster_version = cluster_info.get("cluster_version", 0)
            self.last_update = time.time()
    
    def get_leaders(self) -> List[BrokerInfo]:
        """Get list of healthy leader brokers."""
        with self.topology_lock:
            return [leader for leader in self.leaders if leader.is_healthy]
    
    def get_all_brokers(self) -> List[BrokerInfo]:
        """Get list of all healthy brokers (leaders + replicas)."""
        with self.topology_lock:
            all_brokers = self.leaders + self.replicas
            return [broker for broker in all_brokers if broker.is_healthy]
    
    def mark_broker_unhealthy(self, broker_id: str):
        """Mark a broker as unhealthy."""
        with self.topology_lock:
            for broker_list in [self.leaders, self.replicas]:
                for broker in broker_list:
                    if broker.broker_id == broker_id:
                        broker.is_healthy = False
    
    def _query_broker_topology(self, host: str, port: int) -> Optional[Dict[str, Any]]:
        """Query broker for current cluster topology."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            
            query = {
                "operation": "CLUSTER_QUERY",
                "cluster_id": self.cluster_id
            }
            
            response = SocketUtils.send_request(sock, query)
            sock.close()
            return response
        except Exception:
            return None


class Client:
    """Client library that connects to the distributed queuing cluster and performs operations."""
    
    def __init__(self, cluster_id: str, seed_brokers: List[str], client_id: str = None):
        """Initialize client with cluster configuration."""
        self.cluster_id = cluster_id
        self.client_id = client_id or generate_client_id()
        self.seed_brokers = seed_brokers
        
        # Configuration
        self.refresh_interval = 30.0
        self.retry_attempts = 3
        self.operation_timeout = 10.0
        
        # Topology management
        self.topology = ClusterTopology(seed_brokers, cluster_id)
        self.leader_index = 0
        
        # State
        self.is_connected = False
        self.shutdown_event = threading.Event()
        self.refresh_thread = None
    
    # Connection Management
    def connect_to_cluster(self) -> bool:
        """Initial connection and topology discovery."""
        try:
            print(f"Client {self.client_id} connecting to cluster {self.cluster_id}")
            self.topology.discover_cluster()
            self.is_connected = True
            
            # Start topology refresh thread
            self._start_topology_refresh()
            
            leaders_count = len(self.topology.get_leaders())
            replicas_count = len(self.topology.replicas)
            print(f"Connected to cluster with {leaders_count} leaders, {replicas_count} replicas")
            return True
            
        except Exception as e:
            print(f"[Client {self.client_id}] Failed to connect: {e}")
            return False
    
    def disconnect(self):
        """Clean shutdown of all connections."""
        print(f"Client {self.client_id} disconnecting")
        self.shutdown_event.set()
        self.is_connected = False
        
        if self.refresh_thread:
            self.refresh_thread.join(timeout=1.0)
    
    # Queue Operations
    def create_queue(self) -> Dict[str, Any]:
        """Create new queue with auto-generated ID via leader broker."""
        request = {
            "operation": "CREATE_QUEUE",
            "client_id": self.client_id
        }
        return self._execute_request(request)
    
    def append_message(self, queue_id: str, data: int) -> Dict[str, Any]:
        """Add message via leader broker."""
        request = {
            "operation": "APPEND",
            "queue_name": queue_id,
            "data": data,
            "client_id": self.client_id
        }
        return self._execute_request(request, queue_id=queue_id)
    
    def read_message(self, queue_id: str) -> Dict[str, Any]:
        """Read next message from leader broker (with position replication)."""
        request = {
            "operation": "READ",
            "queue_name": queue_id,
            "client_id": self.client_id
        }
        return self._execute_request(request, queue_id=queue_id)
    
    def queue_exists(self, queue_id: str) -> bool:
        """Check if queue exists by attempting to read from it."""
        try:
            result = self.read_message(queue_id)
            return result.get("status") in ["success", "no_messages"]
        except:
            return False
    
    # Cluster Information
    def get_cluster_info(self) -> Dict[str, Any]:
        """Get current cluster information."""
        leaders = self.topology.get_leaders()
        if not leaders:
            return {"error": "No leaders available"}
        
        try:
            sock = self._create_connection(leaders[0])
            request = {
                "operation": "CLUSTER_QUERY",
                "client_id": self.client_id,
                "cluster_id": self.cluster_id
            }
            response = SocketUtils.send_request(sock, request, self.operation_timeout)
            sock.close()
            return response
        except Exception as e:
            return {"error": str(e)}
    
    def get_status(self) -> Dict[str, Any]:
        """Get client status and cluster information."""
        return {
            "client_id": self.client_id,
            "cluster_id": self.cluster_id,
            "is_connected": self.is_connected,
            "cluster_version": self.topology.cluster_version,
            "leaders_count": len(self.topology.get_leaders()),
            "replicas_count": len(self.topology.replicas),
            "last_topology_update": self.topology.last_update
        }
    
    # Private Methods
    def _execute_request(self, request: Dict[str, Any], queue_id: str = None) -> Dict[str, Any]:
        """Execute operation with automatic failover on broker failure."""
        for attempt in range(self.retry_attempts):
            try:
                broker = self._select_leader(queue_id)
                if not broker:
                    self.refresh_topology()
                    broker = self._select_leader(queue_id)
                    if not broker:
                        return {"status": "error", "message": "No leaders available"}
                
                # Execute request
                sock = self._create_connection(broker)
                response = SocketUtils.send_request(sock, request, self.operation_timeout)
                sock.close()
                
                # Check for leader redirect
                if (response.get("status") == "error" and 
                    "Only leaders" in response.get("message", "")):
                    self.refresh_topology()
                    continue
                
                return response
                
            except Exception as e:
                print(f"[Client {self.client_id}] Attempt {attempt + 1} failed: {e}")
                
                # Mark broker as unhealthy if we have one
                if 'broker' in locals():
                    self.topology.mark_broker_unhealthy(broker.broker_id)
                
                # Refresh topology on last attempt
                if attempt == self.retry_attempts - 1:
                    self.refresh_topology()
        
        return {"status": "error", "message": "All brokers unavailable"}
    
    def _select_leader(self, queue_id: str = None) -> Optional[BrokerInfo]:
        """Choose leader broker, preferring same cluster as queue if specified."""
        leaders = self.topology.get_leaders()
        if not leaders:
            return None
        
        # If queue_id provided, try to use same-cluster leader
        if queue_id:
            try:
                queue_cluster_id = extract_cluster_from_queue_id(queue_id)
                same_cluster_leaders = [
                    leader for leader in leaders 
                    if leader.cluster_id == queue_cluster_id
                ]
                
                if same_cluster_leaders:
                    leader = same_cluster_leaders[self.leader_index % len(same_cluster_leaders)]
                    self.leader_index += 1
                    return leader
            except ValueError:
                pass  # Invalid queue ID format, fall back to any leader
        
        # Round-robin through all leaders
        leader = leaders[self.leader_index % len(leaders)]
        self.leader_index += 1
        return leader
    
    def _create_connection(self, broker: BrokerInfo) -> socket.socket:
        """Create connection to specific broker."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((broker.host, broker.port))
        return sock
    
    def refresh_topology(self) -> bool:
        """Update broker list and roles."""
        try:
            old_version = self.topology.cluster_version
            self.topology.discover_cluster()
            
            if self.topology.cluster_version > old_version:
                print(f"[Client {self.client_id}] Topology updated to version {self.topology.cluster_version}")
                return True
            return False
        except Exception as e:
            print(f"[Client {self.client_id}] Failed to refresh topology: {e}")
            return False
    
    def _start_topology_refresh(self):
        """Start background topology refresh thread."""
        self.refresh_thread = threading.Thread(target=self._topology_refresh_loop, daemon=True)
        self.refresh_thread.start()
    
    def _topology_refresh_loop(self):
        """Background thread for periodic topology refresh."""
        while not self.shutdown_event.is_set():
            try:
                self.refresh_topology()
            except Exception as e:
                print(f"[Client {self.client_id}] Error in topology refresh: {e}")
            
            self.shutdown_event.wait(self.refresh_interval)