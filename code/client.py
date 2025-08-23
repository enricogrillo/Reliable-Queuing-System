import socket
import threading
import time
import json
import uuid
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
                    
            except Exception as e:
                print(f"Failed to discover from {seed_broker}: {e}")
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
            
            self._send_message(sock, query)
            response = self._receive_message(sock)
            sock.close()
            
            return response
            
        except Exception as e:
            print(f"Failed to query broker {host}:{port}: {e}")
            return None
    
    def _send_message(self, sock: socket.socket, message: Dict[str, Any]):
        """Send JSON message over socket."""
        data = json.dumps(message).encode('utf-8')
        length = len(data)
        sock.sendall(length.to_bytes(4, byteorder='big') + data)
    
    def _receive_message(self, sock: socket.socket) -> Optional[Dict[str, Any]]:
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
            
        except Exception as e:
            print(f"Error receiving message: {e}")
            return None


class BrokerConnectionPool:
    """Manages connections to multiple brokers."""
    
    def __init__(self, topology: ClusterTopology):
        self.topology = topology
        self.connections: Dict[str, socket.socket] = {}
        self.leader_index = 0
        self.broker_index = 0
        self.connection_lock = threading.Lock()
    
    def get_leader_connection(self) -> Optional[BrokerInfo]:
        """Round-robin selection of leader brokers."""
        leaders = self.topology.get_leaders()
        if not leaders:
            return None
        
        leader = leaders[self.leader_index % len(leaders)]
        self.leader_index += 1
        return leader
    
    def get_any_connection(self) -> Optional[BrokerInfo]:
        """Round-robin selection across all broker connections."""
        all_brokers = self.topology.get_all_brokers()
        if not all_brokers:
            return None
        
        broker = all_brokers[self.broker_index % len(all_brokers)]
        self.broker_index += 1
        return broker
    
    def create_connection(self, broker: BrokerInfo) -> socket.socket:
        """Create connection to specific broker."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((broker.host, broker.port))
        return sock


class Client:
    """Client library that connects to the distributed queuing cluster and performs operations."""
    
    def __init__(self, cluster_id: str, seed_brokers: List[str], client_id: str = None):
        """Initialize client with cluster configuration."""
        self.cluster_id = cluster_id
        self.client_id = client_id or generate_client_id()
        self.seed_brokers = seed_brokers
        
        # Topology management
        self.topology = ClusterTopology(seed_brokers, cluster_id)
        self.connection_pool = BrokerConnectionPool(self.topology)
        
        # Configuration
        self.refresh_interval = 30.0  # seconds
        self.retry_attempts = 3
        self.operation_timeout = 10.0  # seconds
        
        # State
        self.is_connected = False
        self.shutdown_event = threading.Event()
        
        # Thread management
        self.refresh_thread = None
    
    def connect_to_cluster(self) -> bool:
        """Initial connection and topology discovery."""
        try:
            print(f"Client {self.client_id} connecting to cluster {self.cluster_id}")
            self.topology.discover_cluster()
            self.is_connected = True
            
            # Start topology refresh thread
            self._start_topology_refresh()
            
            print(f"Connected to cluster with {len(self.topology.get_leaders())} leaders, "
                  f"{len(self.topology.replicas)} replicas")
            return True
            
        except Exception as e:
            print(f"[Client {self.client_id}] Failed to connect to cluster: {e}")
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
        
        return self._execute_with_failover(request, use_leaders=True)
    
    def append_message(self, queue_id: str, data: int) -> Dict[str, Any]:
        """Add message via leader broker."""
        request = {
            "operation": "APPEND",
            "queue_name": queue_id,
            "data": data,
            "client_id": self.client_id
        }
        
        return self._execute_with_failover(request, use_leaders=True, queue_id=queue_id)
    
    def read_message(self, queue_id: str) -> Dict[str, Any]:
        """Read next message from leader broker (with position replication)."""
        request = {
            "operation": "READ",
            "queue_name": queue_id,
            "client_id": self.client_id
        }
        
        return self._execute_with_failover(request, use_leaders=True, queue_id=queue_id)
    
    def queue_exists(self, queue_id: str) -> bool:
        """Check if queue exists by attempting to read from it."""
        try:
            result = self.read_message(queue_id)
            return result.get("status") in ["success", "no_messages"]
        except:
            return False
    
    # Connection Management
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
    
    def get_cluster_info(self) -> Dict[str, Any]:
        """Get current cluster information."""
        leaders = self.topology.get_leaders()
        if not leaders:
            return {"error": "No leaders available"}
        
        broker = leaders[0]
        
        try:
            sock = self.connection_pool.create_connection(broker)
            
            request = {
                "operation": "CLUSTER_QUERY",
                "client_id": self.client_id,
                "cluster_id": self.cluster_id
            }
            
            response = self._send_request(sock, request)
            sock.close()
            
            return response
            
        except Exception as e:
            print(f"[Client {self.client_id}] Failed to get cluster info: {e}")
            return {"error": str(e)}
    
    # Broker Selection
    def select_leader(self) -> Optional[BrokerInfo]:
        """Choose leader broker for all operations (reads and writes)."""
        return self.connection_pool.get_leader_connection()
    
    def select_leader_for_queue(self, queue_id: str) -> Optional[BrokerInfo]:
        """Choose leader broker, preferring same cluster as queue."""
        try:
            # Extract cluster from queue ID
            queue_cluster_id = extract_cluster_from_queue_id(queue_id)
            
            # Get all leaders
            leaders = self.topology.get_leaders()
            if not leaders:
                return None
            
            # Prefer leaders from the same cluster
            same_cluster_leaders = [
                leader for leader in leaders 
                if leader.cluster_id == queue_cluster_id
            ]
            
            if same_cluster_leaders:
                # Use round-robin within same cluster leaders
                leader = same_cluster_leaders[self.connection_pool.leader_index % len(same_cluster_leaders)]
                self.connection_pool.leader_index += 1
                print(f"[Client {self.client_id}] Selected leader {leader.broker_id} from same cluster {queue_cluster_id} for queue {queue_id}")
                return leader
            else:
                # Fall back to any leader if no same-cluster leader available
                leader = self.connection_pool.get_leader_connection()
                if leader:
                    print(f"[Client {self.client_id}] No same-cluster leader found, using {leader.broker_id} for queue {queue_id}")
                return leader
                
        except ValueError:
            # Invalid queue ID format, fall back to regular leader selection
            print(f"[Client {self.client_id}] Invalid queue ID format: {queue_id}, using any leader")
            return self.connection_pool.get_leader_connection()
    
    def select_any_broker(self) -> Optional[BrokerInfo]:
        """Choose any broker for non-critical operations (deprecated - use select_leader)."""
        return self.connection_pool.get_any_connection()
    
    # Error Handling and Failover
    def _execute_with_failover(self, request: Dict[str, Any], use_leaders: bool = True, queue_id: str = None) -> Dict[str, Any]:
        """Execute operation with automatic failover on broker failure."""
        for attempt in range(self.retry_attempts):
            try:
                # Choose broker based on queue cluster affinity if queue_id provided
                if queue_id:
                    broker = self.select_leader_for_queue(queue_id)
                else:
                    broker = self.select_leader()
                    
                if not broker:
                    # Refresh topology and try again
                    self.refresh_topology()
                    if queue_id:
                        broker = self.select_leader_for_queue(queue_id)
                    else:
                        broker = self.select_leader()
                    if not broker:
                        return {"status": "error", "message": "No leaders available"}
                
                # Execute request
                response = self._execute_on_broker(broker, request)
                
                # Check for leader redirect
                if (response.get("status") == "error" and 
                    "Only leaders" in response.get("message", "")):
                    # Broker is not a leader, refresh topology and retry
                    self.refresh_topology()
                    continue
                
                return response
                
            except Exception as e:
                print(f"[Client {self.client_id}] Attempt {attempt + 1} failed: {e}")
                
                # Mark broker as unhealthy
                if 'broker' in locals():
                    self.topology.mark_broker_unhealthy(broker.broker_id)
                
                # Refresh topology on last attempt
                if attempt == self.retry_attempts - 1:
                    self.refresh_topology()
        
        return {"status": "error", "message": "All brokers unavailable"}
    
    def _execute_on_broker(self, broker: BrokerInfo, request: Dict[str, Any]) -> Dict[str, Any]:
        """Execute request on specific broker."""
        sock = self.connection_pool.create_connection(broker)
        
        try:
            response = self._send_request(sock, request)
            return response
        finally:
            sock.close()
    
    def _send_request(self, sock: socket.socket, request: Dict[str, Any]) -> Dict[str, Any]:
        """Send request and receive response."""
        # Set socket timeout
        sock.settimeout(self.operation_timeout)
        
        # Send request
        data = json.dumps(request).encode('utf-8')
        length = len(data)
        sock.sendall(length.to_bytes(4, byteorder='big') + data)
        
        # Receive response
        length_data = sock.recv(4)
        if len(length_data) != 4:
            raise Exception("Invalid response length")
        
        message_length = int.from_bytes(length_data, byteorder='big')
        
        response_data = b''
        while len(response_data) < message_length:
            chunk = sock.recv(message_length - len(response_data))
            if not chunk:
                raise Exception("Connection closed during response")
            response_data += chunk
        
        return json.loads(response_data.decode('utf-8'))
    
    # Background Tasks
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
    
    # Utility Methods
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
