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
    """Simple cluster topology management."""
    
    def __init__(self, seed_brokers: List[str], cluster_id: str):
        self.seed_brokers = seed_brokers
        self.cluster_id = cluster_id
        self.brokers = {}  # broker_id -> BrokerInfo
        self.cluster_version = 0
        
    def _update_from_response(self, response: Dict[str, Any]):
        """Update topology from cluster response."""
        if 'brokers' not in response:
            return
        
        self.cluster_version = response.get('cluster_version', self.cluster_version + 1)
        self.brokers.clear()
        
        for broker_data in response['brokers']:
            broker_info = BrokerInfo(
                broker_id=broker_data['broker_id'],
                host=broker_data['host'],
                port=broker_data['port'],
                role=BrokerRole.LEADER if broker_data.get('role') == 'leader' else BrokerRole.REPLICA,
                status=broker_data.get('status', 'active'),
                cluster_id=self.cluster_id
            )
            self.brokers[broker_info.broker_id] = broker_info
    
    def get_leader(self) -> Optional[BrokerInfo]:
        """Get the first healthy leader broker."""
        leaders = [b for b in self.brokers.values() 
                  if b.role == BrokerRole.LEADER and b.is_healthy]
        return leaders[0] if leaders else None
    
    def get_replicas(self) -> List[BrokerInfo]:
        """Get healthy replica brokers."""
        return [b for b in self.brokers.values() 
                if b.role == BrokerRole.REPLICA and b.is_healthy]
    
    @property
    def replicas(self):
        """Backward compatibility property."""
        return self.get_replicas()
    
    def mark_broker_unhealthy(self, broker_id: str):
        """Mark a broker as unhealthy."""
        if broker_id in self.brokers:
            self.brokers[broker_id].is_healthy = False


class Client:
    """Client library that connects to the distributed queuing cluster and performs operations."""
    
    def __init__(self, seed_brokers: List[str], client_id: str = None):
        """Initialize client with seed brokers list."""
        self.client_id = client_id or generate_client_id()
        self.retry_attempts = 3
        self.operation_timeout = 10.0
        
        # Seed brokers and cluster discovery
        self.seed_brokers = list(seed_brokers) if seed_brokers else []
        self.clusters = {}  # cluster_id -> {topology, is_connected, seed_brokers}
        self.cluster_creation_index = 0
        self.assigned_seed_brokers = set()  # Track which seed brokers are already assigned to clusters
        
        # Background refresh
        self.shutdown_event = threading.Event()
        self.refresh_thread = None
    
    def add_seed_brokers(self, seed_brokers: List[str]):
        """Add new seed brokers to the client."""
        new_brokers = [broker for broker in seed_brokers if broker not in self.seed_brokers]
        self.seed_brokers.extend(new_brokers)
        print(f"[Client {self.client_id}] Added {len(new_brokers)} new seed brokers")
        
        # Immediately try to discover clusters from new seed brokers
        if new_brokers and self.refresh_thread is not None:  # Only if background refresh is running
            print(f"[Client {self.client_id}] Attempting immediate discovery from new seed brokers")
            self._discover_new_clusters_from_unassigned_brokers()
    
    def connect(self) -> bool:
        """Connect to all discoverable clusters from seed brokers."""
        print(f"[Client {self.client_id}] Discovering clusters from {len(self.seed_brokers)} seed brokers")
        
        # Use the unified discovery method to scan all seed brokers
        clusters_before = len(self.clusters)
        self._discover_new_clusters_from_unassigned_brokers()
        clusters_after = len(self.clusters)
        
        discovered_clusters = clusters_after - clusters_before
        
        if discovered_clusters > 0:
            print(f"[Client {self.client_id}] Successfully discovered {discovered_clusters} clusters")
        else:
            print(f"[Client {self.client_id}] No clusters discovered initially - will continue scanning in background")
        
        # Always start background refresh for auto-discovery, regardless of initial discovery results
        self._start_background_refresh()
        return True
    
    def _add_discovered_cluster(self, cluster_id: str, broker_addr: str, response: Dict[str, Any]):
        """Add a newly discovered cluster."""
        # Create topology from the broker that responded
        topology = ClusterTopology([broker_addr], cluster_id)
        topology._update_from_response(response)
        
        self.clusters[cluster_id] = {
            'topology': topology,
            'is_connected': True,
            'seed_brokers': [broker_addr]
        }
        # Mark this seed broker as assigned to a cluster
        self.assigned_seed_brokers.add(broker_addr)
    
    def _update_cluster_broker(self, cluster_id: str, broker_addr: str):
        """Update existing cluster with additional broker."""
        cluster = self.clusters.get(cluster_id)
        if cluster and broker_addr not in cluster['seed_brokers']:
            cluster['seed_brokers'].append(broker_addr)
            cluster['topology'].seed_brokers.append(broker_addr)
            # Mark this seed broker as assigned to a cluster
            self.assigned_seed_brokers.add(broker_addr)
    

    
    def _get_next_cluster_for_creation(self) -> str:
        """Get next cluster for queue creation using round-robin."""
        clusters = list(self.clusters.keys())
        if not clusters:
            return None
        
        cluster_id = clusters[self.cluster_creation_index % len(clusters)]
        self.cluster_creation_index += 1
        return cluster_id
    
    def _start_background_refresh(self):
        """Start background topology refresh thread."""
        if self.refresh_thread is not None:
            return  # Already started
        
        self.refresh_thread = threading.Thread(target=self._background_refresh_loop, daemon=True)
        self.refresh_thread.start()
    
    def _background_refresh_loop(self):
        """Background thread for periodic topology refresh and new cluster discovery."""
        while not self.shutdown_event.is_set():
            try:
                self._refresh_all_topologies()
                self._discover_new_clusters_from_unassigned_brokers()
            except Exception as e:
                print(f"[Client {self.client_id}] Error in topology refresh/discovery: {e}")
            
            # Wait 30 seconds between refreshes
            self.shutdown_event.wait(30.0)
    
    def _refresh_all_topologies(self):
        """Refresh topology for all connected clusters."""
        for cluster_id, cluster_info in self.clusters.items():
            if not cluster_info.get('is_connected'):
                continue
                
            topology = cluster_info.get('topology')
            if not topology:
                continue
            
            # Try to refresh from any available broker
            for broker_addr in topology.seed_brokers:
                try:
                    host, port = broker_addr.split(':')
                    port = int(port)
                    
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(5.0)
                    sock.connect((host, port))
                    
                    request = {
                        "operation": "CLUSTER_QUERY",
                        "client_id": self.client_id,
                        "cluster_id": cluster_id
                    }
                    
                    response = SocketUtils.send_request(sock, request, 5.0)
                    sock.close()
                    
                    if response and response.get('brokers'):
                        topology._update_from_response(response)
                        break  # Successfully refreshed
                        
                except Exception:
                    continue  # Try next broker
    
    def _discover_new_clusters_from_unassigned_brokers(self):
        """Scan unassigned seed brokers for new cluster discovery."""
        # Find seed brokers that are not assigned to any cluster
        unassigned_brokers = [broker for broker in self.seed_brokers 
                            if broker not in self.assigned_seed_brokers]
        
        if not unassigned_brokers:
            return  # All seed brokers are assigned
        
        print(f"[Client {self.client_id}] Scanning {len(unassigned_brokers)} unassigned seed brokers for new clusters")
        
        for broker_addr in unassigned_brokers:
            try:
                host, port = broker_addr.split(':')
                port = int(port)
                
                # Try to connect and get cluster info
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5.0)
                sock.connect((host, port))
                
                request = {
                    "operation": "CLUSTER_QUERY",
                    "client_id": self.client_id
                }
                
                response = SocketUtils.send_request(sock, request, 5.0)
                sock.close()
                
                if response and response.get('cluster_id'):
                    cluster_id = response['cluster_id']
                    
                    # Check if this is a new cluster or existing one
                    if cluster_id not in self.clusters:
                        # New cluster discovered!
                        self._add_discovered_cluster(cluster_id, broker_addr, response)
                        print(f"[Client {self.client_id}] Auto-discovered new cluster: {cluster_id} from broker {broker_addr}")
                    else:
                        # This broker belongs to an existing cluster, update it
                        self._update_cluster_broker(cluster_id, broker_addr)
                        print(f"[Client {self.client_id}] Found additional broker {broker_addr} for existing cluster {cluster_id}")
                    
            except Exception as e:
                # Broker might be down or unreachable, skip silently in background discovery
                continue
    
    def disconnect(self):
        """Shutdown client and stop background threads."""
        print(f"[Client {self.client_id}] Disconnecting")
        self.shutdown_event.set()
        
        if self.refresh_thread:
            self.refresh_thread.join(timeout=1.0)
    
    # Queue Operations
    def create_queue(self) -> Dict[str, Any]:
        """Create new queue with auto-generated ID via leader broker (round-robin cluster selection)."""
        cluster_id = self._get_next_cluster_for_creation()
        if not cluster_id:
            return {"status": "error", "message": "No clusters available"}
        
        request = {
            "operation": "CREATE_QUEUE",
            "client_id": self.client_id
        }
        return self._execute_request_on_cluster(request, cluster_id)
    
    def append_message(self, queue_id: str, data: int) -> Dict[str, Any]:
        """Add message via leader broker (automatically routes to correct cluster)."""
        # Extract cluster ID from queue ID
        try:
            cluster_id = extract_cluster_from_queue_id(queue_id)
        except ValueError:
            # If queue ID format is invalid, use first available cluster
            clusters = list(self.clusters.keys())
            cluster_id = clusters[0] if clusters else None
        
        if not cluster_id:
            return {"status": "error", "message": "Cannot determine cluster for queue"}
        
        request = {
            "operation": "APPEND",
            "queue_name": queue_id,
            "data": data,
            "client_id": self.client_id
        }
        return self._execute_request_on_cluster(request, cluster_id)
    
    def read_message(self, queue_id: str) -> Dict[str, Any]:
        """Read next message from leader broker (automatically routes to correct cluster)."""
        # Extract cluster ID from queue ID
        try:
            cluster_id = extract_cluster_from_queue_id(queue_id)
        except ValueError:
            # If queue ID format is invalid, use first available cluster
            clusters = list(self.clusters.keys())
            cluster_id = clusters[0] if clusters else None
        
        if not cluster_id:
            return {"status": "error", "message": "Cannot determine cluster for queue"}
        
        request = {
            "operation": "READ",
            "queue_name": queue_id,
            "client_id": self.client_id
        }
        return self._execute_request_on_cluster(request, cluster_id)
    
    def queue_exists(self, queue_id: str) -> bool:
        """Check if queue exists by attempting to read from it."""
        try:
            result = self.read_message(queue_id)
            return result.get("status") in ["success", "no_messages"]
        except:
            return False
    
    # Cluster Information
    def get_cluster_info(self, cluster_id: str = None) -> Dict[str, Any]:
        """Get cluster information."""
        # Use provided cluster_id or first available cluster
        target_cluster_id = cluster_id
        if not target_cluster_id:
            clusters = list(self.clusters.keys())
            target_cluster_id = clusters[0] if clusters else None
        
        if not target_cluster_id:
            return {"error": "No cluster specified"}
        
        request = {
            "operation": "CLUSTER_QUERY",
            "client_id": self.client_id,
            "cluster_id": target_cluster_id
        }
        return self._execute_request_on_cluster(request, target_cluster_id)
    
    def get_all_clusters_info(self) -> Dict[str, Any]:
        """Get information about all connected clusters."""
        all_info = {}
        for cluster_id in self.clusters.keys():
            all_info[cluster_id] = self.get_cluster_info(cluster_id)
        return all_info
    
    # Private Methods
    def _execute_request_on_cluster(self, request: Dict[str, Any], cluster_id: str) -> Dict[str, Any]:
        """Execute operation on specific cluster with automatic failover."""
        cluster = self.clusters.get(cluster_id)
        if not cluster:
            return {"status": "error", "message": f"Cluster {cluster_id} not found"}
        
        topology = cluster.get('topology')
        if not topology:
            return {"status": "error", "message": f"No topology for cluster {cluster_id}"}
        
        for attempt in range(self.retry_attempts):
            try:
                broker = topology.get_leader()
                if not broker:
                    return {"status": "error", "message": f"No leaders available in cluster {cluster_id}"}
                
                # Execute request
                sock = self._create_connection(broker)
                response = SocketUtils.send_request(sock, request, self.operation_timeout)
                sock.close()
                return response
                
            except Exception as e:
                print(f"[Client {self.client_id}] Attempt {attempt + 1} failed on cluster {cluster_id}: {e}")
                if 'broker' in locals() and topology:
                    topology.mark_broker_unhealthy(broker.broker_id)
                if attempt < self.retry_attempts - 1:
                    time.sleep(0.5)
        
        return {"status": "error", "message": f"All brokers unavailable in cluster {cluster_id}"}
    
    def _create_connection(self, broker: BrokerInfo) -> socket.socket:
        """Create connection to specific broker."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((broker.host, broker.port))
        return sock