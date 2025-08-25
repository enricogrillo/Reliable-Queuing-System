import socket
import threading
import time
import json
import uuid
import sys
import os
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum

from code.client import SocketUtils

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(__file__))
from broker_data_manager import BrokerDataManager
from id_generator import generate_broker_id, generate_queue_id


class BrokerRole(Enum):
    LEADER = "leader"
    REPLICA = "replica"


class BrokerStatus(Enum):
    STARTING = "starting"
    JOINING = "joining"
    ACTIVE = "active"
    FAILED = "failed"


class BrokerInfo:
    """Information about a broker in the cluster."""
    def __init__(self, broker_id: str, host: str, port: int, role: BrokerRole, status: BrokerStatus):
        self.broker_id = broker_id
        self.host = host
        self.port = port
        self.role = role
        self.status = status
        self.last_heartbeat = time.time()


class Broker:
    """Main broker node that handles client requests, cluster membership, and replication coordination."""
    
    def __init__(self, broker_id: str, cluster_id: str, listen_host: str, listen_port: int, 
                 seed_brokers: List[str], db_path: str = None):
        """Initialize broker with configuration."""
        self.broker_id = broker_id or generate_broker_id()
        self.cluster_id = cluster_id
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.seed_brokers = seed_brokers
        
        # State management
        self.role = BrokerRole.REPLICA  # Start as replica, may become leader
        self.status = BrokerStatus.STARTING
        self.cluster_members: Dict[str, BrokerInfo] = {}
        self.cluster_version = 0
        
        # Data management
        db_file = db_path or f"broker_{self.broker_id}.db"
        self.data_manager = BrokerDataManager(db_file)
        
        # Network management
        self.server_socket = None
        self.broker_connections: Dict[str, socket.socket] = {}
        self.client_connections: Dict[str, socket.socket] = {}
        
        # Threading
        self.shutdown_event = threading.Event()
        self.heartbeat_interval = 5.0  # seconds
        self.election_timeout = 15.0  # seconds
        
        # Thread-safe locks
        self.cluster_lock = threading.Lock()
        self.connection_lock = threading.Lock()
    
    def start(self):
        """Start the broker and join cluster."""
        print(f"Starting broker {self.broker_id} on {self.listen_host}:{self.listen_port}")
        
        # Restore state from persistent storage
        self._restore_state()
        
        # Start network server
        self._start_server()
        
        # Join cluster or become initial leader
        if not self._join_cluster():
            self._become_initial_leader()
        
        # Start background threads
        self._start_background_threads()
        
        self.status = BrokerStatus.ACTIVE
        print(f"Broker {self.broker_id} is now {self.status.value} as {self.role.value}")
    
    def stop(self):
        """Gracefully stop the broker."""
        print(f"Stopping broker {self.broker_id}")
        self.shutdown_event.set()
        
        # Close all connections
        with self.connection_lock:
            for conn in list(self.broker_connections.values()) + list(self.client_connections.values()):
                try:
                    conn.close()
                except:
                    pass
        
        # Close server socket
        if self.server_socket:
            self.server_socket.close()
        
        # Close data manager
        self.data_manager.close()
    
    # Cluster Operations
    def _join_cluster(self) -> bool:
        """Connect to existing cluster using seed brokers."""
        for seed_broker in self.seed_brokers:
            try:
                host, port = seed_broker.split(':')
                port = int(port)
                
                # Connect to seed broker
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((host, port))
                
                # Send join request
                join_request = {
                    "operation": "JOIN_CLUSTER",
                    "broker_id": self.broker_id,
                    "host": self.listen_host,
                    "port": self.listen_port,
                    "cluster_id": self.cluster_id
                }
                
                SocketUtils.send_message(sock, join_request)
                response = SocketUtils.receive_message(sock)
                
                if response and response.get("status") == "success":
                    # Update cluster membership from response
                    self._update_cluster_membership(response.get("cluster_info", {}))
                    
                    # Apply data snapshot if provided for catch-up
                    data_snapshot = response.get("data_snapshot")
                    if data_snapshot:
                        print(f"[{self.broker_id}] Applying data snapshot for catch-up...")
                        success = self.data_manager.apply_data_snapshot(data_snapshot)
                        if success:
                            print(f"[{self.broker_id}] Successfully caught up with cluster data")
                        else:
                            print(f"[{self.broker_id}] Failed to apply data snapshot")
                    
                    sock.close()
                    return True
                
                sock.close()
                
            except Exception as e:
                print(f"[{self.broker_id}] Failed to connect to seed broker {seed_broker}: {e}")
                continue
        
        return False
    
    def _become_initial_leader(self):
        """Become the initial leader of a new cluster."""
        print(f"[{self.broker_id}] Becoming initial leader of cluster {self.cluster_id}")
        self.role = BrokerRole.LEADER
        self.cluster_version += 1
        
        # Add self to cluster membership
        with self.cluster_lock:
            self.cluster_members[self.broker_id] = BrokerInfo(
                self.broker_id, self.listen_host, self.listen_port, 
                BrokerRole.LEADER, BrokerStatus.ACTIVE
            )
    
    def handle_broker_join(self, join_request: Dict[str, Any]) -> Dict[str, Any]:
        """Process join requests from new brokers."""
        if self.role != BrokerRole.LEADER:
            return {"status": "error", "message": "Not a leader"}
        
        new_broker_id = join_request["broker_id"]
        
        with self.cluster_lock:
            # Add new broker as replica
            self.cluster_members[new_broker_id] = BrokerInfo(
                new_broker_id, join_request["host"], join_request["port"],
                BrokerRole.REPLICA, BrokerStatus.ACTIVE
            )
            self.cluster_version += 1
            
            # Prepare cluster info for new broker
            cluster_info = {
                "cluster_version": self.cluster_version,
                "brokers": [
                    {
                        "broker_id": info.broker_id,
                        "host": info.host,
                        "port": info.port,
                        "role": info.role.value,
                        "status": info.status.value
                    }
                    for info in self.cluster_members.values()
                ]
            }
        
        # Replicate membership change to other brokers
        self._replicate_membership_change()
        
        # Get data snapshot for the new broker to catch up
        data_snapshot = self.data_manager.get_full_data_snapshot()
        
        return {
            "status": "success",
            "cluster_info": cluster_info,
            "data_snapshot": data_snapshot
        }
    
    def _update_cluster_membership(self, cluster_info: Dict[str, Any]):
        """Update local cluster membership from cluster info."""
        with self.cluster_lock:
            self.cluster_version = cluster_info.get("cluster_version", 0)
            self.cluster_members.clear()
            
            for broker_data in cluster_info.get("brokers", []):
                broker_info = BrokerInfo(
                    broker_data["broker_id"],
                    broker_data["host"], 
                    broker_data["port"],
                    BrokerRole(broker_data["role"]),
                    BrokerStatus(broker_data["status"])
                )
                self.cluster_members[broker_info.broker_id] = broker_info
    
    def send_heartbeat(self):
        """Send periodic heartbeat to other brokers."""
        heartbeat_msg = {
            "operation": "HEARTBEAT",
            "broker_id": self.broker_id,
            "timestamp": time.time(),
            "role": self.role.value,
            "cluster_version": self.cluster_version
        }
        
        # leader sends heartbeats to everyone
        if self.role == BrokerRole.LEADER:
            with self.cluster_lock:
                other_brokers = [info for info in self.cluster_members.values() 
                            if info.broker_id != self.broker_id]

        # replicas send heartbeats to leader only
        else:
            with self.cluster_lock:
                other_brokers = [info for info in self.cluster_members.values() 
                            if info.broker_id != self.broker_id and info.role == BrokerRole.LEADER]
            
        for broker_info in other_brokers:
            try:
                self._send_to_broker(broker_info, heartbeat_msg)
            except Exception as e:
                print(f"[{self.broker_id}] Failed to send heartbeat to {broker_info.broker_id}: {e}")

    
    def detect_failures(self):
        """Monitor broker health and handle failures."""

        current_time = time.time()
        failed_brokers = []

        with self.cluster_lock:
            for broker_id, broker_info in self.cluster_members.items():
                if ((self.role == BrokerRole.LEADER or broker_info.role == BrokerRole.LEADER) and 
                    broker_id != self.broker_id and 
                    current_time - broker_info.last_heartbeat > self.election_timeout):
                    failed_brokers.append(broker_id)
                    broker_info.status = BrokerStatus.FAILED

        if failed_brokers:
            print(f"[{self.broker_id}] Detected failed brokers: {failed_brokers}")
            self._handle_broker_failures(failed_brokers)
    
    def _handle_broker_failures(self, failed_brokers: List[str]):
        """Handle broker failures and trigger leader election if needed."""
        leader_failed = any(
            self.cluster_members[broker_id].role == BrokerRole.LEADER 
            for broker_id in failed_brokers 
            if broker_id in self.cluster_members
        )
        
        # Remove failed brokers from cluster membership
        if self.role == BrokerRole.LEADER:
            self._remove_failed_brokers_from_cluster(failed_brokers)
        
        if leader_failed and self.role == BrokerRole.REPLICA:
            self._trigger_leader_election()
    
    def _trigger_leader_election(self):
        """Trigger leader election process."""
        print(f"[{self.broker_id}] Triggering leader election")
        
        # Simple election: broker with lowest ID becomes leader
        with self.cluster_lock:
            active_brokers = [
                broker_id for broker_id, info in self.cluster_members.items()
                if info.status == BrokerStatus.ACTIVE
            ]
        
        if active_brokers and min(active_brokers) == self.broker_id:
            self.promote_to_leader()
    
    def promote_to_leader(self):
        """Transition from replica to leader role."""
        print(f"[{self.broker_id}] Promoting to leader")
        
        self.role = BrokerRole.LEADER
        self.cluster_version += 1
        
        with self.cluster_lock:
            if self.broker_id in self.cluster_members:
                self.cluster_members[self.broker_id].role = BrokerRole.LEADER
        
        # Announce promotion to other brokers
        promotion_msg = {
            "operation": "PROMOTE_TO_LEADER",
            "broker_id": self.broker_id,
            "cluster_version": self.cluster_version
        }
        
        self._broadcast_to_replicas(promotion_msg)
    
    def _remove_failed_brokers_from_cluster(self, failed_brokers: List[str]):
        """Remove failed brokers from cluster membership and propagate changes."""
        if self.role != BrokerRole.LEADER:
            return
        
        removed_brokers = []
        with self.cluster_lock:
            for broker_id in failed_brokers:
                if broker_id in self.cluster_members:
                    del self.cluster_members[broker_id]
                    removed_brokers.append(broker_id)
            
            if removed_brokers:
                self.cluster_version += 1
                print(f"[{self.broker_id}] Removed failed brokers from cluster: {removed_brokers}")
        
        # Propagate membership change to remaining healthy brokers
        if removed_brokers:
            self._replicate_membership_change()
    
    # Client Operations
    def create_queue(self) -> Dict[str, Any]:
        """Create new queue with auto-generated ID (leaders only)."""
        if self.role != BrokerRole.LEADER:
            return {"status": "error", "message": "Only leaders can create queues"}
        
        # Generate new queue ID
        queue_id = generate_queue_id(self.cluster_id)
        
        if self.data_manager.create_queue(queue_id):
            # Replicate queue creation to replicas
            replication_msg = {
                "operation": "REPLICATE",
                "type": "CREATE_QUEUE",
                "queue_id": queue_id,
                "timestamp": time.time()
            }
            
            if self._replicate_to_replicas(replication_msg):
                print(f"Created queue {queue_id}")
                return {"status": "success", "queue_id": queue_id}
            else:
                return {"status": "error", "message": "Replication failed"}
        else:
            return {"status": "error", "message": "Queue already exists"}
    
    def append_message(self, queue_name: str, data: int) -> Dict[str, Any]:
        """Add message to queue (leaders only)."""
        if self.role != BrokerRole.LEADER:
            return {"status": "error", "message": "Only leaders can append messages"}
        
        if not self.data_manager.queue_exists(queue_name):
            return {"status": "error", "message": "Queue does not exist"}
        
        sequence_num = self.data_manager.append_message(queue_name, data)
        
        # Replicate message to replicas
        replication_msg = {
            "operation": "REPLICATE",
            "type": "APPEND_MESSAGE",
            "queue_id": queue_name,
            "sequence_num": sequence_num,
            "data": data,
            "timestamp": time.time()
        }
        
        if self._replicate_to_replicas(replication_msg):
            print(f"Append value {data} to queue {queue_name}")
            return {"status": "success", "sequence_num": sequence_num}
        else:
            return {"status": "error", "message": "Replication failed"}
    
    def read_message(self, queue_name: str, client_id: str) -> Dict[str, Any]:
        """Read next message for client (leaders only)."""
        if self.role != BrokerRole.LEADER:
            return {"status": "error", "message": "Only leaders can handle read operations"}
        
        if not self.data_manager.queue_exists(queue_name):
            return {"status": "error", "message": "Queue does not exist"}
        
        current_position = self.data_manager.get_client_position(client_id, queue_name)
        message = self.data_manager.get_next_message(queue_name, current_position)
        
        if message:
            sequence_num, data = message
            
            # Replicate position update to replicas before responding
            position_update = {
                "operation": "REPLICATE",
                "type": "UPDATE_POSITION",
                "client_id": client_id,
                "queue_id": queue_name,
                "new_position": sequence_num,
                "timestamp": time.time()
            }
            
            # Update local position first
            self.data_manager.update_client_position(client_id, queue_name, sequence_num)
            
            # Replicate position update to all replicas
            if self._replicate_to_replicas(position_update):
                print(f"Client {client_id} read from queue {queue_name}")
                return {
                    "status": "success",
                    "sequence_num": sequence_num,
                    "data": data
                }
            else:
                # Rollback local position update if replication failed
                self.data_manager.update_client_position(client_id, queue_name, current_position)
                return {"status": "error", "message": "Position replication failed"}
        else:
            return {"status": "no_messages", "message": "No new messages"}
    
    def get_cluster_info(self) -> Dict[str, Any]:
        """Return cluster topology to clients."""
        with self.cluster_lock:
            brokers = [
                {
                    "broker_id": info.broker_id,
                    "host": info.host,
                    "port": info.port,
                    "role": info.role.value,
                    "status": info.status.value,
                    "last_heartbeat": info.last_heartbeat
                }
                for info in self.cluster_members.values()
                if info.status == BrokerStatus.ACTIVE
            ]
        
        return {
            "cluster_id": self.cluster_id,
            "cluster_version": self.cluster_version,
            "brokers": brokers
        }
    
    # Replication Operations
    def _replicate_to_replicas(self, operation_data: Dict[str, Any]) -> bool:
        """Send data to all replica brokers and wait for majority ACK."""
        with self.cluster_lock:
            replicas = [info for info in self.cluster_members.values() 
                       if info.role == BrokerRole.REPLICA and info.status == BrokerStatus.ACTIVE]
        
        if not replicas:
            return True  # No replicas to replicate to
        
        acks_received = 0
        required_acks = len(replicas) // 2 + 1  # Majority
        
        for replica in replicas:
            try:
                response = self._send_to_broker(replica, operation_data)
                if response and response.get("status") == "success":
                    acks_received += 1
            except Exception as e:
                print(f"[{self.broker_id}] Failed to replicate to {replica.broker_id}: {e}")
        
        return acks_received >= required_acks
    
    def handle_replication(self, operation_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process replication from leader."""
        try:
            op_type = operation_data.get("type")
            
            if op_type == "CREATE_QUEUE":
                self.data_manager.create_queue(operation_data["queue_id"])
            
            elif op_type == "APPEND_MESSAGE":
                # For replicas, we need to insert with specific sequence number
                queue_id = operation_data["queue_id"]
                sequence_num = operation_data["sequence_num"] 
                data = operation_data["data"]
                
                # Direct insert (bypassing auto-increment)
                self.data_manager.db_connection.execute(
                    "INSERT INTO queue_data (queue_id, sequence_num, data) VALUES (?, ?, ?)",
                    (queue_id, sequence_num, data)
                )
                self.data_manager.db_connection.commit()
            
            elif op_type == "UPDATE_POSITION":
                # Replicate client position update
                client_id = operation_data["client_id"]
                queue_id = operation_data["queue_id"]
                new_position = operation_data["new_position"]
                
                self.data_manager.update_client_position(client_id, queue_id, new_position)
            
            return {"status": "success"}
            
        except Exception as e:
            print(f"Replication failed: {e}")
            return {"status": "error", "message": str(e)}
    
    def _broadcast_to_replicas(self, message: Dict[str, Any]):
        """Broadcast message to all replica brokers."""
        with self.cluster_lock:
            replicas = [info for info in self.cluster_members.values() 
                       if info.role == BrokerRole.REPLICA and info.status == BrokerStatus.ACTIVE]
        
        for replica in replicas:
            try:
                self._send_to_broker(replica, message)
            except Exception as e:
                print(f"[{self.broker_id}] Failed to broadcast to {replica.broker_id}: {e}")
    
    def _replicate_membership_change(self):
        """Replicate cluster membership changes to other brokers."""
        membership_msg = {
            "operation": "MEMBERSHIP_UPDATE",
            "cluster_version": self.cluster_version,
            "cluster_info": self.get_cluster_info()
        }
        
        self._broadcast_to_replicas(membership_msg)
    
    # Network Communication
    def _start_server(self):
        """Start TCP server for client and broker connections."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.listen_host, self.listen_port))
        self.server_socket.listen(10)
        
        # Start connection handler thread
        threading.Thread(target=self._handle_connections, daemon=True).start()
    
    def _handle_connections(self):
        """Handle incoming connections."""
        while not self.shutdown_event.is_set():
            try:
                conn, addr = self.server_socket.accept()
                threading.Thread(target=self._handle_client_connection, args=(conn, addr), daemon=True).start()
            except Exception as e:
                if not self.shutdown_event.is_set():
                    print(f"Error accepting connection: {e}")
    
    def _handle_client_connection(self, conn: socket.socket, addr: tuple):
        """Handle individual client connection."""
        try:
            while not self.shutdown_event.is_set():
                message = SocketUtils.receive_message(conn)
                if not message:
                    break
                
                response = self._process_message(message)
                SocketUtils.send_message(conn, response)
                
        except Exception as e:
            print(f"Error handling client {addr}: {e}")
        finally:
            conn.close()
    
    def _process_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Process incoming message and return response."""
        operation = message.get("operation")
        
        if operation == "CREATE_QUEUE":
            return self.create_queue()
        
        elif operation == "APPEND":
            return self.append_message(message.get("queue_name"), message.get("data"))
        
        elif operation == "READ":
            return self.read_message(message.get("queue_name"), message.get("client_id"))
        
        elif operation == "CLUSTER_QUERY":
            return self.get_cluster_info()
        
        elif operation == "JOIN_CLUSTER":
            return self.handle_broker_join(message)
        
        elif operation == "REPLICATE":
            return self.handle_replication(message)
        
        elif operation == "HEARTBEAT":
            return self._handle_heartbeat(message)
        
        elif operation == "MEMBERSHIP_UPDATE":
            return self._update_cluster_membership(message['cluster_info'])
        
        elif operation == "DATA_SYNC_REQUEST":
            return self._handle_data_sync_request(message)
        
        else:
            return {"status": "error", "message": f"Unknown operation: {operation}"}
    
    def _handle_heartbeat(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle heartbeat from another broker."""
        broker_id = message.get("broker_id")
        
        with self.cluster_lock:
            if broker_id in self.cluster_members:
                self.cluster_members[broker_id].last_heartbeat = time.time()
        
        return {"status": "success"}
    
    def _handle_data_sync_request(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle data synchronization request from another broker."""
        if self.role != BrokerRole.LEADER:
            return {"status": "error", "message": "Only leader can provide data snapshot"}
        
        # Generate and return data snapshot
        data_snapshot = self.data_manager.get_full_data_snapshot()
        
        return {
            "status": "success",
            "data_snapshot": data_snapshot
        }
    
    def request_data_sync_from_leader(self) -> bool:
        """Request data synchronization from the current leader."""
        with self.cluster_lock:
            leaders = [info for info in self.cluster_members.values() 
                      if info.role == BrokerRole.LEADER and info.status == BrokerStatus.ACTIVE]
        
        if not leaders:
            print(f"[{self.broker_id}] No leader available for data sync")
            return False
        
        leader = leaders[0]
        try:
            sync_request = {
                "operation": "DATA_SYNC_REQUEST",
                "broker_id": self.broker_id
            }
            
            response = self._send_to_broker(leader, sync_request)
            if response and response.get("status") == "success":
                data_snapshot = response.get("data_snapshot")
                if data_snapshot:
                    success = self.data_manager.apply_data_snapshot(data_snapshot)
                    if success:
                        print(f"[{self.broker_id}] Successfully synchronized data from leader {leader.broker_id}")
                        return True
                    else:
                        print(f"[{self.broker_id}] Failed to apply data snapshot")
                        return False
            
            print(f"[{self.broker_id}] Data sync request failed: {response}")
            return False
            
        except Exception as e:
            print(f"[{self.broker_id}] Failed to sync data from leader: {e}")
            return False
    
    def _send_to_broker(self, broker_info: BrokerInfo, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Send message to specific broker and return response."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((broker_info.host, broker_info.port))
            
            SocketUtils.send_message(sock, message)
            response = SocketUtils.receive_message(sock)
            
            sock.close()
            return response
            
        except Exception as e:
            print(f"[{self.broker_id}] Failed to send message to {broker_info.broker_id}: {e}")
            return None
    
    # Background Threads
    def _start_background_threads(self):
        """Start background maintenance threads."""
        # Heartbeat thread
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        
        # Failure detection thread
        threading.Thread(target=self._failure_detection_loop, daemon=True).start()
    
    def _heartbeat_loop(self):
        """Background thread for sending heartbeats."""
        while not self.shutdown_event.is_set():
            self.send_heartbeat()
            self.shutdown_event.wait(self.heartbeat_interval)
    
    def _failure_detection_loop(self):
        """Background thread for detecting failures."""
        while not self.shutdown_event.is_set():
            self.detect_failures()
            self.shutdown_event.wait(self.heartbeat_interval)
    
    # State Management
    def _restore_state(self):
        """Restore broker state from persistent storage."""
        state = self.data_manager.restore_broker_state()
        print(f"Restored state: {len(state['queues'])} queues, {len(state['client_positions'])} client positions")
