#!/usr/bin/env python3
"""
Broker Implementation - Leader-Replica Architecture with Strong Consistency
Based on BROKER_DETAILED_EXPLANATION.md
"""

import json
import socket
import struct
import threading
import time
import random
from enum import Enum
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

from .broker_data_manager import BrokerDataManager
from .id_generator import generate_queue_id


class BrokerRole(Enum):
    """Broker role enumeration."""
    LEADER = "leader"
    REPLICA = "replica"


class BrokerStatus(Enum):
    """Broker status enumeration."""
    STARTING = "starting"
    ACTIVE = "active"
    FAILED = "failed"
    STOPPED = "stopped"


@dataclass
class ClusterMember:
    """Information about a cluster member."""
    broker_id: str
    host: str
    port: int
    role: BrokerRole
    status: BrokerStatus
    last_heartbeat: float
    cluster_version: int


class Broker:
    """
    Main broker implementation with leader-replica architecture.
    
    Features:
    - Leader-replica replication with strong consistency
    - Majority consensus for all operations
    - Leader election with failure handling
    - TCP network server with JSON protocol
    - Persistent storage via SQLite
    """
    
    def __init__(self, broker_id: str, cluster_id: str, listen_host: str, listen_port: int, seed_brokers: List[str] = None):
        """Initialize broker instance."""
        self.broker_id = broker_id
        self.cluster_id = cluster_id
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.seed_brokers = seed_brokers or []
        
        # Core state
        self.role = BrokerRole.REPLICA  # Start as replica
        self.status = BrokerStatus.STARTING
        self.cluster_members: Dict[str, ClusterMember] = {}
        self.cluster_version = 0
        
        # Data manager for persistence
        db_filename = f"broker_{broker_id}_{cluster_id}.db"
        self.data_manager = BrokerDataManager(db_filename)
        
        # Network components
        self.server_socket = None
        self.server_thread = None
        self.running = False
        
        # Timing and election state
        self.last_heartbeat_sent = 0
        self.last_heartbeat_received = 0
        self.last_election_time = 0
        self.election_retry_count = 0
        self._election_in_progress = False
        
        # Connection tracking
        self.client_connections: Dict[str, socket.socket] = {}
        self.broker_connections: Dict[str, socket.socket] = {}
        
        # Threading locks
        self.state_lock = threading.RLock()
        self.election_lock = threading.Lock()
        
        print(f"Initialized broker {broker_id} in cluster {cluster_id} on {listen_host}:{listen_port}")
    
    def start(self):
        """Start the broker and begin operations."""
        print(f"Starting broker {self.broker_id}...")
        
        with self.state_lock:
            if self.running:
                return
            
            self.running = True
            self.status = BrokerStatus.STARTING
        
        # Start network server
        self._start_network_server()
        
        # Load any existing state
        self._load_existing_state()
        
        # Join cluster or become initial leader
        if self.seed_brokers:
            self._join_existing_cluster()
        else:
            self._become_initial_leader()
        
        # Start background threads
        self._start_background_threads()
        
        print(f"Broker {self.broker_id} started successfully as {self.role.value}")
    
    def stop(self):
        """Stop the broker and cleanup resources."""
        print(f"Stopping broker {self.broker_id}...")
        
        with self.state_lock:
            if not self.running:
                return
            
            self.running = False
            self.status = BrokerStatus.STOPPED
        
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
        
        # Close data manager
        self.data_manager.close()
        
        print(f"Broker {self.broker_id} stopped")
    
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
            
            print(f"Network server started on {self.listen_host}:{self.listen_port}")
            
        except Exception as e:
            print(f"Failed to start network server: {e}")
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
                    print(f"Server loop error: {e}")
                break
    
    def _handle_connection(self, client_socket: socket.socket, client_address: Tuple[str, int]):
        """Handle individual client/broker connection."""
        connection_id = f"{client_address[0]}:{client_address[1]}"
        
        try:
            while self.running:
                # Receive message
                message = self._receive_message(client_socket)
                if not message:
                    break
                
                # Process message and send response
                response = self._process_message(message, connection_id)
                if response:
                    self._send_message(client_socket, response)
        
        except Exception as e:
            if self.running:
                print(f"Connection {connection_id} error: {e}")
        
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
    
    def _receive_message(self, sock: socket.socket) -> Optional[Dict]:
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
            print(f"Failed to receive message: {e}")
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
    
    def _send_message(self, sock: socket.socket, message: Dict) -> bool:
        """Send length-prefixed JSON message."""
        try:
            json_bytes = json.dumps(message).encode('utf-8')
            length = struct.pack('>I', len(json_bytes))
            sock.sendall(length + json_bytes)
            return True
        except Exception as e:
            print(f"Failed to send message: {e}")
            return False
    
    def _process_message(self, message: Dict, connection_id: str) -> Optional[Dict]:
        """Process incoming message and return response."""
        operation = message.get("operation")
        
        try:
            # Client operations
            if operation == "CREATE_QUEUE":
                return self._handle_create_queue(message)
            elif operation == "APPEND":
                return self._handle_append_message(message)
            elif operation == "READ":
                return self._handle_read_message(message)
            elif operation == "CLUSTER_QUERY":
                return self._handle_cluster_query()
            
            # Broker-to-broker operations
            elif operation == "JOIN_CLUSTER":
                return self._handle_broker_join(message, connection_id)
            elif operation == "HEARTBEAT":
                return self._handle_heartbeat(message)
            elif operation == "REPLICATE":
                return self._handle_replication(message)
            elif operation == "MEMBERSHIP_UPDATE":
                return self._handle_membership_update(message)
            elif operation == "DATA_SYNC_REQUEST":
                return self._handle_data_sync_request(message)
            elif operation == "ELECTION_REQUEST":
                return self._handle_election_request(message)
            elif operation == "PROMOTE_TO_LEADER":
                return self._handle_leader_promotion(message)
            elif operation == "CLUSTER_UPDATE":
                return self._handle_cluster_update(message)
            
            else:
                return {"status": "error", "message": f"Unknown operation: {operation}"}
        
        except Exception as e:
            print(f"Error processing {operation}: {e}")
            return {"status": "error", "message": str(e)}
    
    # ================== CLIENT OPERATIONS ==================
    
    def _handle_create_queue(self, message: Dict) -> Dict:
        """Handle CREATE_QUEUE request."""
        if self.role != BrokerRole.LEADER:
            return {"status": "error", "message": "Only leader can create queues"}
        
        try:
            # Generate unique queue ID
            queue_id = generate_queue_id(self.cluster_id)
            
            # Create queue locally
            if not self.data_manager.create_queue(queue_id):
                return {"status": "error", "message": "Queue creation failed"}
            
            # Replicate to replicas
            replication_data = {
                "operation": "REPLICATE",
                "type": "CREATE_QUEUE",
                "queue_id": queue_id,
                "timestamp": time.time()
            }
            
            if self._replicate_to_replicas(replication_data):
                return {"status": "success", "queue_id": queue_id}
            else:
                # Rollback on replication failure
                # Note: In a real implementation, we'd need to remove the queue
                return {"status": "error", "message": "Replication failed"}
        
        except Exception as e:
            return {"status": "error", "message": f"Create queue failed: {e}"}
    
    def _handle_append_message(self, message: Dict) -> Dict:
        """Handle APPEND message request."""
        if self.role != BrokerRole.LEADER:
            return {"status": "error", "message": "Only leader can append messages"}
        
        queue_name = message.get("queue_name")
        data = message.get("data")
        
        if not queue_name or data is None:
            return {"status": "error", "message": "Missing queue_name or data"}
        
        try:
            # Validate queue exists
            if not self.data_manager.queue_exists(queue_name):
                return {"status": "error", "message": "Queue does not exist"}
            
            # Insert message locally
            sequence_num = self.data_manager.append_message(queue_name, data)
            
            # Replicate to replicas
            replication_data = {
                "operation": "REPLICATE",
                "type": "APPEND_MESSAGE",
                "queue_id": queue_name,
                "sequence_num": sequence_num,
                "data": data,
                "timestamp": time.time()
            }
            
            if self._replicate_to_replicas(replication_data):
                return {"status": "success", "sequence_num": sequence_num}
            else:
                # In a real implementation, we'd rollback the local message
                return {"status": "error", "message": "Replication failed"}
        
        except Exception as e:
            return {"status": "error", "message": f"Append failed: {e}"}
    
    def _handle_read_message(self, message: Dict) -> Dict:
        """Handle READ message request with strong consistency."""
        if self.role != BrokerRole.LEADER:
            return {"status": "error", "message": "Only leader can serve reads"}
        
        queue_name = message.get("queue_name")
        client_id = message.get("client_id")
        
        if not queue_name or not client_id:
            return {"status": "error", "message": "Missing queue_name or client_id"}
        
        try:
            # Validate queue exists
            if not self.data_manager.queue_exists(queue_name):
                return {"status": "error", "message": "Queue does not exist"}
            
            # Get client's current position
            current_position = self.data_manager.get_client_position(client_id, queue_name)
            
            # Fetch next message
            next_message = self.data_manager.get_next_message(queue_name, current_position)
            
            if next_message is None:
                return {"status": "success", "message": "no_messages"}
            
            sequence_num, data = next_message
            
            # Update position locally first
            self.data_manager.update_client_position(client_id, queue_name, sequence_num)
            
            # Replicate position update
            replication_data = {
                "operation": "REPLICATE",
                "type": "UPDATE_POSITION",
                "client_id": client_id,
                "queue_id": queue_name,
                "new_position": sequence_num,
                "timestamp": time.time()
            }
            
            if self._replicate_to_replicas(replication_data):
                return {
                    "status": "success",
                    "sequence_num": sequence_num,
                    "data": data
                }
            else:
                # Rollback position update
                self.data_manager.update_client_position(client_id, queue_name, current_position)
                return {"status": "error", "message": "Position replication failed"}
        
        except Exception as e:
            return {"status": "error", "message": f"Read failed: {e}"}
    
    def _handle_cluster_query(self) -> Dict:
        """Handle cluster information query."""
        with self.state_lock:
            members_info = []
            for member in self.cluster_members.values():
                members_info.append({
                    "broker_id": member.broker_id,
                    "host": member.host,
                    "port": member.port,
                    "role": member.role.value,
                    "status": member.status.value
                })
            
            return {
                "status": "success",
                "cluster_id": self.cluster_id,
                "cluster_version": self.cluster_version,
                "self_role": self.role.value,
                "members": members_info
            }
    
    # ================== REPLICATION ==================
    
    def _replicate_to_replicas(self, operation_data: Dict) -> bool:
        """Replicate operation to replica brokers, require majority consensus."""
        active_replicas = self._get_active_replicas()
        
        if not active_replicas:
            # No replicas, operation succeeds (single broker cluster)
            return True
        
        required_acks = len(active_replicas) // 2 + 1
        successful_acks = 0
        
        for replica_id in active_replicas:
            if self._send_to_broker(replica_id, operation_data):
                successful_acks += 1
        
        return successful_acks >= required_acks
    
    def _get_active_replicas(self) -> List[str]:
        """Get list of active replica broker IDs."""
        with self.state_lock:
            return [
                broker_id for broker_id, member in self.cluster_members.items()
                if (member.role == BrokerRole.REPLICA and 
                    member.status == BrokerStatus.ACTIVE and
                    broker_id != self.broker_id)
            ]
    
    def _send_to_broker(self, broker_id: str, message: Dict, timeout: float = 5.0) -> bool:
        """Send message to specific broker and wait for response."""
        if broker_id not in self.cluster_members:
            return False
        
        member = self.cluster_members[broker_id]
        
        # Skip sending to failed brokers to reduce noise
        if member.status == BrokerStatus.FAILED:
            return False
        
        try:
            # Create connection to broker
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect((member.host, member.port))
            
            # Send message
            if not self._send_message(sock, message):
                sock.close()
                return False
            
            # Receive response
            response = self._receive_message(sock)
            sock.close()
            
            return response and response.get("status") == "success"
        
        except Exception as e:
            # Only print error if the broker is still marked as active
            if member.status == BrokerStatus.ACTIVE:
                print(f"Failed to send to broker {broker_id}: {e}")
            return False
    
    def _handle_replication(self, message: Dict) -> Dict:
        """Handle replication message from leader."""
        if self.role != BrokerRole.REPLICA:
            return {"status": "error", "message": "Only replicas handle replication"}
        
        op_type = message.get("type")
        
        try:
            if op_type == "CREATE_QUEUE":
                queue_id = message.get("queue_id")
                if queue_id:
                    self.data_manager.create_queue(queue_id)
                
            elif op_type == "APPEND_MESSAGE":
                queue_id = message.get("queue_id")
                sequence_num = message.get("sequence_num")
                data = message.get("data")
                
                if queue_id and sequence_num is not None and data is not None:
                    # Insert with exact sequence number (no auto-increment)
                    with self.data_manager.transaction_lock:
                        cursor = self.data_manager.db_connection.cursor()
                        cursor.execute(
                            "INSERT OR REPLACE INTO queue_data (queue_id, sequence_num, data) VALUES (?, ?, ?)",
                            (queue_id, sequence_num, data)
                        )
                        self.data_manager.db_connection.commit()
                
            elif op_type == "UPDATE_POSITION":
                client_id = message.get("client_id")
                queue_id = message.get("queue_id")
                new_position = message.get("new_position")
                
                if client_id and queue_id and new_position is not None:
                    self.data_manager.update_client_position(client_id, queue_id, new_position)
            
            return {"status": "success"}
        
        except Exception as e:
            print(f"Replication failed: {e}")
            return {"status": "error", "message": str(e)}
    
    # ================== CLUSTER MANAGEMENT ==================
    
    def _load_existing_state(self):
        """Load any existing state from persistent storage."""
        try:
            state = self.data_manager.restore_broker_state()
            print(f"Loaded state: {len(state['queues'])} queues, {len(state['client_positions'])} client positions")
        except Exception as e:
            print(f"Failed to load existing state: {e}")
    
    def _join_existing_cluster(self):
        """Join existing cluster using seed brokers."""
        print(f"Joining cluster via seed brokers: {self.seed_brokers}")
        
        for seed_addr in self.seed_brokers:
            if self._try_join_via_seed(seed_addr):
                self.status = BrokerStatus.ACTIVE
                return
        
        print("Failed to join via any seed broker, becoming initial leader")
        self._become_initial_leader()
    
    def _try_join_via_seed(self, seed_addr: str) -> bool:
        """Attempt to join cluster via specific seed broker."""
        try:
            host, port = seed_addr.split(':')
            port = int(port)
            
            # Connect to seed broker
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10.0)
            sock.connect((host, port))
            
            # Send join request
            join_message = {
                "operation": "JOIN_CLUSTER",
                "broker_id": self.broker_id,
                "host": self.listen_host,
                "port": self.listen_port,
                "cluster_id": self.cluster_id
            }
            
            if not self._send_message(sock, join_message):
                sock.close()
                return False
            
            # Receive response
            response = self._receive_message(sock)
            sock.close()
            
            if response and response.get("status") == "success":
                # Update cluster membership
                cluster_info = response.get("cluster_info", {})
                data_snapshot = response.get("data_snapshot", {})
                
                self._update_cluster_membership_from_info(cluster_info)
                
                # Apply data snapshot for catch-up
                if data_snapshot:
                    self.data_manager.apply_data_snapshot(data_snapshot)
                
                print(f"Successfully joined cluster via {seed_addr}")
                return True
        
        except Exception as e:
            print(f"Failed to join via {seed_addr}: {e}")
        
        return False
    
    def _become_initial_leader(self):
        """Become initial leader of new cluster."""
        print(f"Becoming initial leader of cluster {self.cluster_id}")
        
        with self.state_lock:
            self.role = BrokerRole.LEADER
            self.status = BrokerStatus.ACTIVE
            self.cluster_version = 1
            
            # Add self to cluster membership
            self.cluster_members[self.broker_id] = ClusterMember(
                broker_id=self.broker_id,
                host=self.listen_host,
                port=self.listen_port,
                role=BrokerRole.LEADER,
                status=BrokerStatus.ACTIVE,
                last_heartbeat=time.time(),
                cluster_version=self.cluster_version
            )
    
    def _handle_broker_join(self, message: Dict, connection_id: str) -> Dict:
        """Handle JOIN_CLUSTER request from new broker."""
        if self.role != BrokerRole.LEADER:
            return {"status": "error", "message": "Only leader can handle join requests"}
        
        broker_id = message.get("broker_id")
        host = message.get("host")
        port = message.get("port")
        joining_cluster_id = message.get("cluster_id")
        
        if not all([broker_id, host, port, joining_cluster_id]):
            return {"status": "error", "message": "Missing required fields"}
        
        if joining_cluster_id != self.cluster_id:
            return {"status": "error", "message": "Cluster ID mismatch"}
        
        print(f"Broker {broker_id} joining cluster from {host}:{port}")
        
        with self.state_lock:
            # Add new member to cluster
            self.cluster_members[broker_id] = ClusterMember(
                broker_id=broker_id,
                host=host,
                port=port,
                role=BrokerRole.REPLICA,
                status=BrokerStatus.ACTIVE,
                last_heartbeat=time.time(),
                cluster_version=self.cluster_version
            )
            
            # Update cluster version
            self.cluster_version += 1
            
            # Prepare cluster info
            cluster_info = {
                "cluster_version": self.cluster_version,
                "members": {
                    mid: {
                        "broker_id": member.broker_id,
                        "host": member.host,
                        "port": member.port,
                        "role": member.role.value,
                        "status": member.status.value
                    }
                    for mid, member in self.cluster_members.items()
                }
            }
            
            # Get data snapshot for catch-up
            data_snapshot = self.data_manager.get_full_data_snapshot()
        
        # Broadcast updated cluster membership to all existing brokers (outside lock)
        self._broadcast_cluster_membership_update()
        
        return {
            "status": "success",
            "cluster_info": cluster_info,
            "data_snapshot": data_snapshot
        }
    
    def _update_cluster_membership_from_info(self, cluster_info: Dict):
        """Update cluster membership from received cluster info."""
        with self.state_lock:
            self.cluster_version = cluster_info.get("cluster_version", 0)
            self.cluster_members.clear()
            
            for member_info in cluster_info.get("members", {}).values():
                broker_id = member_info.get("broker_id")
                if broker_id:
                    self.cluster_members[broker_id] = ClusterMember(
                        broker_id=broker_id,
                        host=member_info.get("host"),
                        port=member_info.get("port"),
                        role=BrokerRole(member_info.get("role", "replica")),
                        status=BrokerStatus(member_info.get("status", "active")),
                        last_heartbeat=time.time(),
                        cluster_version=self.cluster_version
                    )
                    
                    # Update our own role if we find ourselves
                    if broker_id == self.broker_id:
                        self.role = BrokerRole(member_info.get("role", "replica"))
    
    # ================== HEARTBEAT AND FAILURE DETECTION ==================
    
    def _start_background_threads(self):
        """Start background threads for heartbeats and failure detection."""
        # Heartbeat sender thread
        heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat_thread.start()
        
        # Failure detection thread
        failure_thread = threading.Thread(target=self._failure_detection_loop, daemon=True)
        failure_thread.start()
    
    def _heartbeat_loop(self):
        """Send periodic heartbeats."""
        while self.running:
            try:
                if self.status == BrokerStatus.ACTIVE:
                    self._send_heartbeats()
                time.sleep(5.0)  # Send heartbeat every 5 seconds
            except Exception as e:
                if self.running:
                    print(f"Heartbeat loop error: {e}")
    
    def _send_heartbeats(self):
        """Send heartbeat to all other cluster members."""
        heartbeat_message = {
            "operation": "HEARTBEAT",
            "broker_id": self.broker_id,
            "timestamp": time.time(),
            "role": self.role.value,
            "cluster_version": self.cluster_version
        }
        
        with self.state_lock:
            other_members = [
                broker_id for broker_id in self.cluster_members.keys()
                if broker_id != self.broker_id
            ]
        
        for broker_id in other_members:
            self._send_to_broker(broker_id, heartbeat_message)
        
        self.last_heartbeat_sent = time.time()
    
    def _handle_heartbeat(self, message: Dict) -> Dict:
        """Handle heartbeat from another broker."""
        broker_id = message.get("broker_id")
        timestamp = message.get("timestamp", time.time())
        role = message.get("role")
        cluster_version = message.get("cluster_version", 0)
        
        if broker_id and broker_id in self.cluster_members:
            with self.state_lock:
                member = self.cluster_members[broker_id]
                member.last_heartbeat = timestamp
                member.cluster_version = cluster_version
                
                # Update role if provided
                if role:
                    member.role = BrokerRole(role)
        
        self.last_heartbeat_received = time.time()
        return {"status": "success"}
    
    def _failure_detection_loop(self):
        """Detect and handle broker failures."""
        while self.running:
            try:
                if self.status == BrokerStatus.ACTIVE:
                    self._detect_failures()
                time.sleep(3.0)  # Check every 3 seconds
            except Exception as e:
                if self.running:
                    print(f"Failure detection error: {e}")
    
    def _detect_failures(self):
        """Detect failed brokers based on heartbeat timeouts."""
        # Skip failure detection during elections to avoid interference
        if getattr(self, '_election_in_progress', False):
            return
            
        current_time = time.time()
        failed_brokers = []
        
        with self.state_lock:
            for broker_id, member in self.cluster_members.items():
                if (broker_id != self.broker_id and 
                    member.status == BrokerStatus.ACTIVE and  # Only check ACTIVE brokers
                    current_time - member.last_heartbeat > 8.0):  # 8 second timeout for faster detection
                    failed_brokers.append(broker_id)
        
        if failed_brokers:
            self._handle_broker_failures(failed_brokers)
    
    def _handle_broker_failures(self, failed_brokers: List[str]):
        """Handle detected broker failures."""
        print(f"Detected broker failures: {failed_brokers}")
        
        with self.state_lock:
            leader_failed = False
            
            for broker_id in failed_brokers:
                if broker_id in self.cluster_members:
                    member = self.cluster_members[broker_id]
                    if member.role == BrokerRole.LEADER:
                        leader_failed = True
                    
                    # Mark as failed but don't remove immediately
                    # This maintains cluster size for proper vote calculation
                    member.status = BrokerStatus.FAILED
            
            # Update cluster version to reflect status change
            if failed_brokers:
                self.cluster_version += 1
            
            # Only trigger election if leader failed AND we don't currently have an active leader
            if leader_failed and self.role == BrokerRole.REPLICA:
                # Check if we currently have an active leader
                has_active_leader = any(
                    member.role == BrokerRole.LEADER and member.status == BrokerStatus.ACTIVE
                    for member in self.cluster_members.values()
                )
                
                if not has_active_leader:
                    print("Leader failed, triggering election")
                    self._trigger_leader_election()
                else:
                    print("Leader failed, but new leader already active")
            elif self.role == BrokerRole.LEADER and failed_brokers:
                # Leader can remove failed brokers from membership
                for broker_id in failed_brokers:
                    if broker_id in self.cluster_members:
                        del self.cluster_members[broker_id]
                # Replicate membership update to remaining brokers
                self._replicate_membership_update()
    
    # ================== LEADER ELECTION ==================
    
    def _trigger_leader_election(self):
        """Trigger leader election process."""
        with self.election_lock:
            current_time = time.time()
            
            # Prevent rapid successive elections
            if current_time - self.last_election_time < 5.0:
                return
            
            # Add moderate staggered delay based on broker ID to prevent simultaneous elections
            # But keep delays reasonable so backup elections can happen quickly
            with self.state_lock:
                all_broker_ids = sorted(self.cluster_members.keys())
                try:
                    broker_position = all_broker_ids.index(self.broker_id)
                    # Shorter delays: B-AAA=0s, B-BBB=2s, B-CCC=4s, B-DDD=6s, B-EEE=8s
                    broker_delay = broker_position * 2.0  # Linear, not exponential
                except ValueError:
                    broker_delay = 5.0  # Default delay if not found
            
            def delayed_election():
                time.sleep(broker_delay)
                
                # Recheck if we still need an election after delay
                with self.state_lock:
                    has_leader = any(
                        member.role == BrokerRole.LEADER and member.status == BrokerStatus.ACTIVE
                        for member in self.cluster_members.values()
                    )
                    
                    if has_leader:
                        # Silently cancel election if leader already exists
                        return
                    
                    # Simple check - proceed if we're a viable candidate
                    active_brokers = self._get_active_broker_ids()
                    if not active_brokers or self.broker_id not in active_brokers:
                        print(f"Not in active brokers list, canceling election")
                        return
                
                # Proceed with election
                self.last_election_time = time.time()
                
                # Determine if we should be the candidate based on ALL cluster members
                # Use total cluster for candidate selection to avoid split decisions
                with self.state_lock:
                    all_broker_ids = sorted(self.cluster_members.keys())
                    # Remove brokers that are definitively failed (connection refused)
                    potentially_active = []
                    for bid in all_broker_ids:
                        member = self.cluster_members[bid]
                        if bid == self.broker_id:
                            potentially_active.append(bid)  # Self is always active
                        elif member.status != BrokerStatus.FAILED:
                            potentially_active.append(bid)  # Include non-failed brokers
                
                if not potentially_active:
                    return
                
                # Lowest ID becomes candidate - more deterministic selection
                candidate_id = min(potentially_active)
                
                print(f"Candidate selection: potentially_active={potentially_active}, selected={candidate_id}")
                
                if candidate_id == self.broker_id:
                    print(f"Starting election as candidate after {broker_delay:.1f}s delay")
                    self._conduct_election()
                else:
                    print(f"Waiting for candidate {candidate_id} to start election")
                    self._monitor_election(candidate_id)
            
            threading.Thread(target=delayed_election, daemon=True).start()
    
    def _get_active_broker_ids(self) -> List[str]:
        """Get list of active broker IDs, excluding failed brokers."""
        with self.state_lock:
            return [
                broker_id for broker_id, member in self.cluster_members.items()
                if member.status == BrokerStatus.ACTIVE
            ]
    
    def _send_election_request(self, broker_id: str, election_message: Dict) -> str:
        """Send election request and return vote result: 'granted', 'denied', or 'failed'."""
        if broker_id not in self.cluster_members:
            return "failed"
        
        member = self.cluster_members[broker_id]
        
        try:
            # Create connection to broker
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(3.0)  # Shorter timeout for elections
            sock.connect((member.host, member.port))
            
            # Send election request
            if not self._send_message(sock, election_message):
                sock.close()
                return "failed"
            
            # Receive vote response
            response = self._receive_message(sock)
            sock.close()
            
            if response:
                status = response.get("status")
                if status == "granted":
                    return "granted"
                elif status == "denied":
                    return "denied"
                else:
                    return "failed"
            else:
                return "failed"
        
        except Exception as e:
            # Only print election failures for active brokers 
            member = self.cluster_members.get(broker_id)
            if member and member.status == BrokerStatus.ACTIVE:
                print(f"Failed to get vote from broker {broker_id}: {e}")
            return "failed"
    
    def _conduct_election(self):
        """Conduct leader election as candidate."""
        print(f"Conducting election...")
        
        # Disable failure detection during election to avoid interference
        self._election_in_progress = True
        
        # Get current view of ALL brokers for election (including failed ones)
        # We need to contact everyone to get proper vote counts
        with self.state_lock:
            active_brokers = self._get_active_broker_ids()
            all_brokers = list(self.cluster_members.keys())
            other_brokers = [bid for bid in all_brokers if bid != self.broker_id]
            
            # Debug: Print cluster membership view
            print(f"[{self.broker_id}] Election cluster view: {all_brokers}")
            print(f"[{self.broker_id}] Will contact: {other_brokers}")
        
        election_message = {
            "operation": "ELECTION_REQUEST",
            "candidate_id": self.broker_id,
            "cluster_version": self.cluster_version + 1,
            "timestamp": time.time()
        }
        
        # Vote for self
        votes_received = 1
        successful_contacts = [self.broker_id]  # Track who we successfully contacted
        failed_contacts = []
        
        # Request votes from other brokers
        for broker_id in other_brokers:
            vote_response = self._send_election_request(broker_id, election_message)
            if vote_response == "granted":
                votes_received += 1
                successful_contacts.append(broker_id)
            elif vote_response == "denied":
                successful_contacts.append(broker_id)  # Contact successful but vote denied
            else:  # vote_response == "failed"
                failed_contacts.append(broker_id)
        
        # Calculate majority based on responsive brokers during election
        # This is more realistic than requiring votes from definitely failed brokers
        responsive_brokers = len(successful_contacts) + len(failed_contacts)
        
        if responsive_brokers == 1:
            # If only self is responsive, allow single-broker election
            required_votes = 1
        else:
            # Need majority of responsive brokers
            required_votes = (responsive_brokers // 2) + 1
        
        with self.state_lock:
            total_cluster_size = len(self.cluster_members)
        
        print(f"Election results: {votes_received}/{responsive_brokers} votes, need {required_votes}")
        print(f"Responsive brokers: {responsive_brokers}, Total cluster size: {total_cluster_size}")
        print(f"Votes granted by: {[bid for bid in successful_contacts if bid != self.broker_id]}")
        print(f"Failed contacts: {failed_contacts}")
        
        if votes_received >= required_votes:
            # Clean up failed brokers from our view before becoming leader
            if failed_contacts:
                self._handle_broker_failures(failed_contacts)
            self._become_leader([])
        else:
            print("Election failed, insufficient votes")
            # Clean up failed brokers even if election failed
            if failed_contacts:
                self._handle_broker_failures(failed_contacts)
            self._schedule_retry_election()
        
        # Re-enable failure detection after election
        self._election_in_progress = False
    
    def _become_leader(self, failed_brokers: List[str]):
        """Promote self to leader after winning election."""
        print(f"Won election, becoming leader")
        
        with self.state_lock:
            self.role = BrokerRole.LEADER
            self.cluster_version += 1
            
            # Update self in cluster membership
            if self.broker_id in self.cluster_members:
                self.cluster_members[self.broker_id].role = BrokerRole.LEADER
                self.cluster_members[self.broker_id].cluster_version = self.cluster_version
            
            # Remove failed brokers
            for broker_id in failed_brokers:
                if broker_id in self.cluster_members:
                    del self.cluster_members[broker_id]
        
        # Announce promotion to remaining brokers
        promotion_message = {
            "operation": "PROMOTE_TO_LEADER",
            "broker_id": self.broker_id,
            "cluster_version": self.cluster_version
        }
        
        active_replicas = self._get_active_replicas()
        for replica_id in active_replicas:
            self._send_to_broker(replica_id, promotion_message)
    
    def _handle_election_request(self, message: Dict) -> Dict:
        """Handle election request from candidate."""
        candidate_id = message.get("candidate_id")
        candidate_version = message.get("cluster_version", 0)
        election_timestamp = message.get("timestamp", 0)
        
        current_time = time.time()
        
        # Validation checks
        if self.role != BrokerRole.REPLICA:
            return {"status": "denied", "reason": "Not a replica"}
        
        # Check if we already have a leader
        with self.state_lock:
            has_active_leader = any(
                member.role == BrokerRole.LEADER and member.status == BrokerStatus.ACTIVE
                for member in self.cluster_members.values()
            )
            
            if has_active_leader:
                return {"status": "denied", "reason": "Already have active leader"}
            
            # Priority-based voting: prefer lower ID candidates
            # Check if there's a better candidate (lower ID) that might be active
            all_broker_ids = sorted(self.cluster_members.keys())
            for bid in all_broker_ids:
                if bid < candidate_id and bid != self.broker_id:
                    member = self.cluster_members.get(bid)
                    if member and member.status != BrokerStatus.FAILED:
                        # Don't vote for higher ID if lower ID might still be available
                        if current_time - member.last_heartbeat < 12.0:  # More lenient for priority
                            return {"status": "denied", "reason": f"Waiting for higher priority candidate {bid}"}
        
        if current_time - election_timestamp > 15.0:
            return {"status": "denied", "reason": "Election too old"}
        
        if current_time - self.last_election_time < 3.0:
            return {"status": "denied", "reason": "Too soon since last vote"}
        
        # Grant vote
        self.last_election_time = current_time
        print(f"Granted vote to candidate {candidate_id}")
        return {"status": "granted"}
    
    def _handle_leader_promotion(self, message: Dict) -> Dict:
        """Handle leader promotion announcement."""
        new_leader_id = message.get("broker_id")
        new_version = message.get("cluster_version", 0)
        
        if not new_leader_id or new_version <= self.cluster_version:
            return {"status": "error", "message": "Invalid promotion"}
        
        with self.state_lock:
            if new_leader_id in self.cluster_members:
                # Update cluster membership
                self.cluster_members[new_leader_id].role = BrokerRole.LEADER
                self.cluster_version = new_version
                
                # If we were leader, step down
                if self.role == BrokerRole.LEADER:
                    self.role = BrokerRole.REPLICA
                
                print(f"Accepted {new_leader_id} as new leader")
                return {"status": "success"}
        
        return {"status": "error", "message": "Unknown broker"}
    
    def _broadcast_cluster_membership_update(self):
        """Broadcast cluster membership update to all existing brokers."""
        with self.state_lock:
            cluster_info = {
                "cluster_version": self.cluster_version,
                "members": {
                    mid: {
                        "broker_id": member.broker_id,
                        "host": member.host,
                        "port": member.port,
                        "role": member.role.value,
                        "status": member.status.value
                    }
                    for mid, member in self.cluster_members.items()
                }
            }
            other_brokers = [bid for bid in self.cluster_members.keys() if bid != self.broker_id]
        
        # Send cluster update to all other brokers
        update_message = {
            "operation": "CLUSTER_UPDATE",
            "cluster_info": cluster_info
        }
        
        for broker_id in other_brokers:
            try:
                self._send_to_broker(broker_id, update_message)
            except Exception as e:
                print(f"Failed to send cluster update to {broker_id}: {e}")
    
    def _handle_cluster_update(self, message: Dict) -> Dict:
        """Handle cluster membership update from leader."""
        cluster_info = message.get("cluster_info", {})
        
        if cluster_info:
            print(f"Received cluster membership update, version {cluster_info.get('cluster_version', 0)}")
            self._update_cluster_membership_from_info(cluster_info)
            return {"status": "success"}
        else:
            return {"status": "error", "message": "No cluster info provided"}
    
    def _monitor_election(self, candidate_id: str):
        """Monitor election progress and trigger backup if needed."""
        # Wait for election result, but check periodically for candidate failure
        for i in range(6):  # Check 6 times over 6 seconds - faster detection
            time.sleep(1.0)
            
            # Check if candidate still exists and is responsive
            with self.state_lock:
                if candidate_id not in self.cluster_members:
                    print(f"Candidate {candidate_id} removed from cluster, triggering backup election")
                    self._trigger_leader_election()
                    return
                
                # Check if candidate is marked as failed
                candidate_member = self.cluster_members.get(candidate_id)
                if candidate_member and candidate_member.status == BrokerStatus.FAILED:
                    print(f"Candidate {candidate_id} marked as failed, triggering backup election")
                    self._trigger_leader_election()
                    return
                
                # Check if we have a leader
                has_leader = any(
                    member.role == BrokerRole.LEADER and member.status == BrokerStatus.ACTIVE
                    for member in self.cluster_members.values()
                )
                
                if has_leader:
                    # Election completed successfully - stop monitoring
                    return
        
        # Final timeout check
        with self.state_lock:
            has_leader = any(
                member.role == BrokerRole.LEADER and member.status == BrokerStatus.ACTIVE
                for member in self.cluster_members.values()
            )
            
            if not has_leader:
                print(f"Election timeout after monitoring {candidate_id}, triggering backup election")
                self._trigger_leader_election()
    
    def _schedule_retry_election(self):
        """Schedule retry election with random delay."""
        delay = random.uniform(5.0, 10.0)
        
        def retry_election():
            time.sleep(delay)
            if self._still_need_leader():
                self._trigger_leader_election()
        
        threading.Thread(target=retry_election, daemon=True).start()
    
    def _still_need_leader(self) -> bool:
        """Check if we still need a leader."""
        with self.state_lock:
            return not any(
                member.role == BrokerRole.LEADER and member.status == BrokerStatus.ACTIVE
                for member in self.cluster_members.values()
            )
    
    def get_current_leader(self) -> str:
        """Get the current leader's broker ID."""
        with self.state_lock:
            for broker_id, member in self.cluster_members.items():
                if member.role == BrokerRole.LEADER and member.status == BrokerStatus.ACTIVE:
                    return broker_id
            return None
    
    # ================== UTILITY METHODS ==================
    
    def _handle_membership_update(self, message: Dict) -> Dict:
        """Handle membership update from leader."""
        # Implementation for handling membership updates
        return {"status": "success"}
    
    def _handle_data_sync_request(self, message: Dict) -> Dict:
        """Handle data sync request from joining broker."""
        if self.role != BrokerRole.LEADER:
            return {"status": "error", "message": "Only leader can provide data sync"}
        
        data_snapshot = self.data_manager.get_full_data_snapshot()
        return {
            "status": "success",
            "data_snapshot": data_snapshot
        }
    
    def _replicate_membership_update(self):
        """Replicate membership changes to replicas."""
        membership_data = {
            "operation": "MEMBERSHIP_UPDATE",
            "cluster_version": self.cluster_version,
            "members": {
                broker_id: {
                    "broker_id": member.broker_id,
                    "host": member.host,
                    "port": member.port,
                    "role": member.role.value,
                    "status": member.status.value
                }
                for broker_id, member in self.cluster_members.items()
            }
        }
        
        self._replicate_to_replicas(membership_data)

