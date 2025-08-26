#!/usr/bin/env python3
"""
Broker2 Implementation - Refactored with Helper Classes
Based on the original broker.py but organized with helper classes for better maintainability.
"""

import json
import time
import threading
from typing import Dict, List, Optional, Tuple, Any

from .broker_data_manager import BrokerDataManager
from .id_generator import generate_queue_id
from .network_handler import NetworkHandler
from .cluster_manager import ClusterManager
from .leader_election import LeaderElection
from .replication_manager import ReplicationManager
from .types import BrokerRole, BrokerStatus, ClusterMember


class Broker:
    """
    Refactored broker implementation using helper classes.
    
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
        self.running = False
        
        # Data manager for persistence
        db_filename = f"broker_{broker_id}_{cluster_id}.db"
        self.data_manager = BrokerDataManager(db_filename)
        
        # Initialize helper components
        self.network_handler = NetworkHandler(
            broker_id, listen_host, listen_port, self._process_message
        )
        
        self.cluster_manager = ClusterManager(
            broker_id, cluster_id, listen_host, listen_port,
            self.network_handler, self.data_manager, self._on_leader_failure
        )
        
        self.leader_election = LeaderElection(
            broker_id, self.network_handler, self.cluster_manager
        )
        
        self.replication_manager = ReplicationManager(
            broker_id, self.network_handler, self.cluster_manager, self.data_manager
        )
        
        print(f"[{self.broker_id}] Initialized broker {broker_id} in cluster {cluster_id} on {listen_host}:{listen_port}")
    
    def start(self):
        """Start the broker and begin operations."""
        print(f"[{self.broker_id}] Starting broker {self.broker_id}...")
        
        if self.running:
            return
        
        self.running = True
        
        # Start network handler
        self.network_handler.start()
        
        # Load any existing state
        self._load_existing_state()
        
        # Start cluster manager
        self.cluster_manager.start(self.seed_brokers)
        
        print(f"[{self.broker_id}] Broker {self.broker_id} started successfully as {self.cluster_manager.role.value}")
    
    def stop(self):
        """Stop the broker and cleanup resources."""
        print(f"[{self.broker_id}] Stopping broker {self.broker_id}...")
        
        if not self.running:
            return
        
        self.running = False
        
        # Stop components
        self.cluster_manager.stop()
        self.network_handler.stop()
        
        # Close data manager
        self.data_manager.close()
        
        print(f"[{self.broker_id}] Broker {self.broker_id} stopped")
    
    def _load_existing_state(self):
        """Load any existing state from persistent storage."""
        try:
            state = self.data_manager.restore_broker_state()
            print(f"[{self.broker_id}] Loaded state: {len(state['queues'])} queues, {len(state['client_positions'])} client positions")
        except Exception as e:
            print(f"[{self.broker_id}] Failed to load existing state: {e}")
    
    def _on_leader_failure(self):
        """Handle leader failure by triggering election."""
        self.leader_election.trigger_leader_election()
    
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
                return self.cluster_manager.handle_broker_join(message)
            elif operation == "HEARTBEAT":
                return self.cluster_manager.handle_heartbeat(message)
            elif operation == "REPLICATE":
                return self.replication_manager.handle_replication(message)
            elif operation == "MEMBERSHIP_UPDATE":
                return self.replication_manager.handle_membership_update(message)
            elif operation == "DATA_SYNC_REQUEST":
                return self.replication_manager.handle_data_sync_request(message)
            elif operation == "ELECTION_REQUEST":
                return self.leader_election.handle_election_request(message)
            elif operation == "PROMOTE_TO_LEADER":
                return self.leader_election.handle_leader_promotion(message)
            elif operation == "CLUSTER_UPDATE":
                return self.cluster_manager.handle_cluster_update(message)
            
            else:
                return {"status": "error", "message": f"Unknown operation: {operation}"}
        
        except Exception as e:
            print(f"[{self.broker_id}] Error processing {operation}: {e}")
            return {"status": "error", "message": str(e)}
    
    # ================== CLIENT OPERATIONS ==================
    
    def _handle_create_queue(self, message: Dict) -> Dict:
        """Handle CREATE_QUEUE request."""
        # Generate unique queue ID
        queue_id = generate_queue_id(self.cluster_id)
        
        # Use replication manager to create and replicate
        return self.replication_manager.create_queue_with_replication(queue_id)
    
    def _handle_append_message(self, message: Dict) -> Dict:
        """Handle APPEND message request."""
        queue_name = message.get("queue_name")
        data = message.get("data")
        
        if not queue_name or data is None:
            return {"status": "error", "message": "Missing queue_name or data"}
        
        # Use replication manager to append and replicate
        return self.replication_manager.append_message_with_replication(queue_name, data)
    
    def _handle_read_message(self, message: Dict) -> Dict:
        """Handle READ message request with strong consistency."""
        queue_name = message.get("queue_name")
        client_id = message.get("client_id")
        
        if not queue_name or not client_id:
            return {"status": "error", "message": "Missing queue_name or client_id"}
        
        # Use replication manager to read and replicate position update
        return self.replication_manager.read_message_with_replication(queue_name, client_id)
    
    def _handle_cluster_query(self) -> Dict:
        """Handle cluster information query."""
        cluster_info = self.cluster_manager.get_cluster_info()
        return {
            "status": "success",
            **cluster_info,
            "members": cluster_info["brokers"]  # Keep backward compatibility
        }
    
    # ================== UTILITY METHODS ==================
    
    @property
    def role(self) -> BrokerRole:
        """Get current broker role."""
        return self.cluster_manager.role
    
    @property
    def status(self) -> BrokerStatus:
        """Get current broker status."""
        return self.cluster_manager.status
    
    def get_current_leader(self) -> Optional[str]:
        """Get the current leader's broker ID."""
        return self.cluster_manager.get_current_leader()
    
    def get_cluster_info(self) -> Dict:
        """Get detailed cluster information."""
        return self.cluster_manager.get_cluster_info()
    
    def get_connection_stats(self) -> Dict:
        """Get network connection statistics."""
        return self.network_handler.get_connection_count()
    
    def is_leader(self) -> bool:
        """Check if this broker is the leader."""
        return self.cluster_manager.role == BrokerRole.LEADER
    
    def is_replica(self) -> bool:
        """Check if this broker is a replica."""
        return self.cluster_manager.role == BrokerRole.REPLICA
    
    def is_active(self) -> bool:
        """Check if this broker is active."""
        return self.cluster_manager.status == BrokerStatus.ACTIVE
    
    def get_cluster_size(self) -> int:
        """Get the total number of brokers in the cluster."""
        return len(self.cluster_manager.cluster_members)
    
    def get_active_replicas_count(self) -> int:
        """Get the number of active replica brokers."""
        return len(self.cluster_manager.get_active_replicas())
    
    def force_leader_election(self):
        """Force a leader election (for testing/debugging)."""
        if self.cluster_manager.role == BrokerRole.REPLICA:
            self.leader_election.trigger_leader_election()
        else:
            print(f"[{self.broker_id}] Cannot trigger election - not a replica")
    
    def get_health_status(self) -> Dict:
        """Get comprehensive health status of the broker."""
        return {
            "broker_id": self.broker_id,
            "cluster_id": self.cluster_id,
            "role": self.cluster_manager.role.value,
            "status": self.cluster_manager.status.value,
            "running": self.running,
            "cluster_size": self.get_cluster_size(),
            "active_replicas": self.get_active_replicas_count(),
            "current_leader": self.get_current_leader(),
            "cluster_version": self.cluster_manager.cluster_version,
            "connection_stats": self.get_connection_stats(),
            "election_in_progress": self.leader_election.is_election_in_progress()
        }
