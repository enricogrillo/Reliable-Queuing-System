#!/usr/bin/env python3
"""
ClusterManager - Handles cluster membership, joining, heartbeats, and failure detection.
"""

import time
import threading
import sys
import os
from typing import Dict, List, Optional, Callable

# Add broker directory to path for local imports
sys.path.insert(0, os.path.dirname(__file__))
from broker_types import BrokerRole, BrokerStatus, ClusterMember


class ClusterManager:
    """
    Manages cluster membership, broker joining, heartbeats, and failure detection.
    """
    
    def __init__(self, broker_id: str, cluster_id: str, listen_host: str, listen_port: int, 
                 network_handler, data_manager, on_leader_failure: Callable):
        """Initialize cluster manager."""
        self.broker_id = broker_id
        self.cluster_id = cluster_id
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.network_handler = network_handler
        self.data_manager = data_manager
        self.on_leader_failure = on_leader_failure
        
        # Cluster state
        self.cluster_members: Dict[str, ClusterMember] = {}
        self.cluster_version = 0
        self.role = BrokerRole.REPLICA
        self.status = BrokerStatus.STARTING
        
        # Timing
        self.last_heartbeat_sent = 0
        self.last_heartbeat_received = 0
        
        # Threading
        self.state_lock = threading.RLock()
        self.running = False
        
        print(f"[{self.broker_id}] ClusterManager initialized")
    
    def start(self, seed_brokers: List[str] = None):
        """Start cluster manager operations."""
        if self.running:
            return
            
        self.running = True
        print(f"[{self.broker_id}] Starting cluster manager...")
        
        # Join cluster or become initial leader
        if seed_brokers:
            self._join_existing_cluster(seed_brokers)
        else:
            self._become_initial_leader()
        
        # Start background threads
        self._start_background_threads()
    
    def stop(self):
        """Stop cluster manager operations."""
        if not self.running:
            return
            
        print(f"[{self.broker_id}] Stopping cluster manager...")
        self.running = False
        
        with self.state_lock:
            # Remove self from cluster when stopping
            if self.broker_id in self.cluster_members:
                del self.cluster_members[self.broker_id]
    
    def _join_existing_cluster(self, seed_brokers: List[str]):
        """Join existing cluster using seed brokers."""
        print(f"[{self.broker_id}] Joining cluster via seed brokers: {seed_brokers}")
        
        for seed_addr in seed_brokers:
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
            
            # Send join request
            join_message = {
                "operation": "JOIN_CLUSTER",
                "broker_id": self.broker_id,
                "host": self.listen_host,
                "port": self.listen_port,
                "cluster_id": self.cluster_id
            }
            
            response = self.network_handler.send_to_broker(host, port, join_message, timeout=10.0)
            
            if response and response.get("status") == "success":
                # Update cluster membership
                cluster_info = response.get("cluster_info", {})
                data_snapshot = response.get("data_snapshot", {})
                
                self._update_cluster_membership_from_info(cluster_info)
                
                # Apply data snapshot for catch-up
                if data_snapshot:
                    self.data_manager.apply_data_snapshot(data_snapshot)
                
                print(f"[{self.broker_id}] Successfully joined cluster via {seed_addr}")
                return True
        
        except Exception as e:
            print(f"[{self.broker_id}] Failed to join via {seed_addr}: {e}")
        
        return False
    
    def _become_initial_leader(self):
        """Become initial leader of new cluster."""
        print(f"[{self.broker_id}] Becoming initial leader of cluster {self.cluster_id}")
        
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
    
    def handle_broker_join(self, message: Dict) -> Dict:
        """Handle JOIN_CLUSTER request from new broker."""
        # If we're not the leader, forward the request to the leader
        if self.role != BrokerRole.LEADER:
            return self._forward_join_to_leader(message)
        
        broker_id = message.get("broker_id")
        host = message.get("host")
        port = message.get("port")
        joining_cluster_id = message.get("cluster_id")
        
        if not all([broker_id, host, port, joining_cluster_id]):
            return {"status": "error", "message": "Missing required fields"}
        
        if joining_cluster_id != self.cluster_id:
            return {"status": "error", "message": "Cluster ID mismatch"}
        
        print(f"[{self.broker_id}] Broker {broker_id} joining cluster from {host}:{port}")
        
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
        
        # Broadcast updated cluster membership to all existing brokers
        self._broadcast_cluster_membership_update()
        
        return {
            "status": "success",
            "cluster_info": cluster_info,
            "data_snapshot": data_snapshot
        }
    
    def _forward_join_to_leader(self, message: Dict) -> Dict:
        """Forward JOIN_CLUSTER request to the current leader."""
        current_leader_id = self.get_current_leader()
        
        if not current_leader_id:
            return {"status": "error", "message": "No leader available to forward join request"}
        
        if current_leader_id == self.broker_id:
            return {"status": "error", "message": "Cannot forward to self - inconsistent state"}
        
        with self.state_lock:
            leader_member = self.cluster_members.get(current_leader_id)
        
        if not leader_member:
            return {"status": "error", "message": "Leader information not available"}
        
        print(f"[{self.broker_id}] Forwarding JOIN_CLUSTER request from {message.get('broker_id')} to leader {current_leader_id}")
        
        try:
            # Forward the original join message to the leader
            response = self.network_handler.send_to_broker(
                leader_member.host, 
                leader_member.port, 
                message, 
                timeout=15.0
            )
            
            if response:
                print(f"[{self.broker_id}] Successfully forwarded join request, leader response: {response.get('status')}")
                return response
            else:
                return {"status": "error", "message": "Failed to get response from leader"}
        
        except Exception as e:
            print(f"[{self.broker_id}] Failed to forward join request to leader: {e}")
            return {"status": "error", "message": f"Failed to forward to leader: {str(e)}"}
    
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
                    print(f"[{self.broker_id}] Heartbeat loop error: {e}")
    
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
                (self.cluster_members[bid].host, self.cluster_members[bid].port)
                for bid in self.cluster_members.keys()
                if bid != self.broker_id
            ]
        
        for host, port in other_members:
            self.network_handler.send_to_broker(host, port, heartbeat_message)
        
        self.last_heartbeat_sent = time.time()
    
    def handle_heartbeat(self, message: Dict) -> Dict:
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
                    print(f"[{self.broker_id}] Failure detection error: {e}")
    
    def _detect_failures(self):
        """Detect failed brokers based on heartbeat timeouts."""
        current_time = time.time()
        failed_brokers = []
        
        with self.state_lock:
            for broker_id, member in self.cluster_members.items():
                if (broker_id != self.broker_id and 
                    member.status == BrokerStatus.ACTIVE and
                    current_time - member.last_heartbeat > 8.0):
                    failed_brokers.append(broker_id)
        
        if failed_brokers:
            self._handle_broker_failures(failed_brokers)
    
    def _handle_broker_failures(self, failed_brokers: List[str]):
        """Handle detected broker failures."""
        print(f"[{self.broker_id}] Detected broker failures: {failed_brokers}")
        
        with self.state_lock:
            leader_failed = False
            
            for broker_id in failed_brokers:
                if broker_id in self.cluster_members:
                    member = self.cluster_members[broker_id]
                    if member.role == BrokerRole.LEADER:
                        leader_failed = True
                    
                    # Remove failed broker from cluster immediately
                    print(f"[{self.broker_id}] Removing failed broker {broker_id} from cluster")
                    del self.cluster_members[broker_id]
            
            # Update cluster version
            if failed_brokers:
                self.cluster_version += 1
            
            # Handle leader failure
            if leader_failed and self.role == BrokerRole.REPLICA:
                has_active_leader = any(
                    member.role == BrokerRole.LEADER
                    for member in self.cluster_members.values()
                )
                
                if not has_active_leader:
                    print("Leader failed, triggering election")
                    self.on_leader_failure()
            elif self.role == BrokerRole.LEADER and failed_brokers:
                # Broadcast cluster update after removing failed brokers
                self._broadcast_cluster_membership_update()
    
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
                member = self.cluster_members[broker_id]
                self.network_handler.send_to_broker(member.host, member.port, update_message)
            except Exception as e:
                print(f"[{self.broker_id}] Failed to send cluster update to {broker_id}: {e}")
    
    def handle_cluster_update(self, message: Dict) -> Dict:
        """Handle cluster membership update from leader."""
        cluster_info = message.get("cluster_info", {})
        
        if cluster_info:
            print(f"[{self.broker_id}] Received cluster membership update, version {cluster_info.get('cluster_version', 0)}")
            self._update_cluster_membership_from_info(cluster_info)
            return {"status": "success"}
        else:
            return {"status": "error", "message": "No cluster info provided"}
    
    def get_cluster_info(self) -> Dict:
        """Get current cluster information."""
        with self.state_lock:
            brokers_info = []
            for member in self.cluster_members.values():
                brokers_info.append({
                    "broker_id": member.broker_id,
                    "host": member.host,
                    "port": member.port,
                    "role": member.role.value,
                    "status": member.status.value
                })
            
            return {
                "cluster_id": self.cluster_id,
                "cluster_version": self.cluster_version,
                "self_role": self.role.value,
                "brokers": brokers_info
            }
    
    def get_current_leader(self) -> Optional[str]:
        """Get the current leader's broker ID."""
        with self.state_lock:
            for broker_id, member in self.cluster_members.items():
                if member.role == BrokerRole.LEADER and member.status == BrokerStatus.ACTIVE:
                    return broker_id
            return None
    
    def get_active_replicas(self) -> List[str]:
        """Get list of active replica broker IDs."""
        with self.state_lock:
            return [
                broker_id for broker_id, member in self.cluster_members.items()
                if (member.role == BrokerRole.REPLICA and 
                    member.status == BrokerStatus.ACTIVE and
                    broker_id != self.broker_id)
            ]
    
    def promote_to_leader(self):
        """Promote this broker to leader."""
        with self.state_lock:
            self.role = BrokerRole.LEADER
            self.cluster_version += 1
            
            # Update self in cluster membership
            if self.broker_id in self.cluster_members:
                self.cluster_members[self.broker_id].role = BrokerRole.LEADER
                self.cluster_members[self.broker_id].cluster_version = self.cluster_version
        
        print(f"[{self.broker_id}] Promoted to leader")
    
    def demote_to_replica(self):
        """Demote this broker to replica."""
        with self.state_lock:
            if self.role == BrokerRole.LEADER:
                self.role = BrokerRole.REPLICA
        
        print(f"[{self.broker_id}] Demoted to replica")
