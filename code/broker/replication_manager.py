#!/usr/bin/env python3
"""
ReplicationManager - Handles data replication between leader and replicas.
"""

import time
from typing import Dict, List, Optional

from .types import BrokerRole, BrokerStatus


class ReplicationManager:
    """
    Manages data replication between leader and replica brokers.
    """
    
    def __init__(self, broker_id: str, network_handler, cluster_manager, data_manager):
        """Initialize replication manager."""
        self.broker_id = broker_id
        self.network_handler = network_handler
        self.cluster_manager = cluster_manager
        self.data_manager = data_manager
        
        print(f"[{self.broker_id}] ReplicationManager initialized")
    
    def replicate_to_replicas(self, operation_data: Dict) -> bool:
        """Replicate operation to replica brokers, require majority consensus."""
        active_replicas = self.cluster_manager.get_active_replicas()
        
        if not active_replicas:
            # No replicas, operation succeeds (single broker cluster)
            return True
        
        required_acks = len(active_replicas) // 2 + 1
        successful_acks = 0
        
        for replica_id in active_replicas:
            if self._send_to_replica(replica_id, operation_data):
                successful_acks += 1
        
        return successful_acks >= required_acks
    
    def _send_to_replica(self, replica_id: str, message: Dict, timeout: float = 5.0) -> bool:
        """Send message to specific replica and wait for response."""
        if replica_id not in self.cluster_manager.cluster_members:
            return False
        
        member = self.cluster_manager.cluster_members[replica_id]
        
        # Failed brokers are removed from cluster, so no need to check
        
        try:
            response = self.network_handler.send_to_broker(member.host, member.port, message, timeout)
            return response and response.get("status") == "success"
        
        except Exception as e:
            if member.status == BrokerStatus.ACTIVE:
                print(f"[{self.broker_id}] Failed to send to replica {replica_id}: {e}")
            return False
    
    def handle_replication(self, message: Dict) -> Dict:
        """Handle replication message from leader."""
        if self.cluster_manager.role != BrokerRole.REPLICA:
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
                    # Insert with exact sequence number
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
            print(f"[{self.broker_id}] Replication failed: {e}")
            return {"status": "error", "message": str(e)}
    
    def create_queue_with_replication(self, queue_id: str) -> Dict:
        """Create queue and replicate to all replicas."""
        if self.cluster_manager.role != BrokerRole.LEADER:
            return {"status": "error", "message": "Only leader can create queues"}
        
        try:
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
            
            if self.replicate_to_replicas(replication_data):
                return {"status": "success", "queue_id": queue_id}
            else:
                # Rollback on replication failure
                return {"status": "error", "message": "Replication failed"}
        
        except Exception as e:
            return {"status": "error", "message": f"Create queue failed: {e}"}
    
    def append_message_with_replication(self, queue_name: str, data: str) -> Dict:
        """Append message and replicate to all replicas."""
        if self.cluster_manager.role != BrokerRole.LEADER:
            return {"status": "error", "message": "Only leader can append messages"}
        
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
            
            if self.replicate_to_replicas(replication_data):
                return {"status": "success", "sequence_num": sequence_num}
            else:
                return {"status": "error", "message": "Replication failed"}
        
        except Exception as e:
            return {"status": "error", "message": f"Append failed: {e}"}
    
    def read_message_with_replication(self, queue_name: str, client_id: str) -> Dict:
        """Read message and replicate position update."""
        if self.cluster_manager.role != BrokerRole.LEADER:
            return {"status": "error", "message": "Only leader can serve reads"}
        
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
            
            if self.replicate_to_replicas(replication_data):
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
    
    def replicate_membership_update(self):
        """Replicate membership changes to replicas."""
        membership_data = {
            "operation": "MEMBERSHIP_UPDATE",
            "cluster_version": self.cluster_manager.cluster_version,
            "members": {
                broker_id: {
                    "broker_id": member.broker_id,
                    "host": member.host,
                    "port": member.port,
                    "role": member.role.value,
                    "status": member.status.value
                }
                for broker_id, member in self.cluster_manager.cluster_members.items()
            }
        }
        
        self.replicate_to_replicas(membership_data)
    
    def handle_data_sync_request(self, message: Dict) -> Dict:
        """Handle data sync request from joining broker."""
        if self.cluster_manager.role != BrokerRole.LEADER:
            return {"status": "error", "message": "Only leader can provide data sync"}
        
        data_snapshot = self.data_manager.get_full_data_snapshot()
        return {
            "status": "success",
            "data_snapshot": data_snapshot
        }
    
    def handle_membership_update(self, message: Dict) -> Dict:
        """Handle membership update from leader."""
        # Implementation for handling membership updates
        return {"status": "success"}
