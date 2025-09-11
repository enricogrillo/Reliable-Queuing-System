#!/usr/bin/env python3
"""
ReplicationManager - Handles data replication between leader and replicas.
"""

import time
from typing import Dict

# Add broker directory to path for local imports
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
from broker_types import BrokerRole, BrokerStatus


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
        
        # print(f"[{self.broker_id}] ReplicationManager initialized")
    
    def replicate_to_replicas(self, operation_data: Dict) -> bool:
        """Replicate operation to replica brokers, require ALL replicas to acknowledge."""
        active_replicas = self.cluster_manager.get_active_replicas()
        
        if not active_replicas:
            # No replicas, operation succeeds (single broker cluster)
            return True
        
        successful_acks = 0
        failed_replicas = []
        
        for replica_id in active_replicas:
            if self._send_to_replica(replica_id, operation_data):
                successful_acks += 1
            else:
                failed_replicas.append(replica_id)
        
        # Require ALL active replicas to acknowledge
        if successful_acks == len(active_replicas):
            return True
        else:
            print(f"[{self.broker_id}] Replication failed: {successful_acks}/{len(active_replicas)} acks, failed replicas: {failed_replicas}")
            return False
    
    def _send_to_replica(self, replica_id: str, message: Dict, timeout: float = 3.0) -> bool:
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
            
            elif op_type == "ROLLBACK_QUEUE":
                queue_id = message.get("queue_id")
                if queue_id:
                    self.data_manager.rollback_queue_creation(queue_id)
            
            elif op_type == "ROLLBACK_MESSAGE":
                queue_id = message.get("queue_id")
                sequence_num = message.get("sequence_num")
                
                if queue_id and sequence_num is not None:
                    with self.data_manager.transaction_lock:
                        cursor = self.data_manager.db_connection.cursor()
                        cursor.execute(
                            "DELETE FROM queue_data WHERE queue_id = ? AND sequence_num = ?",
                            (queue_id, sequence_num)
                        )
                        self.data_manager.db_connection.commit()
            
            elif op_type == "ROLLBACK_POSITION":
                client_id = message.get("client_id")
                queue_id = message.get("queue_id")
                old_position = message.get("old_position")
                
                if client_id and queue_id and old_position is not None:
                    self.data_manager.update_client_position(client_id, queue_id, old_position)
            
            return {"status": "success"}
        
        except Exception as e:
            print(f"[{self.broker_id}] Replication failed: {e}")
            return {"status": "error", "message": str(e)}
    
    def create_queue_with_replication(self, queue_id: str) -> Dict:
        """Create queue and replicate to all replicas."""
        if self.cluster_manager.role != BrokerRole.LEADER:
            return {"status": "error", "message": "Only leader can create queues"}
        
        try:
            # Create queue locally first
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
                # Rollback on replication failure - remove queue locally
                print(f"[{self.broker_id}] Rolling back queue creation {queue_id} due to replication failure")
                self.data_manager.rollback_queue_creation(queue_id)
                
                # Send rollback to replicas that are still in cluster
                self._send_rollback_to_replicas("ROLLBACK_QUEUE", {"queue_id": queue_id})
                
                return {"status": "error", "message": "Replication failed"}
        
        except Exception as e:
            return {"status": "error", "message": f"Create queue failed: {e}"}
    
    def append_message_with_replication(self, queue_name: str, data: int) -> Dict:
        """Append message and replicate to all replicas."""
        if self.cluster_manager.role != BrokerRole.LEADER:
            return {"status": "error", "message": "Only leader can append messages"}
        
        try:
            # Validate queue exists
            if not self.data_manager.queue_exists(queue_name):
                return {"status": "error", "message": "Queue does not exist"}
            
            # Insert message locally first
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
                # Rollback on replication failure - remove message locally
                print(f"[{self.broker_id}] Rolling back message append {sequence_num} in queue {queue_name} due to replication failure")
                with self.data_manager.transaction_lock:
                    cursor = self.data_manager.db_connection.cursor()
                    cursor.execute(
                        "DELETE FROM queue_data WHERE queue_id = ? AND sequence_num = ?",
                        (queue_name, sequence_num)
                    )
                    self.data_manager.db_connection.commit()
                
                # Send rollback to replicas that are still in cluster
                self._send_rollback_to_replicas("ROLLBACK_MESSAGE", {
                    "queue_id": queue_name,
                    "sequence_num": sequence_num
                })
                
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
                # Rollback position update locally
                print(f"[{self.broker_id}] Rolling back position update for client {client_id} in queue {queue_name} due to replication failure")
                self.data_manager.update_client_position(client_id, queue_name, current_position)
                
                # Send rollback to replicas that are still in cluster
                self._send_rollback_to_replicas("ROLLBACK_POSITION", {
                    "client_id": client_id,
                    "queue_id": queue_name,
                    "old_position": current_position
                })
                
                return {"status": "error", "message": "Position replication failed"}
        
        except Exception as e:
            return {"status": "error", "message": f"Read failed: {e}"}

    
    def _send_rollback_to_replicas(self, rollback_type: str, rollback_data: Dict) -> bool:
        """Send rollback message to replicas still in cluster and wait for acknowledgments."""
        active_replicas = self.cluster_manager.get_active_replicas()
        
        if not active_replicas:
            return True  # No replicas to rollback
        
        rollback_message = {
            "operation": "REPLICATE",
            "type": rollback_type,
            "timestamp": time.time(),
            **rollback_data
        }
        
        successful_rollback_acks = 0
        failed_replicas = []
        
        for replica_id in active_replicas:
            if self._send_to_replica(replica_id, rollback_message):
                successful_rollback_acks += 1
            else:
                failed_replicas.append(replica_id)
        
        if successful_rollback_acks == len(active_replicas):
            print(f"[{self.broker_id}] Rollback {rollback_type} completed successfully on all replicas")
            return True
        else:
            print(f"[{self.broker_id}] Rollback {rollback_type} failed on some replicas: {failed_replicas}")
            return False

    def handle_data_sync_request(self, message: Dict) -> Dict:
        """Handle data sync request from joining broker."""
        if self.cluster_manager.role != BrokerRole.LEADER:
            return {"status": "error", "message": "Only leader can provide data sync"}
        
        data_snapshot = self.data_manager.get_full_data_snapshot()
        return {
            "status": "success",
            "data_snapshot": data_snapshot
        }
