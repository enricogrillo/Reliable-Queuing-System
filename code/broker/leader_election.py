#!/usr/bin/env python3
"""
LeaderElection - Handles leader election process and voting.
"""

import time
import random
import threading
from typing import Dict, List, Optional, Callable

from .types import BrokerRole, BrokerStatus


class LeaderElection:
    """
    Manages leader election process, voting, and leader promotion.
    """
    
    def __init__(self, broker_id: str, network_handler, cluster_manager):
        """Initialize leader election handler."""
        self.broker_id = broker_id
        self.network_handler = network_handler
        self.cluster_manager = cluster_manager
        
        # Election state
        self.last_election_time = 0
        self.election_retry_count = 0
        self._election_in_progress = False
        
        # Threading
        self.election_lock = threading.Lock()
        
        print(f"[{self.broker_id}] LeaderElection initialized")
    
    def trigger_leader_election(self):
        """Trigger leader election process."""
        with self.election_lock:
            current_time = time.time()
            
            # Prevent rapid successive elections
            if current_time - self.last_election_time < 5.0:
                return
            
            # Add staggered delay based on broker ID
            all_broker_ids = sorted(self.cluster_manager.cluster_members.keys())
            try:
                broker_position = all_broker_ids.index(self.broker_id)
                broker_delay = broker_position * 2.0
            except ValueError:
                broker_delay = 5.0
            
            def delayed_election():
                time.sleep(broker_delay)
                
                # Recheck if we still need an election after delay
                if self.cluster_manager.get_current_leader():
                    return
                
                active_brokers = self._get_active_broker_ids()
                if not active_brokers or self.broker_id not in active_brokers:
                    return
                
                self.last_election_time = time.time()
                
                # Determine candidate
                potentially_active = self._get_potentially_active_brokers()
                if not potentially_active:
                    return
                
                candidate_id = min(potentially_active)
                
                print(f"[{self.broker_id}] Candidate selection: potentially_active={potentially_active}, selected={candidate_id}")
                
                if candidate_id == self.broker_id:
                    print(f"[{self.broker_id}] Starting election as candidate after {broker_delay:.1f}s delay")
                    self._conduct_election()
                else:
                    print(f"[{self.broker_id}] Waiting for candidate {candidate_id} to start election")
                    self._monitor_election(candidate_id)
            
            threading.Thread(target=delayed_election, daemon=True).start()
    
    def _get_active_broker_ids(self) -> List[str]:
        """Get list of active broker IDs."""
        return [
            broker_id for broker_id, member in self.cluster_manager.cluster_members.items()
            if member.status == BrokerStatus.ACTIVE
        ]
    
    def _get_potentially_active_brokers(self) -> List[str]:
        """Get potentially active brokers for candidate selection."""
        all_broker_ids = sorted(self.cluster_manager.cluster_members.keys())
        potentially_active = []
        
        for bid in all_broker_ids:
            member = self.cluster_manager.cluster_members[bid]
            if bid == self.broker_id:
                potentially_active.append(bid)  # Self is always active
            else:
                # Quick connectivity check
                if self._quick_connectivity_check(bid):
                    potentially_active.append(bid)
        
        return potentially_active
    
    def _quick_connectivity_check(self, broker_id: str) -> bool:
        """Quick connectivity check for candidate selection."""
        if broker_id not in self.cluster_manager.cluster_members:
            return False
        
        member = self.cluster_manager.cluster_members[broker_id]
        try:
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            sock.connect((member.host, member.port))
            sock.close()
            return True
        except:
            return False
    
    def _conduct_election(self):
        """Conduct leader election as candidate."""
        print(f"[{self.broker_id}] Conducting election...")
        
        # Disable failure detection during election
        self._election_in_progress = True
        
        # Get current view of all brokers
        all_brokers = list(self.cluster_manager.cluster_members.keys())
        other_brokers = [bid for bid in all_brokers if bid != self.broker_id]
        
        print(f"[{self.broker_id}] Election cluster view: {all_brokers}")
        print(f"[{self.broker_id}] Will contact: {other_brokers}")
        
        election_message = {
            "operation": "ELECTION_REQUEST",
            "candidate_id": self.broker_id,
            "cluster_version": self.cluster_manager.cluster_version + 1,
            "timestamp": time.time()
        }
        
        # Vote for self
        votes_received = 1
        successful_contacts = [self.broker_id]
        failed_contacts = []
        
        # Request votes from other brokers
        for broker_id in other_brokers:
            vote_response = self._send_election_request(broker_id, election_message)
            if vote_response == "granted":
                votes_received += 1
                successful_contacts.append(broker_id)
            elif vote_response == "denied":
                successful_contacts.append(broker_id)
            else:  # failed
                failed_contacts.append(broker_id)
        
        # Calculate majority based on responsive brokers
        responsive_brokers = len(successful_contacts) + len(failed_contacts)
        
        if responsive_brokers == 1:
            required_votes = 1
        else:
            required_votes = (responsive_brokers // 2) + 1
        
        total_cluster_size = len(self.cluster_manager.cluster_members)
        
        print(f"[{self.broker_id}] Election results: {votes_received}/{responsive_brokers} votes, need {required_votes}")
        print(f"[{self.broker_id}] Responsive brokers: {responsive_brokers}, Total cluster size: {total_cluster_size}")
        
        if votes_received >= required_votes:
            # Clean up failed brokers before becoming leader
            if failed_contacts:
                self.cluster_manager._handle_broker_failures(failed_contacts)
            self._become_leader([])
        else:
            print("Election failed, insufficient votes")
            if failed_contacts:
                self.cluster_manager._handle_broker_failures(failed_contacts)
            self._schedule_retry_election()
        
        # Re-enable failure detection
        self._election_in_progress = False
    
    def _send_election_request(self, broker_id: str, election_message: Dict) -> str:
        """Send election request and return vote result."""
        if broker_id not in self.cluster_manager.cluster_members:
            return "failed"
        
        member = self.cluster_manager.cluster_members[broker_id]
        
        try:
            response = self.network_handler.send_to_broker(
                member.host, member.port, election_message, timeout=3.0
            )
            
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
            print(f"[{self.broker_id}] Failed to get vote from broker {broker_id}: {e}")
            return "failed"
    
    def _become_leader(self, failed_brokers: List[str]):
        """Promote self to leader after winning election."""
        print(f"[{self.broker_id}] Won election, becoming leader")
        
        # Promote to leader in cluster manager
        self.cluster_manager.promote_to_leader()
        
        # Remove failed brokers
        for broker_id in failed_brokers:
            if broker_id in self.cluster_manager.cluster_members:
                del self.cluster_manager.cluster_members[broker_id]
        
        # Announce promotion to remaining brokers
        promotion_message = {
            "operation": "PROMOTE_TO_LEADER",
            "broker_id": self.broker_id,
            "cluster_version": self.cluster_manager.cluster_version
        }
        
        active_replicas = self.cluster_manager.get_active_replicas()
        for replica_id in active_replicas:
            member = self.cluster_manager.cluster_members[replica_id]
            self.network_handler.send_to_broker(member.host, member.port, promotion_message)
    
    def handle_election_request(self, message: Dict) -> Dict:
        """Handle election request from candidate."""
        candidate_id = message.get("candidate_id")
        candidate_version = message.get("cluster_version", 0)
        election_timestamp = message.get("timestamp", 0)
        
        current_time = time.time()
        
        # Validation checks
        if self.cluster_manager.role != BrokerRole.REPLICA:
            return {"status": "denied", "reason": "Not a replica"}
        
        # Check if we already have a leader
        if self.cluster_manager.get_current_leader():
            return {"status": "denied", "reason": "Already have active leader"}
        
        # Priority-based voting: prefer lower ID candidates
        all_broker_ids = sorted(self.cluster_manager.cluster_members.keys())
        for bid in all_broker_ids:
            if bid < candidate_id and bid != self.broker_id:
                member = self.cluster_manager.cluster_members.get(bid)
                if member:
                    if current_time - member.last_heartbeat < 12.0:
                        return {"status": "denied", "reason": f"Waiting for higher priority candidate {bid}"}
        
        if current_time - election_timestamp > 15.0:
            return {"status": "denied", "reason": "Election too old"}
        
        if current_time - self.last_election_time < 3.0:
            return {"status": "denied", "reason": "Too soon since last vote"}
        
        # Grant vote
        self.last_election_time = current_time
        print(f"[{self.broker_id}] Granted vote to candidate {candidate_id}")
        return {"status": "granted"}
    
    def handle_leader_promotion(self, message: Dict) -> Dict:
        """Handle leader promotion announcement."""
        new_leader_id = message.get("broker_id")
        new_version = message.get("cluster_version", 0)
        
        if not new_leader_id or new_version <= self.cluster_manager.cluster_version:
            return {"status": "error", "message": "Invalid promotion"}
        
        if new_leader_id in self.cluster_manager.cluster_members:
            # Update cluster membership
            self.cluster_manager.cluster_members[new_leader_id].role = BrokerRole.LEADER
            self.cluster_manager.cluster_version = new_version
            
            # If we were leader, step down
            if self.cluster_manager.role == BrokerRole.LEADER:
                self.cluster_manager.demote_to_replica()
            
            print(f"[{self.broker_id}] Accepted {new_leader_id} as new leader")
            return {"status": "success"}
        
        return {"status": "error", "message": "Unknown broker"}
    
    def _monitor_election(self, candidate_id: str):
        """Monitor election progress and trigger backup if needed."""
        for i in range(6):  # Check 6 times over 6 seconds
            time.sleep(1.0)
            
            # Check if candidate still exists and is responsive
            if candidate_id not in self.cluster_manager.cluster_members:
                print(f"[{self.broker_id}] Candidate {candidate_id} removed from cluster, triggering backup election")
                self.trigger_leader_election()
                return
            
            # Candidate check is handled by removal from cluster on failure
            # so if candidate is in cluster_members, it's still viable
            
            # Check if we have a leader
            if self.cluster_manager.get_current_leader():
                return
        
        # Final timeout check
        if not self.cluster_manager.get_current_leader():
            print(f"[{self.broker_id}] Election timeout after monitoring {candidate_id}, triggering backup election")
            self.trigger_leader_election()
    
    def _schedule_retry_election(self):
        """Schedule retry election with random delay."""
        delay = random.uniform(5.0, 10.0)
        
        def retry_election():
            time.sleep(delay)
            if self._still_need_leader():
                self.trigger_leader_election()
        
        threading.Thread(target=retry_election, daemon=True).start()
    
    def _still_need_leader(self) -> bool:
        """Check if we still need a leader."""
        return not self.cluster_manager.get_current_leader()
    
    def is_election_in_progress(self) -> bool:
        """Check if election is currently in progress."""
        return self._election_in_progress
