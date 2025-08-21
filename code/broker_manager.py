from __future__ import annotations

import threading
import time
from typing import Dict, List, Optional, Tuple

from .broker import Broker
from .http_client import HttpJsonClient


class BrokerManager:
    """Manager for spawning and controlling broker instances."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._brokers: Dict[str, Broker] = {}
        self._groups: Dict[str, List[str]] = {}
        self._next_broker_id = 1

    # =============================================================================
    # Properties
    # =============================================================================

    @property
    def broker_count(self) -> int:
        """Get total number of managed brokers."""
        return len(self._brokers)

    @property
    def group_names(self) -> List[str]:
        """Get list of all group names."""
        return list(self._groups.keys())

    def get_broker_names(self, group_name: Optional[str] = None) -> List[str]:
        """Get list of broker names in a group (or all if no group specified)."""
        with self._lock:
            if group_name:
                return self._groups.get(group_name, []).copy()
            return list(self._brokers.keys())

    def get_broker_urls(self, group_name: Optional[str] = None) -> List[str]:
        """Get URLs of all brokers in a group (or all brokers if no group specified)."""
        with self._lock:
            broker_names = self.get_broker_names(group_name)
            return [self._brokers[name].self_url for name in broker_names if name in self._brokers]

    # =============================================================================
    # Broker Lifecycle Management
    # =============================================================================

    def spawn_broker(self, address: str, port: int, is_leader: bool = False, 
                    group_name: str = "default") -> str:
        """Spawn a new broker instance."""
        with self._lock:
            # Generate unique broker ID
            broker_id = self._next_broker_id
            self._next_broker_id += 1
            broker_name = f"broker-{broker_id}"
            
            # Check for address conflicts
            self._validate_address_availability(address, port)
            
            # Create and start broker
            broker = Broker(broker_id, address, port, is_leader)
            broker.start_http_server()
            
            # Track broker
            self._brokers[broker_name] = broker
            self._add_to_group(broker_name, group_name)
            
            print(f"✓ Spawned {broker_name} at http://{address}:{port} ({'leader' if is_leader else 'follower'}) in group '{group_name}'")
            return broker_name

    def stop_broker(self, broker_name: str) -> bool:
        """Stop a specific broker."""
        with self._lock:
            if broker_name not in self._brokers:
                return False
            
            broker = self._brokers[broker_name]
            self._shutdown_broker(broker)
            
            # Remove from tracking
            self._remove_from_tracking(broker_name)
            
            print(f"✓ Stopped broker '{broker_name}'")
            return True

    def stop_all_brokers(self) -> None:
        """Stop all managed brokers."""
        with self._lock:
            broker_names = list(self._brokers.keys())
            for broker_name in broker_names:
                self.stop_broker(broker_name)
            self._groups.clear()

    def restart_broker(self, broker_name: str) -> bool:
        """Restart a specific broker."""
        with self._lock:
            if broker_name not in self._brokers:
                return False
            
            # Save current configuration
            broker = self._brokers[broker_name]
            config = self._get_broker_config(broker_name)
            
            # Stop and restart
            self.stop_broker(broker_name)
            time.sleep(0.1)
            self.spawn_broker(config["address"], config["port"], config["is_leader"], config["group"])
            return True

    def simulate_crash(self, broker_name: str) -> bool:
        """Simulate a broker crash by stopping it abruptly."""
        with self._lock:
            if broker_name not in self._brokers:
                return False
            
            broker = self._brokers[broker_name]
            
            # Abruptly stop without graceful shutdown
            broker._hb_stop.set()
            broker._monitor_stop.set()
            broker.stop_http_server()
            
            # Remove from tracking but don't call close() to simulate crash
            self._remove_from_tracking(broker_name)
            
            print(f"💥 Simulated crash for broker '{broker_name}'")
            return True

    # =============================================================================
    # Group Management
    # =============================================================================

    def spawn_group(self, group_name: str, configs: List[Tuple[str, int, bool]]) -> List[str]:
        """Spawn a group of brokers with automatic replication setup."""
        broker_names = []
        
        # Spawn all brokers
        for address, port, is_leader in configs:
            broker_name = self.spawn_broker(address, port, is_leader, group_name)
            broker_names.append(broker_name)
        
        # Set up replication if multiple brokers
        if len(broker_names) > 1:
            self.setup_replication(group_name)
        
        return broker_names

    def setup_replication(self, group_name: str) -> bool:
        """Set up replication relationships within a group."""
        with self._lock:
            if group_name not in self._groups:
                return False
            
            broker_names = self._groups[group_name]
            if len(broker_names) < 2:
                print(f"Warning: Group '{group_name}' has only {len(broker_names)} broker(s), replication not needed")
                return False
            
            # Find leader and followers
            leader_broker, follower_brokers = self._categorize_brokers(broker_names)
            
            if not leader_broker:
                print(f"Warning: No leader found in group '{group_name}'")
                return False
            
            # Configure replication relationships
            self._configure_replication(broker_names, leader_broker, follower_brokers)
            
            print(f"✓ Set up replication for group '{group_name}' (1 leader, {len(follower_brokers)} followers)")
            return True

    def start_cluster_tasks(self, group_name: str, proxy_url: Optional[str] = None) -> bool:
        """Start cluster tasks for all brokers in a group."""
        with self._lock:
            if group_name not in self._groups:
                return False
            
            broker_names = self._groups[group_name]
            for broker_name in broker_names:
                if broker_name in self._brokers:
                    broker = self._brokers[broker_name]
                    broker.start_cluster_tasks(proxy_url)
            
            print(f"✓ Started cluster tasks for group '{group_name}' ({len(broker_names)} brokers)")
            return True

    # =============================================================================
    # Status and Information
    # =============================================================================

    def get_broker_status(self, broker_name: str) -> Dict:
        """Get status information for a specific broker."""
        with self._lock:
            if broker_name not in self._brokers:
                return {"error": f"Broker '{broker_name}' not found"}
            
            broker = self._brokers[broker_name]
            try:
                return self._fetch_broker_info(broker_name, broker)
            except Exception as e:
                return {
                    "name": broker_name,
                    "url": broker.self_url,
                    "status": "error",
                    "error": str(e)
                }

    def get_all_broker_status(self) -> Dict[str, Dict]:
        """Get status for all managed brokers organized by group."""
        with self._lock:
            result = {}
            for group_name, broker_names in self._groups.items():
                result[group_name] = {}
                for broker_name in broker_names:
                    if broker_name in self._brokers:
                        result[group_name][broker_name] = self.get_broker_status(broker_name)
            return result

    def broker_exists(self, broker_name: str) -> bool:
        """Check if a broker exists."""
        return broker_name in self._brokers

    def group_exists(self, group_name: str) -> bool:
        """Check if a group exists."""
        return group_name in self._groups

    # =============================================================================
    # Private Helper Methods
    # =============================================================================

    def _validate_address_availability(self, address: str, port: int) -> None:
        """Check if address is already in use."""
        for existing_broker in self._brokers.values():
            if existing_broker.ip == address and existing_broker.port == port:
                raise ValueError(f"Address {address}:{port} already in use")

    def _add_to_group(self, broker_name: str, group_name: str) -> None:
        """Add broker to a group."""
        if group_name not in self._groups:
            self._groups[group_name] = []
        self._groups[group_name].append(broker_name)

    def _remove_from_tracking(self, broker_name: str) -> None:
        """Remove broker from all tracking structures."""
        del self._brokers[broker_name]
        
        # Remove from groups
        for group_brokers in self._groups.values():
            if broker_name in group_brokers:
                group_brokers.remove(broker_name)

    def _shutdown_broker(self, broker: Broker) -> None:
        """Gracefully shutdown a broker."""
        broker.stop_cluster_tasks()
        broker.stop_http_server()
        broker.close()

    def _get_broker_config(self, broker_name: str) -> Dict:
        """Get configuration for a broker."""
        broker = self._brokers[broker_name]
        
        # Find group
        group_name = "default"
        for gname, brokers in self._groups.items():
            if broker_name in brokers:
                group_name = gname
                break
        
        return {
            "address": broker.ip,
            "port": broker.port,
            "is_leader": broker.is_leader,
            "group": group_name
        }

    def _categorize_brokers(self, broker_names: List[str]) -> Tuple[Optional[Broker], List[Broker]]:
        """Separate leader and followers from broker names."""
        leader_broker = None
        follower_brokers = []
        
        for name in broker_names:
            broker = self._brokers[name]
            if broker.is_leader:
                leader_broker = broker
            else:
                follower_brokers.append(broker)
        
        return leader_broker, follower_brokers

    def _configure_replication(self, broker_names: List[str], leader_broker: Broker, 
                             follower_brokers: List[Broker]) -> None:
        """Configure replication relationships between brokers."""
        # Build URL lists
        peer_urls = [self._brokers[name].self_url for name in broker_names]
        follower_urls = [broker.self_url for broker in follower_brokers]
        
        # Configure leader
        leader_broker.set_follower_urls(follower_urls)
        leader_broker.set_peer_urls(peer_urls)
        
        # Configure followers
        for broker in follower_brokers:
            broker.set_peer_urls(peer_urls)

    def _fetch_broker_info(self, broker_name: str, broker: Broker) -> Dict:
        """Fetch comprehensive information about a broker."""
        client = HttpJsonClient(timeout=2)
        
        # Get broker data
        info = client.post_json(broker.self_url, "/broker_info", {})
        queues = client.post_json(broker.self_url, "/get_all_queues", {})
        messages = client.post_json(broker.self_url, "/get_all_messages", {})
        pointers = client.post_json(broker.self_url, "/get_all_pointers", {})
        
        return {
            "name": broker_name,
            "url": broker.self_url,
            "is_leader": info.get("is_leader", False),
            "broker_id": info.get("broker_id"),
            "queue_count": len(queues.get("queue_ids", [])),
            "message_count": len(messages.get("messages", [])),
            "client_count": len(set(p["client_id"] for p in pointers.get("pointers", []))),
            "queues": queues.get("queue_ids", []),
            "messages": messages.get("messages", []),
            "pointers": pointers.get("pointers", []),
            "status": "running"
        }