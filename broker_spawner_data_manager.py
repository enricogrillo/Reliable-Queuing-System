#!/usr/bin/env python3
"""
BrokerSpawnerDataManager - Handles persistence of BrokerSpawner configuration
"""

import json
import os
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
import threading


@dataclass
class PersistedBrokerConfig:
    """Configuration for a persisted broker."""
    broker_id: str
    cluster_id: str
    host: str
    port: int
    seed_brokers: List[str]
    start_time: float


@dataclass
class PersistedSpawnerState:
    """Complete persisted state of the BrokerSpawner."""
    brokers: Dict[str, PersistedBrokerConfig]
    clusters: Dict[str, List[str]]
    next_port: int
    last_cluster_id: Optional[str]
    custom_ip_aliases: Dict[str, str]
    timestamp: float


class BrokerSpawnerDataManager:
    """Manages persistence of BrokerSpawner configuration."""
    
    def __init__(self, data_file: str = None, enabled: bool = True):
        """
        Initialize the data manager.
        
        Args:
            data_file: Path to the persistence file. If None, uses default location.
            enabled: Whether persistence is enabled for this session.
        """
        self.enabled = enabled
        if data_file is None:
            # Use data directory for persistence files
            data_dir = os.path.join(os.path.dirname(__file__), "data")
            os.makedirs(data_dir, exist_ok=True)
            data_file = os.path.join(data_dir, "broker_spawner_state.json")
        self.data_file = data_file
        self._lock = threading.Lock()
    
    def save_state(self, spawner) -> bool:
        """
        Save the current spawner state to disk.
        
        Args:
            spawner: BrokerSpawner instance to save state from
            
        Returns:
            True if save was successful, False otherwise
        """
        if not self.enabled:
            return True  # Silently succeed when disabled
        
        try:
            with self._lock:
                # Convert BrokerInstance objects to PersistedBrokerConfig
                # Save all broker configurations, not just running ones
                persisted_brokers = {}
                for broker_id, broker_instance in spawner.brokers.items():
                    # Extract seed brokers from the broker object
                    seed_brokers = []
                    if hasattr(broker_instance.broker, 'seed_brokers'):
                        seed_brokers = broker_instance.broker.seed_brokers[:]
                    
                    persisted_brokers[broker_id] = PersistedBrokerConfig(
                        broker_id=broker_instance.broker_id,
                        cluster_id=broker_instance.cluster_id,
                        host=broker_instance.host,
                        port=broker_instance.port,
                        seed_brokers=seed_brokers,
                        start_time=broker_instance.start_time
                    )
                
                # Create the state object
                state = PersistedSpawnerState(
                    brokers=persisted_brokers,
                    clusters=spawner.clusters.copy(),
                    next_port=spawner.next_port,
                    last_cluster_id=spawner.last_cluster_id,
                    custom_ip_aliases=spawner.ip_manager.custom_aliases.copy(),
                    timestamp=time.time()
                )
                
                # Convert to dict for JSON serialization
                state_dict = asdict(state)
                
                # Write to file atomically
                temp_file = self.data_file + ".tmp"
                with open(temp_file, 'w') as f:
                    json.dump(state_dict, f, indent=2, sort_keys=True)
                
                # Atomic move
                os.rename(temp_file, self.data_file)
                
                return True
                
        except Exception as e:
            print(f"Warning: Failed to save spawner state: {e}")
            return False
    
    def load_state(self) -> Optional[PersistedSpawnerState]:
        """
        Load the spawner state from disk.
        
        Returns:
            PersistedSpawnerState if loaded successfully, None otherwise
        """
        if not self.enabled:
            return None
        
        try:
            with self._lock:
                if not os.path.exists(self.data_file):
                    return None
                
                with open(self.data_file, 'r') as f:
                    state_dict = json.load(f)
                
                # Convert broker configs back from dict
                persisted_brokers = {}
                for broker_id, broker_dict in state_dict.get('brokers', {}).items():
                    persisted_brokers[broker_id] = PersistedBrokerConfig(**broker_dict)
                
                # Create the state object
                state = PersistedSpawnerState(
                    brokers=persisted_brokers,
                    clusters=state_dict.get('clusters', {}),
                    next_port=state_dict.get('next_port', 9001),
                    last_cluster_id=state_dict.get('last_cluster_id'),
                    custom_ip_aliases=state_dict.get('custom_ip_aliases', {}),
                    timestamp=state_dict.get('timestamp', 0)
                )
                
                return state
                
        except Exception as e:
            print(f"Warning: Failed to load spawner state: {e}")
            return None
    
    def clear_state(self) -> bool:
        """
        Clear the persisted state file.
        
        Returns:
            True if cleared successfully, False otherwise
        """
        if not self.enabled:
            return True
        
        try:
            with self._lock:
                if os.path.exists(self.data_file):
                    os.remove(self.data_file)
                return True
        except Exception as e:
            print(f"Warning: Failed to clear spawner state: {e}")
            return False
    
    def get_state_info(self) -> str:
        """
        Get information about the current state file.
        
        Returns:
            Human-readable string with state file information
        """
        if not self.enabled:
            return "Persistence disabled for this session"
        
        try:
            if not os.path.exists(self.data_file):
                return f"No state file found at {self.data_file}"
            
            stat = os.stat(self.data_file)
            size = stat.st_size
            mtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stat.st_mtime))
            
            # Try to load and get basic info
            state = self.load_state()
            if state:
                broker_count = len(state.brokers)
                cluster_count = len(state.clusters)
                state_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(state.timestamp))
                return (f"State file: {self.data_file}\n"
                       f"Size: {size} bytes, Modified: {mtime}\n"
                       f"Contains: {broker_count} brokers, {cluster_count} clusters\n"
                       f"State timestamp: {state_time}")
            else:
                return f"State file exists but failed to load: {self.data_file}"
                
        except Exception as e:
            return f"Error getting state info: {e}"
