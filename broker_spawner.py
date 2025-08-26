#!/usr/bin/env python3
"""
Broker Spawner CLI - Interactive tool for spawning and managing broker clusters
Based on cli/broker_spawner_documentation.md
"""

import os
import sys
import socket
import threading
import time
import argparse
from typing import Dict, List, Optional, Tuple
import json

# Add code directory to path
sys.path.insert(0, 'code')
from code.broker import Broker
from code.id_generator import generate_broker_id, generate_cluster_id
from code.common_cli import BaseCLI
from broker_spawner_data_manager import BrokerSpawnerDataManager


class BrokerInstance:
    """Represents a running broker instance."""
    
    def __init__(self, broker_id: str, cluster_id: str, host: str, port: int, broker: Broker):
        self.broker_id = broker_id
        self.cluster_id = cluster_id
        self.host = host
        self.port = port
        self.broker = broker
        self.thread = None
        self.start_time = time.time()
        self.running = False
    
    def start(self):
        """Start the broker in a separate thread."""
        if self.running:
            return
        
        def run_broker():
            try:
                self.broker.start()
                self.running = True
                # Keep the broker running
                while self.running:
                    time.sleep(0.1)
            except Exception as e:
                print(f"Broker {self.broker_id} error: {e}")
            finally:
                self.running = False
        
        self.thread = threading.Thread(target=run_broker, daemon=True)
        self.thread.start()
        
        # Give broker a moment to start
        time.sleep(0.1)
    
    def is_alive(self) -> bool:
        """Check if broker is still running."""
        return self.running and (self.thread is not None and self.thread.is_alive())
    
    def stop(self):
        """Stop the broker."""
        if self.running:
            self.running = False
            try:
                self.broker.stop()
            except:
                pass
        
        if self.thread and self.thread.is_alive():
            # Give thread time to finish
            self.thread.join(timeout=2.0)


class BrokerSpawner(BaseCLI):
    """Main broker spawner CLI application."""
    
    def __init__(self, enable_persistence: bool = True):
        super().__init__(".broker_spawner_history")
        self.brokers: Dict[str, BrokerInstance] = {}  # broker_id -> BrokerInstance
        self.clusters: Dict[str, List[str]] = {}      # cluster_id -> [broker_ids]
        self.next_port = 9001
        self.last_cluster_id = None  # Cache last cluster ID for convenience commands
        
        # Initialize data manager for persistence
        self.data_manager = BrokerSpawnerDataManager(enabled=enable_persistence)
        self.persistence_enabled = enable_persistence
        
        # Load previous state if available
        self._load_state()
        
        # Auto-start default cluster if no brokers are running
        self._auto_start_cluster()
    

    
    def _load_state(self):
        """Load previous spawner state if available."""
        if not self.persistence_enabled:
            return
        
        try:
            state = self.data_manager.load_state()
            if not state:
                return
            
            print(f"Loading previous state ({len(state.brokers)} brokers, {len(state.clusters)} clusters)...")
            
            # Restore IP aliases
            self.ip_manager.custom_aliases.update(state.custom_ip_aliases)
            
            # Restore next port
            self.next_port = state.next_port
            
            # Restore last cluster ID
            self.last_cluster_id = state.last_cluster_id
            
            # Restore clusters dict structure first
            self.clusters = state.clusters.copy()
            
            # Restore brokers - need to recreate them
            restored_count = 0
            failed_count = 0
            
            for broker_id, broker_config in state.brokers.items():
                try:
                    # Check if port is still available
                    if self._is_port_in_use(broker_config.port):
                        print(f"Warning: Port {broker_config.port} for broker {broker_id} is in use, skipping")
                        failed_count += 1
                        continue
                    
                    # Create broker instance (register=False since we're handling registration manually)
                    broker_instance = self._create_broker_instance(
                        broker_config.broker_id,
                        broker_config.cluster_id,
                        broker_config.host,
                        broker_config.port,
                        broker_config.seed_brokers,
                        register=False
                    )
                    
                    if broker_instance:
                        # Restore start time
                        broker_instance.start_time = broker_config.start_time
                        # Register broker manually
                        self.brokers[broker_id] = broker_instance
                        restored_count += 1
                    else:
                        failed_count += 1
                        
                except Exception as e:
                    print(f"Warning: Failed to restore broker {broker_id}: {e}")
                    failed_count += 1
            
            # Clean up clusters that have no running brokers
            clusters_to_remove = []
            for cluster_id, broker_ids in self.clusters.items():
                alive_brokers = [bid for bid in broker_ids if bid in self.brokers and self.brokers[bid].is_alive()]
                if not alive_brokers:
                    clusters_to_remove.append(cluster_id)
                    print(f"Removing empty cluster {cluster_id}")
                else:
                    # Update cluster with only alive brokers
                    self.clusters[cluster_id] = alive_brokers
            
            for cluster_id in clusters_to_remove:
                del self.clusters[cluster_id]
            
            if restored_count > 0:
                print(f"Restored {restored_count} brokers")
            if failed_count > 0:
                print(f"Failed to restore {failed_count} brokers")
            
            # Reset last_cluster_id if its cluster was removed
            if self.last_cluster_id and self.last_cluster_id not in self.clusters:
                self.last_cluster_id = None
                
        except Exception as e:
            print(f"Warning: Failed to load previous state: {e}")
    
    def _save_state(self):
        """Save current spawner state."""
        if self.persistence_enabled:
            self.data_manager.save_state(self)
    
    def _auto_start_cluster(self):
        """Auto-start clusters from saved configuration if no brokers are running."""
        if not self.persistence_enabled:
            return
        
        # Check if any brokers are currently running
        running_brokers = [bid for bid in self.brokers.keys() if self.brokers[bid].is_alive()]
        
        if not running_brokers and self.clusters:
            print("No running brokers found but clusters exist in configuration. Restoring from saved state...")
            
            # Load the full state to get broker configurations
            state = self.data_manager.load_state()
            if state and state.brokers:
                # Try to restore brokers from full configuration
                restored_count = 0
                for broker_id, broker_config in state.brokers.items():
                    if broker_id not in self.brokers:  # Only restore if not already restored
                        try:
                            # Check if port is available
                            if self._is_port_in_use(broker_config.port):
                                print(f"Warning: Port {broker_config.port} for broker {broker_id} is in use, skipping")
                                continue
                            
                            # Create broker instance
                            broker_instance = self._create_broker_instance(
                                broker_config.broker_id,
                                broker_config.cluster_id,
                                broker_config.host,
                                broker_config.port,
                                broker_config.seed_brokers,
                                register=False
                            )
                            
                            if broker_instance:
                                broker_instance.start_time = broker_config.start_time
                                self.brokers[broker_id] = broker_instance
                                restored_count += 1
                                
                        except Exception as e:
                            print(f"Warning: Failed to restore broker {broker_id}: {e}")
                
                if restored_count > 0:
                    print(f"Restored {restored_count} brokers from configuration")
                    self._save_state()
                else:
                    print("Failed to restore brokers from configuration")
    
    def _allocate_port(self) -> int:
        """Allocate next available port."""
        while self._is_port_in_use(self.next_port):
            self.next_port += 1
        port = self.next_port
        self.next_port += 1
        return port
    
    def _is_port_in_use(self, port: int) -> bool:
        """Check if port is already in use."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                return s.connect_ex(('localhost', port)) == 0
        except:
            return False
    
    def _create_broker_instance(self, broker_id: str, cluster_id: str, host: str, port: int, seed_brokers: List[str] = None, register: bool = True) -> Optional[BrokerInstance]:
        """Create and start a broker instance."""
        try:
            # Create broker object
            broker = Broker(
                broker_id=broker_id,
                cluster_id=cluster_id,
                listen_host=host,
                listen_port=port,
                seed_brokers=seed_brokers or []
            )
            
            # Create broker instance
            broker_instance = BrokerInstance(broker_id, cluster_id, host, port, broker)
            
            # Start the broker
            broker_instance.start()
            
            # Register broker if requested
            if register:
                self.brokers[broker_id] = broker_instance
                if cluster_id not in self.clusters:
                    self.clusters[cluster_id] = []
                self.clusters[cluster_id].append(broker_id)
            
            return broker_instance
            
        except Exception as e:
            print(f"Failed to create broker {broker_id}: {e}")
            return None
    

    
    # Implement abstract methods from BaseCLI
    def get_app_banner(self) -> str:
        """Return the application banner/title."""
        return "Broker Spawner CLI"
    
    def get_help_text(self) -> str:
        """Return application-specific help text."""
        persistence_status = "enabled" if self.persistence_enabled else "disabled"
        return f"""Broker Spawner Commands:
  s  <cluster> <count> [seeds]       - Spawn cluster (auto-allocated ports)
  sb <cluster> <broker> <host> [port] [seeds] - Spawn broker
  sc [count]                         - Spawn cluster (auto ID/port)
  sn <host>                          - Spawn single broker (auto all)
  l                                  - List clusters
  k  <cluster>                       - Kill cluster
  kb <broker>                        - Kill broker
  ka                                 - Kill all clusters
  kl [cluster]                       - Kill leader (uses last cluster if not specified)
  km [cluster]                       - Kill min broker ID (uses last cluster if not specified)
  t  [cluster]                       - Show topology (uses last cluster if not specified)
  q  <broker>                        - Show broker queues
  ps                                 - Show persistence status/info
  pc                                 - Clear persistence data

Address aliases: loc (localhost), lan (auto-LAN), custom aliases
Use '.' for auto-generated IDs and ports
Persistence: {persistence_status}"""
    
    def get_command_list(self) -> list:
        """Return list of application-specific commands for tab completion."""
        return ['s', 'sb', 'sc', 'sn', 'l', 'k', 'kb', 'ka', 'kl', 'km', 't', 'q', 'ps', 'pc']
    
    def handle_app_command(self, cmd: str, args: list) -> bool:
        """Handle application-specific commands. Return True if handled, False if unknown."""
        if cmd == 's':
            self._cmd_spawn_cluster(args)
        elif cmd == 'sb':
            self._cmd_spawn_broker(args)
        elif cmd == 'sc':
            self._cmd_spawn_cluster_quick(args)
        elif cmd == 'sn':
            self._cmd_spawn_single_broker(args)
        elif cmd == 'l':
            self._cmd_list_clusters()
        elif cmd == 'k':
            self._cmd_kill_cluster(args)
        elif cmd == 'kb':
            self._cmd_kill_broker(args)
        elif cmd == 'ka':
            self._cmd_kill_all()
        elif cmd == 'kl':
            self._cmd_kill_leader(args)
        elif cmd == 'km':
            self._cmd_kill_min_broker(args)
        elif cmd == 't':
            self._cmd_show_topology(args)
        elif cmd == 'q':
            self._cmd_show_queues(args)
        elif cmd == 'ps':
            self._cmd_persistence_status()
        elif cmd == 'pc':
            self._cmd_clear_persistence()
        else:
            return False  # Unknown command
        return True  # Command was handled
    

    
    def _cmd_spawn_cluster(self, args):
        """Spawn a cluster: s <cluster_id> <broker_count> [seed_brokers]"""
        if len(args) < 2:
            print("Usage: s <cluster_id> <broker_count> [seed_brokers]")
            return
        
        cluster_id = generate_cluster_id() if args[0] == '.' else args[0]
        try:
            broker_count = int(args[1])
        except ValueError:
            print("Error: broker_count must be a number")
            return
        
        if broker_count < 1 or broker_count > 5:
            print("Error: broker_count must be between 1 and 5")
            return
        
        # Parse seed brokers
        seed_brokers = []
        if len(args) > 2:
            seed_addrs = args[2].split(',')
            for addr in seed_addrs:
                if ':' in addr:
                    host_part, port_part = addr.split(':', 1)
                    resolved_host = self.ip_manager.resolve_ip(host_part)
                    seed_brokers.append(f"{resolved_host}:{port_part}")
        
        # Spawn brokers
        spawned = []
        spawned_ports = []
        for i in range(broker_count):
            broker_id = generate_broker_id()
            port = self._allocate_port()  # Use individual port allocation instead of consecutive
            host = self.ip_manager.resolve_ip('loc')  # Default to localhost
            
            broker_instance = self._create_broker_instance(broker_id, cluster_id, host, port, seed_brokers)
            if broker_instance:
                spawned.append(f"{host}:{port}")
                spawned_ports.append(port)
            
            # First broker becomes seed for subsequent ones
            if i == 0 and not seed_brokers:
                seed_brokers = [f"{host}:{port}"]
        
        if spawned:
            self.last_cluster_id = cluster_id  # Cache for convenience commands
            # Show individual ports instead of assuming consecutive range
            if len(spawned_ports) == 1:
                port_display = str(spawned_ports[0])
            else:
                port_display = ','.join(map(str, spawned_ports))
            print(f"Spawned cluster {cluster_id} with {len(spawned)} brokers on ports {port_display}")
            self._save_state()  # Save state after spawning
        else:
            print("Failed to spawn cluster")
    
    def _cmd_spawn_broker(self, args):
        """Spawn individual broker: sb <cluster_id> <broker_id> <host> [port] [seed_brokers]"""
        if len(args) < 3:
            print("Usage: sb <cluster_id> <broker_id> <host> [port] [seed_brokers]")
            return
        
        cluster_id = generate_cluster_id() if args[0] == '.' else args[0]
        broker_id = generate_broker_id() if args[1] == '.' else args[1]
        host = self.ip_manager.resolve_ip(args[2])
        
        # Determine port
        if len(args) > 3 and args[3] != '.':
            try:
                port = int(args[3])
            except ValueError:
                print("Error: port must be a number or '.'")
                return
        else:
            port = self._allocate_port()
        
        # Parse seed brokers
        seed_brokers = []
        if len(args) > 4:
            seed_addrs = args[4].split(',')
            for addr in seed_addrs:
                if ':' in addr:
                    host_part, port_part = addr.split(':', 1)
                    resolved_host = self.ip_manager.resolve_ip(host_part)
                    seed_brokers.append(f"{resolved_host}:{port_part}")
        
        broker_instance = self._create_broker_instance(broker_id, cluster_id, host, port, seed_brokers)
        if broker_instance:
            self.last_cluster_id = cluster_id  # Cache for convenience commands
            join_msg = f", joining via seed brokers" if seed_brokers else ""
            print(f"Spawned broker {broker_id} in cluster {cluster_id} on {host}:{port}{join_msg}")
            self._save_state()  # Save state after spawning
        else:
            print("Failed to spawn broker")
    
    def _cmd_spawn_cluster_quick(self, args):
        """Quick spawn cluster: sc [count]"""
        count = 3  # default
        if args:
            try:
                count = int(args[0])
            except ValueError:
                print("Error: count must be a number")
                return
        
        cluster_id = generate_cluster_id()
        host = self.ip_manager.resolve_ip('loc')
        
        spawned = []
        spawned_ports = []
        seed_brokers = []
        
        for i in range(count):
            broker_id = generate_broker_id()
            port = self._allocate_port()  # Use individual port allocation instead of consecutive
            
            broker_instance = self._create_broker_instance(broker_id, cluster_id, host, port, seed_brokers)
            if broker_instance:
                spawned.append(f"loc:{port}")
                spawned_ports.append(port)
            
            # First broker becomes seed
            if i == 0:
                seed_brokers = [f"{host}:{port}"]
        
        if spawned:
            self.last_cluster_id = cluster_id  # Cache for convenience commands
            # Show individual ports instead of assuming consecutive range
            if len(spawned_ports) == 1:
                port_display = str(spawned_ports[0])
            else:
                port_display = ','.join(map(str, spawned_ports))
            print(f"Spawned cluster {cluster_id} with {len(spawned)} brokers on loc:{port_display}")
            self._save_state()  # Save state after spawning
        else:
            print("Failed to spawn cluster")
    
    def _cmd_spawn_single_broker(self, args):
        """Quick spawn single broker: sn <host>"""
        if not args:
            print("Usage: sn <host>")
            return
        
        cluster_id = generate_cluster_id()
        broker_id = generate_broker_id()
        host = self.ip_manager.resolve_ip(args[0])
        port = self._allocate_port()
        
        broker_instance = self._create_broker_instance(broker_id, cluster_id, host, port)
        if broker_instance:
            self.last_cluster_id = cluster_id  # Cache for convenience commands
            print(f"Spawned broker {broker_id} in cluster {cluster_id} on {args[0]}:{port}")
            self._save_state()  # Save state after spawning
        else:
            print("Failed to spawn broker")
    
    def _cmd_list_clusters(self):
        """List all running clusters."""
        if not self.clusters:
            print("No running clusters")
            return
        
        print("Running clusters:")
        for cluster_id, broker_ids in self.clusters.items():
            alive_brokers = [bid for bid in broker_ids if bid in self.brokers and self.brokers[bid].is_alive()]
            if alive_brokers:
                broker_addrs = []
                for bid in alive_brokers:
                    broker = self.brokers[bid]
                    # Convert IP back to alias if possible
                    host_display = broker.host
                    for alias, ip in self.ip_manager.aliases.items():
                        if ip == broker.host:
                            host_display = alias
                            break
                    for alias, ip in self.ip_manager.custom_aliases.items():
                        if ip == broker.host:
                            host_display = alias
                            break
                    broker_addrs.append(f"{host_display}:{broker.port}")
                
                if len(alive_brokers) == 1:
                    print(f"- {cluster_id}: 1 broker ({broker_addrs[0]})")
                else:
                    # Show port range if consecutive and same host
                    if self._is_consecutive_ports(alive_brokers):
                        # Sort brokers by port for consistent range display
                        sorted_brokers = sorted([self.brokers[bid] for bid in alive_brokers], key=lambda b: b.port)
                        first_broker = sorted_brokers[0]
                        last_broker = sorted_brokers[-1]
                        host_display = broker_addrs[0].split(':')[0]
                        print(f"- {cluster_id}: {len(alive_brokers)} brokers ({host_display}:{first_broker.port}-{last_broker.port})")
                    else:
                        # Sort broker addresses by port for consistent display
                        sorted_addrs = sorted(broker_addrs, key=lambda addr: int(addr.split(':')[1]))
                        print(f"- {cluster_id}: {len(alive_brokers)} brokers ({', '.join(sorted_addrs)})")
    
    def _is_consecutive_ports(self, broker_ids: List[str]) -> bool:
        """Check if brokers have consecutive ports and same host."""
        if len(broker_ids) < 2:
            return False
        
        brokers = [self.brokers[bid] for bid in broker_ids]
        brokers.sort(key=lambda b: b.port)
        
        # Check same host
        first_host = brokers[0].host
        if not all(b.host == first_host for b in brokers):
            return False
        
        # Check consecutive ports
        for i in range(1, len(brokers)):
            if brokers[i].port != brokers[i-1].port + 1:
                return False
        
        return True
    
    def _cmd_kill_cluster(self, args):
        """Kill cluster: k <cluster_id>"""
        if not args:
            print("Usage: k <cluster_id>")
            return
        
        cluster_id = args[0]
        if cluster_id not in self.clusters:
            print(f"Cluster {cluster_id} not found")
            return
        
        broker_ids = self.clusters[cluster_id][:]
        killed_count = 0
        
        for broker_id in broker_ids:
            if broker_id in self.brokers:
                self.brokers[broker_id].stop()
                del self.brokers[broker_id]
                killed_count += 1
        
        del self.clusters[cluster_id]
        print(f"Killed cluster {cluster_id} ({killed_count} brokers)")
        self._save_state()  # Save state after killing
    
    def _cmd_kill_broker(self, args):
        """Kill broker: kb <broker_id>"""
        if not args:
            print("Usage: kb <broker_id>")
            return
        
        broker_id = args[0]
        if broker_id not in self.brokers:
            print(f"Broker {broker_id} not found")
            return
        
        broker = self.brokers[broker_id]
        cluster_id = broker.cluster_id
        
        broker.stop()
        del self.brokers[broker_id]
        
        # Remove from cluster
        if cluster_id in self.clusters:
            self.clusters[cluster_id].remove(broker_id)
            if not self.clusters[cluster_id]:
                del self.clusters[cluster_id]
        
        print(f"Killed broker {broker_id}")
        self._save_state()  # Save state after killing
    
    def _cmd_kill_all(self):
        """Kill all clusters: ka"""
        total_killed = len(self.brokers)
        
        for broker in self.brokers.values():
            broker.stop()
        
        self.brokers.clear()
        self.clusters.clear()
        
        print(f"Killed all clusters ({total_killed} brokers total)")
        self._save_state()  # Save state after killing
    
    def _cmd_kill_leader(self, args):
        """Kill leader broker: kl [cluster_id]"""
        if not args:
            if not self.last_cluster_id:
                print("Usage: kl <cluster_id> (no cached cluster available)")
                return
            cluster_id = self.last_cluster_id
        else:
            cluster_id = args[0]
        
        if cluster_id not in self.clusters:
            print(f"Cluster {cluster_id} not found")
            return
        
        # Find the leader broker
        leader_broker_id = None
        alive_brokers = [bid for bid in self.clusters[cluster_id] if bid in self.brokers and self.brokers[bid].is_alive()]
        
        if not alive_brokers:
            print(f"No alive brokers in cluster {cluster_id}")
            return
        
        # Check each broker's role to find the leader
        for broker_id in alive_brokers:
            broker_instance = self.brokers[broker_id]
            try:
                if hasattr(broker_instance.broker, 'role') and broker_instance.broker.role.value.lower() == 'leader':
                    leader_broker_id = broker_id
                    break
            except:
                pass
        
        if not leader_broker_id:
            # Fallback: assume first broker is leader if we can't determine role
            leader_broker_id = alive_brokers[0]
            print(f"Could not determine leader, assuming {leader_broker_id} is leader")
        
        # Kill the leader
        broker = self.brokers[leader_broker_id]
        broker.stop()
        del self.brokers[leader_broker_id]
        
        # Remove from cluster
        if cluster_id in self.clusters:
            self.clusters[cluster_id].remove(leader_broker_id)
            if not self.clusters[cluster_id]:
                del self.clusters[cluster_id]
        
        print(f"Killed leader broker {leader_broker_id} in cluster {cluster_id}")
        self._save_state()  # Save state after killing
    
    def _cmd_kill_min_broker(self, args):
        """Kill broker with minimum ID: km [cluster_id]"""
        if not args:
            if not self.last_cluster_id:
                print("Usage: km <cluster_id> (no cached cluster available)")
                return
            cluster_id = self.last_cluster_id
        else:
            cluster_id = args[0]
        
        if cluster_id not in self.clusters:
            print(f"Cluster {cluster_id} not found")
            return
        
        # Find alive brokers and get the one with minimum ID
        alive_brokers = [bid for bid in self.clusters[cluster_id] if bid in self.brokers and self.brokers[bid].is_alive()]
        
        if not alive_brokers:
            print(f"No alive brokers in cluster {cluster_id}")
            return
        
        # Get broker with minimum ID (alphabetically)
        min_broker_id = min(alive_brokers)
        
        # Kill the broker
        broker = self.brokers[min_broker_id]
        broker.stop()
        del self.brokers[min_broker_id]
        
        # Remove from cluster
        if cluster_id in self.clusters:
            self.clusters[cluster_id].remove(min_broker_id)
            if not self.clusters[cluster_id]:
                del self.clusters[cluster_id]
        
        print(f"Killed min broker ID {min_broker_id} in cluster {cluster_id}")
        self._save_state()  # Save state after killing
    
    def _cmd_show_topology(self, args):
        """Show cluster topology: t [cluster_id]"""
        if not args:
            if not self.last_cluster_id:
                print("Usage: t <cluster_id> (no cached cluster available)")
                return
            cluster_id = self.last_cluster_id
        else:
            cluster_id = args[0]
        if cluster_id not in self.clusters:
            print(f"Cluster {cluster_id} not found")
            return
        
        broker_ids = [bid for bid in self.clusters[cluster_id] if bid in self.brokers and self.brokers[bid].is_alive()]
        if not broker_ids:
            print(f"No alive brokers in cluster {cluster_id}")
            return
        
        print(f"Cluster {cluster_id}:")
        
        # Query actual broker roles if possible
        for i, broker_id in enumerate(broker_ids):
            broker_instance = self.brokers[broker_id]
            
            # Try to get actual role from broker
            try:
                role = broker_instance.broker.role.value.title() if hasattr(broker_instance.broker, 'role') else ("Leader" if i == 0 else "Replica")
            except:
                role = "Leader" if i == 0 else "Replica"
            
            # Try to get who this broker thinks is the leader
            try:
                current_leader = broker_instance.broker.get_current_leader() if hasattr(broker_instance.broker, 'get_current_leader') else None
                leader_view = f" | thinks leader: {current_leader}" if current_leader else " | thinks leader: None"
            except:
                leader_view = " | thinks leader: Unknown"
            
            status = "✓" if broker_instance.is_alive() else "✗"
            prefix = "├─" if i < len(broker_ids) - 1 else "└─"
            print(f"{prefix} {role}: {broker_id} ({broker_instance.host}:{broker_instance.port}) {status}{leader_view}")
    
    def _cmd_show_queues(self, args):
        """Show broker queues: q <broker_id>"""
        if not args:
            print("Usage: q <broker_id>")
            return
        
        broker_id = args[0]
        if broker_id not in self.brokers:
            print(f"Broker {broker_id} not found")
            return
        
        broker_instance = self.brokers[broker_id]
        if not broker_instance.is_alive():
            print(f"Broker {broker_id} is not running")
            return
        
        # Try to get actual queue information from broker
        try:
            broker = broker_instance.broker
            if hasattr(broker, 'data_manager'):
                # Get queue information
                snapshot = broker.data_manager.get_full_data_snapshot()
                
                print(f"Broker {broker_id} queues:")
                
                if not snapshot.get('queues'):
                    print("No queues found")
                    return
                
                # Show queues with message counts
                queue_stats = {}
                for msg in snapshot.get('messages', []):
                    queue_id = msg['queue_id']
                    if queue_id not in queue_stats:
                        queue_stats[queue_id] = {'count': 0, 'latest_seq': 0, 'latest_data': None}
                    queue_stats[queue_id]['count'] += 1
                    if msg['sequence_num'] > queue_stats[queue_id]['latest_seq']:
                        queue_stats[queue_id]['latest_seq'] = msg['sequence_num']
                        queue_stats[queue_id]['latest_data'] = msg['data']
                
                for queue_id in snapshot['queues']:
                    if queue_id in queue_stats:
                        stats = queue_stats[queue_id]
                        print(f"- {queue_id}: {stats['count']} messages (latest: seq={stats['latest_seq']}, data={stats['latest_data']})")
                    else:
                        print(f"- {queue_id}: 0 messages (empty)")
                
                # Show client positions
                client_positions = snapshot.get('client_positions', [])
                if client_positions:
                    print("\nClient positions:")
                    for pos in client_positions:
                        print(f"- {pos['client_id']} on {pos['queue_id']}: read position {pos['position']}")
                
            else:
                print(f"Broker {broker_id} data manager not available")
                
        except Exception as e:
            print(f"Failed to get queue information from broker {broker_id}: {e}")
    

    
    def _cmd_persistence_status(self):
        """Show persistence status: ps"""
        info = self.data_manager.get_state_info()
        print(info)
    
    def _cmd_clear_persistence(self):
        """Clear persistence data: pc"""
        if not self.persistence_enabled:
            print("Persistence is disabled for this session")
            return
        
        success = self.data_manager.clear_state()
        if success:
            print("Persistence data cleared")
        else:
            print("Failed to clear persistence data")
    
    def handle_cleanup(self):
        """Handle application-specific cleanup."""
        print("Cleaning up broker processes...")
        
        for broker in self.brokers.values():
            try:
                broker.stop()
            except:
                pass
        
        # Save final state before exit
        self._save_state()
        print("Shutdown complete")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Broker Spawner CLI - Interactive tool for spawning and managing broker clusters")
    parser.add_argument('-t', '--temporary', action='store_true', 
                       help='Disable persistence for this session (temporary mode)')
    
    args = parser.parse_args()
    
    # Initialize spawner with persistence setting
    enable_persistence = not args.temporary
    spawner = BrokerSpawner(enable_persistence=enable_persistence)
    
    spawner.run()


if __name__ == "__main__":
    main()
