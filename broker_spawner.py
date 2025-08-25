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
import readline
from typing import Dict, List, Optional, Tuple
import json

# Add code directory to path
sys.path.insert(0, 'code')
from code.id_generator import generate_broker_id, generate_cluster_id
from code.broker import Broker


class IPManager:
    """Manages IP address aliases and discovery."""
    
    def __init__(self):
        self.aliases = {
            'loc': '127.0.0.1',
            'lan': self._discover_lan_ip()
        }
        self.custom_aliases = {}
    
    def _discover_lan_ip(self) -> str:
        """Auto-discover LAN IP address."""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except:
            return "127.0.0.1"
    
    def refresh_lan_ip(self) -> str:
        """Refresh and update LAN IP discovery."""
        old_ip = self.aliases['lan']
        new_ip = self._discover_lan_ip()
        self.aliases['lan'] = new_ip
        return f"Refreshed LAN IP: {new_ip}" + (f" (changed from {old_ip})" if old_ip != new_ip else "")
    
    def add_alias(self, name: str, ip: str) -> str:
        """Add or update custom IP alias."""
        if name in ['loc', 'lan']:
            return f"Cannot override built-in alias '{name}'"
        self.custom_aliases[name] = ip
        return f"Added alias '{name}' -> {ip}"
    
    def remove_alias(self, name: str) -> str:
        """Remove custom IP alias."""
        if name in ['loc', 'lan']:
            return f"Cannot remove built-in alias '{name}'"
        if name in self.custom_aliases:
            ip = self.custom_aliases.pop(name)
            return f"Removed alias '{name}'"
        return f"Alias '{name}' not found"
    
    def resolve_ip(self, addr: str) -> str:
        """Resolve alias to IP address."""
        if addr in self.aliases:
            return self.aliases[addr]
        if addr in self.custom_aliases:
            return self.custom_aliases[addr]
        return addr  # Return as-is if not an alias
    
    def show_mappings(self) -> str:
        """Show all IP mappings."""
        lines = ["IP mappings:"]
        lines.append(f"- loc: {self.aliases['loc']} (localhost)")
        lines.append(f"- lan: {self.aliases['lan']} (auto-discovered)")
        for name, ip in self.custom_aliases.items():
            lines.append(f"- {name}: {ip} (custom)")
        return "\n".join(lines)


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


class BrokerSpawner:
    """Main broker spawner CLI application."""
    
    def __init__(self):
        self.ip_manager = IPManager()
        self.brokers: Dict[str, BrokerInstance] = {}  # broker_id -> BrokerInstance
        self.clusters: Dict[str, List[str]] = {}      # cluster_id -> [broker_ids]
        self.next_port = 9001
        self.history_file = os.path.expanduser("~/.broker_spawner_history")
        self.running = True
        self.last_cluster_id = None  # Cache last cluster ID for convenience commands
        
        # Setup readline
        self._setup_readline()
    
    def _setup_readline(self):
        """Setup readline for command history and completion."""
        try:
            readline.read_history_file(self.history_file)
        except FileNotFoundError:
            pass
        
        # Set history length
        readline.set_history_length(1000)
        
        # Basic tab completion
        commands = ['h', 's', 'sb', 'sc', 'sn', 'l', 'k', 'kb', 'ka', 'kl', 'km', 't', 'q', 'ip', 'fi', 'a', 'ua', 'x']
        def completer(text, state):
            options = [cmd for cmd in commands if cmd.startswith(text)]
            if state < len(options):
                return options[state]
            return None
        
        readline.set_completer(completer)
        readline.parse_and_bind("tab: complete")
    
    def _save_history(self):
        """Save command history to file."""
        try:
            readline.write_history_file(self.history_file)
        except:
            pass
    
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
    
    def _create_broker_instance(self, broker_id: str, cluster_id: str, host: str, port: int, seed_brokers: List[str] = None) -> Optional[BrokerInstance]:
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
            
            # Register broker
            self.brokers[broker_id] = broker_instance
            if cluster_id not in self.clusters:
                self.clusters[cluster_id] = []
            self.clusters[cluster_id].append(broker_id)
            
            return broker_instance
            
        except Exception as e:
            print(f"Failed to create broker {broker_id}: {e}")
            return None
    
    def run(self):
        """Main CLI loop."""
        print("Broker Spawner CLI")
        print("Type 'h' for help, 'x' to exit")
        print()
        
        try:
            while self.running:
                try:
                    command = input("> ").strip()
                    if command:
                        self._process_command(command)
                except EOFError:
                    # Ctrl+D
                    print("\nGoodbye!")
                    break
                except KeyboardInterrupt:
                    # Ctrl+C
                    print("\nGoodbye!")
                    break
        finally:
            self.shutdown()
    
    def _process_command(self, command: str):
        """Process a single command."""
        parts = command.split()
        if not parts:
            return
        
        cmd = parts[0].lower()
        args = parts[1:]
        
        try:
            if cmd in ['h', 'help']:
                self._cmd_help()
            elif cmd == 's':
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
            elif cmd == 'ip':
                self._cmd_show_ip()
            elif cmd == 'fi':
                self._cmd_refresh_ip()
            elif cmd == 'a':
                self._cmd_add_alias(args)
            elif cmd == 'ua':
                self._cmd_remove_alias(args)
            elif cmd in ['x', 'exit', 'quit']:
                print("Goodbye!")
                self.running = False
            else:
                print(f"Unknown command: {cmd}. Type 'h' for help.")
        except Exception as e:
            print(f"Error: {e}")
    
    def _cmd_help(self):
        """Display help information."""
        help_text = """Broker Spawner Commands:
  s  <cluster> <count> [port] [seeds] - Spawn cluster
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
  ip                                 - Show IP mappings
  a  <name> <ip>                     - Add IP alias
  ua <name>                          - Remove IP alias
  x                                  - Exit

Address aliases: loc (localhost), lan (auto-LAN), custom aliases
Use '.' for auto-generated IDs and ports"""
        print(help_text)
    
    def _cmd_spawn_cluster(self, args):
        """Spawn a cluster: s <cluster_id> <broker_count> [port_start] [seed_brokers]"""
        if len(args) < 2:
            print("Usage: s <cluster_id> <broker_count> [port_start] [seed_brokers]")
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
        
        # Determine starting port
        if len(args) > 2 and args[2] != '.':
            try:
                start_port = int(args[2])
            except ValueError:
                print("Error: port_start must be a number or '.'")
                return
        else:
            start_port = self._allocate_port()
        
        # Parse seed brokers
        seed_brokers = []
        if len(args) > 3:
            seed_addrs = args[3].split(',')
            for addr in seed_addrs:
                if ':' in addr:
                    host_part, port_part = addr.split(':', 1)
                    resolved_host = self.ip_manager.resolve_ip(host_part)
                    seed_brokers.append(f"{resolved_host}:{port_part}")
        
        # Spawn brokers
        spawned = []
        for i in range(broker_count):
            broker_id = generate_broker_id()
            port = start_port + i
            host = self.ip_manager.resolve_ip('loc')  # Default to localhost
            
            broker_instance = self._create_broker_instance(broker_id, cluster_id, host, port, seed_brokers)
            if broker_instance:
                spawned.append(f"{host}:{port}")
            
            # First broker becomes seed for subsequent ones
            if i == 0 and not seed_brokers:
                seed_brokers = [f"{host}:{port}"]
        
        if spawned:
            self.last_cluster_id = cluster_id  # Cache for convenience commands
            port_range = f"{start_port}-{start_port + len(spawned) - 1}" if len(spawned) > 1 else str(start_port)
            print(f"Spawned cluster {cluster_id} with {len(spawned)} brokers on ports {port_range}")
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
        start_port = self._allocate_port()
        host = self.ip_manager.resolve_ip('loc')
        
        spawned = []
        seed_brokers = []
        
        for i in range(count):
            broker_id = generate_broker_id()
            port = start_port + i
            
            broker_instance = self._create_broker_instance(broker_id, cluster_id, host, port, seed_brokers)
            if broker_instance:
                spawned.append(f"loc:{port}")
            
            # First broker becomes seed
            if i == 0:
                seed_brokers = [f"{host}:{port}"]
        
        if spawned:
            self.last_cluster_id = cluster_id  # Cache for convenience commands
            port_range = f"{start_port}-{start_port + len(spawned) - 1}" if len(spawned) > 1 else str(start_port)
            print(f"Spawned cluster {cluster_id} with {len(spawned)} brokers on loc:{port_range}")
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
                        first_broker = self.brokers[alive_brokers[0]]
                        last_broker = self.brokers[alive_brokers[-1]]
                        host_display = broker_addrs[0].split(':')[0]
                        print(f"- {cluster_id}: {len(alive_brokers)} brokers ({host_display}:{first_broker.port}-{last_broker.port})")
                    else:
                        print(f"- {cluster_id}: {len(alive_brokers)} brokers ({', '.join(broker_addrs)})")
    
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
    
    def _cmd_kill_all(self):
        """Kill all clusters: ka"""
        total_killed = len(self.brokers)
        
        for broker in self.brokers.values():
            broker.stop()
        
        self.brokers.clear()
        self.clusters.clear()
        
        print(f"Killed all clusters ({total_killed} brokers total)")
    
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
            
            status = "✓" if broker_instance.is_alive() else "✗"
            prefix = "├─" if i < len(broker_ids) - 1 else "└─"
            print(f"{prefix} {role}: {broker_id} ({broker_instance.host}:{broker_instance.port}) {status}")
    
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
    
    def _cmd_show_ip(self):
        """Show IP mappings: ip"""
        print(self.ip_manager.show_mappings())
    
    def _cmd_refresh_ip(self):
        """Refresh LAN IP: fi"""
        result = self.ip_manager.refresh_lan_ip()
        print(result)
    
    def _cmd_add_alias(self, args):
        """Add IP alias: a <name> <ip>"""
        if len(args) != 2:
            print("Usage: a <name> <ip>")
            return
        
        name, ip = args
        result = self.ip_manager.add_alias(name, ip)
        print(result)
    
    def _cmd_remove_alias(self, args):
        """Remove IP alias: ua <name>"""
        if not args:
            print("Usage: ua <name>")
            return
        
        name = args[0]
        result = self.ip_manager.remove_alias(name)
        print(result)
    
    def shutdown(self):
        """Shutdown spawner and clean up."""
        print("Cleaning up broker processes...")
        
        for broker in self.brokers.values():
            try:
                broker.stop()
            except:
                pass
        
        self._save_history()
        print("Shutdown complete")


def main():
    """Main entry point."""
    spawner = BrokerSpawner()
    try:
        spawner.run()
    except KeyboardInterrupt:
        spawner.shutdown()


if __name__ == "__main__":
    main()
