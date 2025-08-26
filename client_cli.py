#!/usr/bin/env python3
"""
Interactive CLI for the Distributed Queuing Platform
"""

import argparse
import sys
import traceback
import socket
from typing import Optional, List

# Enable command history and line editing
try:
    import readline
    # Set up command history
    readline.parse_and_bind("tab: complete")
    readline.parse_and_bind("set editing-mode emacs")
except ImportError:
    # readline not available on some platforms (like Windows)
    readline = None

# Import from code directory
sys.path.append('code')
from code.client import Client


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


class DistributedQueueCLI:
    def __init__(self, brokers: List[str], client_id: Optional[str] = None):
        self.ip_manager = IPManager()
        self.brokers = self._resolve_broker_aliases(brokers)
        self.client = None
        self.cluster_id = None
        self.cached_queue_id = None
        self.client_id = client_id
        self.history_file = ".queue_cli_history"
        
        # Load command history if readline is available
        self._load_history()
    
    def _resolve_broker_aliases(self, brokers: List[str]) -> List[str]:
        """Resolve any IP aliases in broker addresses."""
        resolved = []
        for broker in brokers:
            if ':' in broker:
                host, port = broker.split(':', 1)
                resolved_host = self.ip_manager.resolve_ip(host)
                resolved.append(f"{resolved_host}:{port}")
            else:
                resolved.append(broker)
        return resolved
        
    def _load_history(self):
        """Load command history from file."""
        if readline is None:
            return
            
        try:
            readline.read_history_file(self.history_file)
        except FileNotFoundError:
            # No history file yet, that's fine
            pass
        except Exception as e:
            # Don't fail startup due to history issues
            pass
    
    def _save_history(self):
        """Save command history to file."""
        if readline is None:
            return
            
        try:
            # Limit history to last 500 commands
            readline.set_history_length(500)
            readline.write_history_file(self.history_file)
        except Exception as e:
            # Don't fail due to history saving issues
            pass
        
    def start(self):
        """Start the CLI interface"""
        print("Distributed Queue CLI v1.0 - Multi-Cluster Auto-Discovery")
        print("Type 'h' or 'help' for available commands.")
        print("Use Ctrl+C to exit, or 'x' to quit.")
        print("Auto-discovery: Client automatically scans for new clusters from seed brokers.")
        print()
        
        try:
            while True:
                try:
                    command = input("> ").strip()
                    if not command:
                        continue
                        
                    parts = command.split()
                    cmd = parts[0].lower()
                    args = parts[1:] if len(parts) > 1 else []
                    
                    if cmd in ['x', 'quit', 'exit']:
                        self._cmd_exit()
                        break
                    elif cmd in ['h', 'help']:
                        self._cmd_help()
                    elif cmd == 'c':
                        self._cmd_connect()
                    elif cmd == 'b':
                        self._cmd_add_broker(args)
                    elif cmd == 't':
                        self._cmd_topology()
                    elif cmd == 'l':
                        self._cmd_list_clusters()
                    elif cmd == 'q':
                        self._cmd_create_queue()
                    elif cmd == 's':
                        self._cmd_send(args)
                    elif cmd == 'r':
                        self._cmd_read(args)
                    elif cmd == 'ip':
                        self._cmd_show_ip()
                    elif cmd == 'fi':
                        self._cmd_refresh_ip()
                    elif cmd == 'a':
                        self._cmd_add_alias(args)
                    elif cmd == 'ua':
                        self._cmd_remove_alias(args)
                    else:
                        print(f"Unknown command: {cmd}. Type 'h' for help.")
                        
                except EOFError:
                    # Handle Ctrl+D
                    print("\nGoodbye!")
                    break
                except Exception as e:
                    print(f"Error: {e}")
                    if "--debug" in sys.argv:
                        traceback.print_exc()
                        
        except KeyboardInterrupt:
            # Handle Ctrl+C
            print("\n\nExiting...")
            self._cmd_exit()
        except Exception as e:
            print(f"Fatal error: {e}")
            if "--debug" in sys.argv:
                traceback.print_exc()
        finally:
            # Save command history on exit
            self._save_history()
    
    def _cmd_connect(self):
        """Connect to the broker cluster"""
        if not self.brokers:
            print("No brokers specified. Use -b option or 'b' command to add brokers.")
            return
            
        try:
            # Create client with seed brokers and connect to discover clusters
            self.client = Client(
                seed_brokers=self.brokers,
                client_id=self.client_id
            )
            connected = self.client.connect()
            
            # connect() now always returns True and starts auto-discovery
            # Report on discovered clusters
            clusters = self.client.clusters
            if clusters:
                print(f"Successfully discovered {len(clusters)} cluster(s):")
                for cluster_id in clusters.keys():
                    print(f"  • {cluster_id}")
            else:
                print("No clusters discovered initially - they will be found automatically when available")
            
            print(f"Auto-discovery enabled - will scan for new clusters every 30 seconds")
                
        except Exception as e:
            print(f"Failed to connect: {e}")
            self.client = None
    
    def _cmd_add_broker(self, args):
        """Add new broker to establish new cluster connection"""
        if not args:
            print("Usage: b host:port (supports loc/lan aliases)")
            return
            
        broker_addr = args[0]
        if ':' not in broker_addr:
            print("Invalid broker format. Use host:port")
            return
        
        # Resolve any IP aliases
        host, port = broker_addr.split(':', 1)
        resolved_host = self.ip_manager.resolve_ip(host)
        resolved_broker = f"{resolved_host}:{port}"
        
        self.brokers.append(resolved_broker)
        if broker_addr != resolved_broker:
            print(f"Added broker: {broker_addr} -> {resolved_broker}")
        else:
            print(f"Added broker: {broker_addr}")
        
        # If we have a client, use the new auto-discovery functionality
        if self.client:
            try:
                # Add to client's seed brokers and trigger auto-discovery
                old_clusters = set(self.client.clusters.keys())
                self.client.add_seed_brokers([resolved_broker])
                new_clusters = set(self.client.clusters.keys())
                
                # Report newly discovered clusters
                discovered = new_clusters - old_clusters
                if discovered:
                    print(f"Discovered new clusters: {', '.join(discovered)}")
                else:
                    print("No new clusters discovered from this broker")
                    
            except Exception as e:
                print(f"Warning: Failed to discover clusters from new broker: {e}")
    
    def _cmd_topology(self):
        """Show connection and cluster status topology for all discovered clusters"""
        if not self.client:
            print("Not connected. Use 'c' to connect first.")
            return
            
        try:
            all_clusters_info = self.client.get_all_clusters_info()
            if not all_clusters_info:
                print("No cluster information available")
                return
            
            print(f"Discovered {len(all_clusters_info)} cluster(s):")
            print()
            
            for i, (cluster_id, info) in enumerate(all_clusters_info.items()):
                print(f"Cluster {i+1}: {cluster_id}")
                
                if info and 'brokers' in info:
                    for j, broker in enumerate(info['brokers']):
                        role = broker.get('role', 'unknown').capitalize()
                        status = "✓" if broker.get('is_alive', True) else "✗"
                        host = broker.get('host', 'unknown')
                        port = broker.get('port', 'unknown')
                        broker_id = broker.get('broker_id', 'unknown')
                        
                        if j == len(info['brokers']) - 1:
                            print(f"└─ {role}: {broker_id} ({host}:{port}) {status}")
                        else:
                            print(f"├─ {role}: {broker_id} ({host}:{port}) {status}")
                else:
                    print("├─ (No broker information available)")
                
                # Add spacing between clusters except for the last one
                if i < len(all_clusters_info) - 1:
                    print()
            
            print()
            print(f"Client: {self.client.client_id}")
            print(f"Total seed brokers: {len(self.client.seed_brokers)}")
            print(f"Assigned seed brokers: {len(self.client.assigned_seed_brokers)}")
            print(f"Unassigned seed brokers: {len(self.client.seed_brokers) - len(self.client.assigned_seed_brokers)}")
            
        except Exception as e:
            print(f"Failed to get topology: {e}")
    
    def _cmd_list_clusters(self):
        """List all discovered clusters with their seed brokers"""
        if not self.client:
            print("Not connected. Use 'c' to connect first.")
            return
            
        try:
            if not self.client.clusters:
                print("No clusters discovered yet.")
                return
            
            print(f"Discovered Clusters ({len(self.client.clusters)}):")
            print("=" * 50)
            
            for cluster_id, cluster_info in self.client.clusters.items():
                topology = cluster_info.get('topology')
                is_connected = cluster_info.get('is_connected', False)
                seed_brokers = cluster_info.get('seed_brokers', [])
                
                status = "Connected" if is_connected else "Disconnected"
                print(f"\nCluster: {cluster_id} ({status})")
                print(f"  Seed Brokers: {', '.join(seed_brokers)}")
                
                if topology and hasattr(topology, 'brokers'):
                    leader = topology.get_leader()
                    replicas = topology.get_replicas()
                    
                    if leader:
                        print(f"  Leader: {leader.broker_id} ({leader.host}:{leader.port})")
                    else:
                        print(f"  Leader: None")
                    
                    if replicas:
                        replica_info = [f"{r.broker_id} ({r.host}:{r.port})" for r in replicas]
                        print(f"  Replicas: {', '.join(replica_info)}")
                    else:
                        print(f"  Replicas: None")
                else:
                    print(f"  Topology: Not available")
            
            print("\n" + "=" * 50)
            print(f"Auto-Discovery Status:")
            unassigned_count = len(self.client.seed_brokers) - len(self.client.assigned_seed_brokers)
            print(f"  Total seed brokers: {len(self.client.seed_brokers)}")
            print(f"  Assigned to clusters: {len(self.client.assigned_seed_brokers)}")
            print(f"  Unassigned (will be scanned): {unassigned_count}")
            
            if unassigned_count > 0:
                unassigned = [b for b in self.client.seed_brokers if b not in self.client.assigned_seed_brokers]
                print(f"  Unassigned brokers: {', '.join(unassigned)}")
            
        except Exception as e:
            print(f"Failed to list clusters: {e}")
    
    def _cmd_create_queue(self):
        """Create a new queue"""
        if not self.client:
            print("Not connected. Use 'c' to connect first.")
            return
            
        try:
            response = self.client.create_queue()
            if response and response.get('status') == 'success':
                queue_id = response.get('queue_id')
                self.cached_queue_id = queue_id
                print(f"Queue created: {queue_id}")
            else:
                error = response.get('message', 'Unknown error') if response else 'No response'
                print(f"Failed to create queue: {error}")
                
        except Exception as e:
            print(f"Failed to create queue: {e}")
    
    def _cmd_send(self, args):
        """Send a message to a queue"""
        if not self.client:
            print("Not connected. Use 'c' to connect first.")
            return
            
        if len(args) == 0:
            print("Usage: s <queue_id> <message> or s <message> (uses cached queue)")
            return
        elif len(args) == 1:
            # Use cached queue_id
            if not self.cached_queue_id:
                print("No cached queue. Use 's <queue_id> <message>' first.")
                return
            queue_id = self.cached_queue_id
            message = args[0]
        else:
            # Use provided queue_id
            queue_id = args[0]
            message = ' '.join(args[1:])
            self.cached_queue_id = queue_id
            
        try:
            response = self.client.append_message(queue_id, message)
            if response and response.get('status') == 'success':
                # Try to get broker info from response or client
                broker_info = ""
                if hasattr(self.client, '_last_used_broker'):
                    broker_info = f", broker {self.client._last_used_broker}"
                print(f"Message sent successfully{broker_info}")
            else:
                error = response.get('message', 'Unknown error') if response else 'No response'
                print(f"Failed to send message: {error}")
                
        except Exception as e:
            print(f"Failed to send message: {e}")
    
    def _cmd_read(self, args):
        """Read next message from a queue"""
        if not self.client:
            print("Not connected. Use 'c' to connect first.")
            return
            
        if len(args) == 0:
            # Use cached queue_id
            if not self.cached_queue_id:
                print("No cached queue. Use 'r <queue_id>' first.")
                return
            queue_id = self.cached_queue_id
        else:
            # Use provided queue_id
            queue_id = args[0]
            self.cached_queue_id = queue_id
            
        try:
            response = self.client.read_message(queue_id)
            if response and response.get('status') == 'success':
                message = response.get('data')
                if message is not None:
                    print(f"Message: {message}")
                else:
                    print("No more messages")
            else:
                error = response.get('message', 'Unknown error') if response else 'No response'
                if 'no more messages' in error.lower() or 'no messages' in error.lower():
                    print("No more messages")
                else:
                    print(f"Failed to read message: {error}")
                    
        except Exception as e:
            print(f"Failed to read message: {e}")
    
    def _cmd_help(self):
        """Show available commands"""
        print("Available commands:")
        print()
        print("Connection & Discovery:")
        print("  c              - Connect to seed brokers and discover clusters")
        print("  b <host:port>  - Add new seed broker (triggers auto-discovery)")
        print("  t              - Show topology for all discovered clusters")
        print("  l              - List all discovered clusters with details")
        print()
        print("Queue Operations:")
        print("  q              - Create a new queue (auto-generated ID, cached)")
        print("  s <queue_id> <message> - Send message to specific queue")
        print("  s <message>    - Send message to cached queue_id")
        print("  r <queue_id>   - Read from specific queue")
        print("  r              - Read from cached queue_id")
        print()
        print("IP Address Management:")
        print("  ip             - Show IP mappings (loc, lan, custom aliases)")
        print("  fi             - Refresh LAN IP discovery")
        print("  a <name> <ip>  - Add custom IP alias")
        print("  ua <name>      - Remove custom IP alias")
        print()
        print("Utility:")
        print("  h, help        - Show this help")
        print("  x, quit, exit  - Exit the CLI")
        print()
        print("Address Aliases:")
        print("  • loc          - localhost (127.0.0.1)")
        print("  • lan          - auto-discovered LAN IP")
        print("  • Custom aliases can be added with 'a' command")
        print("  • Use in broker addresses: loc:9001, lan:9002")
        print()
        print("Auto-Discovery:")
        print("  • Client automatically scans unassigned seed brokers every 30s")
        print("  • New clusters are discovered automatically when brokers are added")
        print("  • Use 'l' command to see auto-discovery status")
        print()
        print("Navigation:")
        print("  ↑ / ↓          - Browse command history")
        print("  Ctrl+C         - Exit the CLI")
        print("  Ctrl+D         - Exit the CLI (EOF)")
        print("  Tab            - Command completion (if supported)")
    
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
    
    def _cmd_exit(self):
        """Exit the CLI"""
        if self.client:
            try:
                # Properly disconnect the client to stop background threads
                self.client.disconnect()
            except Exception as e:
                # Don't fail exit due to cleanup issues
                print(f"Warning: Error during cleanup: {e}")
        
        # Save history before exiting
        self._save_history()
        print("Goodbye!")


def parse_brokers(broker_string: str) -> List[str]:
    """Parse comma-separated broker string into list"""
    if not broker_string:
        return []
    return [b.strip() for b in broker_string.split(',') if b.strip()]


def main():
    parser = argparse.ArgumentParser(description='Distributed Queue CLI')
    parser.add_argument('-b', '--brokers', 
                       default='loc:9001,loc:9002,loc:9003',
                       help='Comma-separated list of broker addresses (host:port, supports loc/lan aliases)')
    parser.add_argument('-n', '--client-id', 
                       help='Client ID (auto-generated if not provided)')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug mode with full stack traces')
    
    args = parser.parse_args()
    
    brokers = parse_brokers(args.brokers)
    if not brokers:
        print("Error: No valid brokers specified")
        sys.exit(1)
    
    cli = DistributedQueueCLI(brokers, args.client_id)
    cli.start()


if __name__ == '__main__':
    main()
