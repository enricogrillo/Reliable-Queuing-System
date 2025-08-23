#!/usr/bin/env python3
"""
Interactive CLI for the Distributed Queuing Platform
"""

import argparse
import sys
import traceback
from typing import Optional, List

# Import from code directory
sys.path.append('code')
from client import Client


class DistributedQueueCLI:
    def __init__(self, brokers: List[str], client_id: Optional[str] = None):
        self.brokers = brokers
        self.client = None
        self.cluster_id = None
        self.cached_queue_id = None
        self.client_id = client_id
        
    def start(self):
        """Start the CLI interface"""
        print("Distributed Queue CLI v1.0")
        print("Type 'h' or 'help' for available commands.")
        print()
        
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
                elif cmd == 'q':
                    self._cmd_create_queue()
                elif cmd == 's':
                    self._cmd_send(args)
                elif cmd == 'r':
                    self._cmd_read(args)
                else:
                    print(f"Unknown command: {cmd}. Type 'h' for help.")
                    
            except KeyboardInterrupt:
                print("\nUse 'x' to exit.")
            except EOFError:
                print("\nGoodbye!")
                break
            except Exception as e:
                print(f"Error: {e}")
                if "--debug" in sys.argv:
                    traceback.print_exc()
    
    def _cmd_connect(self):
        """Connect to the broker cluster"""
        if not self.brokers:
            print("No brokers specified. Use -b option or 'b' command to add brokers.")
            return
            
        try:
            # Extract cluster_id from first broker (assume all brokers in same cluster)
            # For simplicity, we'll use a default cluster_id if not specified
            if not self.cluster_id:
                self.cluster_id = "G-DEFLT"  # Default cluster
                
            self.client = Client(
                cluster_id=self.cluster_id,
                initial_brokers=self.brokers,
                client_id=self.client_id
            )
            
            # Test connection by getting cluster info
            info = self.client.get_cluster_info()
            if info and 'brokers' in info:
                leader_count = len([b for b in info['brokers'] if b.get('is_leader', False)])
                total_brokers = len(info['brokers'])
                print(f"Connected to cluster {self.cluster_id} ({total_brokers} brokers, {leader_count} leader{'s' if leader_count != 1 else ''})")
            else:
                print("Connected to cluster")
                
        except Exception as e:
            print(f"Failed to connect: {e}")
            self.client = None
    
    def _cmd_add_broker(self, args):
        """Add new broker to establish new cluster connection"""
        if not args:
            print("Usage: b host:port")
            return
            
        broker_addr = args[0]
        if ':' not in broker_addr:
            print("Invalid broker format. Use host:port")
            return
            
        self.brokers.append(broker_addr)
        print(f"Added broker: {broker_addr}")
        
        # If we have a client, refresh topology
        if self.client:
            try:
                self.client.refresh_topology()
                print("Topology refreshed")
            except Exception as e:
                print(f"Warning: Failed to refresh topology: {e}")
    
    def _cmd_topology(self):
        """Show connection and cluster status topology"""
        if not self.client:
            print("Not connected. Use 'c' to connect first.")
            return
            
        try:
            info = self.client.get_cluster_info()
            if not info or 'brokers' not in info:
                print("No cluster information available")
                return
                
            print(f"Cluster: {self.cluster_id}")
            for broker in info['brokers']:
                role = "Leader" if broker.get('is_leader', False) else "Replica"
                status = "✓" if broker.get('is_alive', True) else "✗"
                host = broker.get('host', 'unknown')
                port = broker.get('port', 'unknown')
                broker_id = broker.get('broker_id', 'unknown')
                
                if broker == info['brokers'][0]:
                    print(f"├─ {role}: {broker_id} ({host}:{port}) {status}")
                elif broker == info['brokers'][-1]:
                    print(f"└─ {role}: {broker_id} ({host}:{port}) {status}")
                else:
                    print(f"├─ {role}: {broker_id} ({host}:{port}) {status}")
                    
            print(f"Client: {self.client.client_id}")
            
        except Exception as e:
            print(f"Failed to get topology: {e}")
    
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
                error = response.get('error', 'Unknown error') if response else 'No response'
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
                error = response.get('error', 'Unknown error') if response else 'No response'
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
                message = response.get('message')
                if message is not None:
                    print(f"Message: {message}")
                else:
                    print("No more messages")
            else:
                error = response.get('error', 'Unknown error') if response else 'No response'
                if 'no more messages' in error.lower() or 'no messages' in error.lower():
                    print("No more messages")
                else:
                    print(f"Failed to read message: {error}")
                    
        except Exception as e:
            print(f"Failed to read message: {e}")
    
    def _cmd_help(self):
        """Show available commands"""
        print("Available commands:")
        print("  c              - Connect to the broker cluster")
        print("  b <host:port>  - Add new broker to establish new cluster connection")
        print("  t              - Show connection and cluster status topology")
        print("  q              - Create a new queue (auto-generated ID, cached)")
        print("  s <queue_id> <message> - Send message to specific queue")
        print("  s <message>    - Send message to cached queue_id")
        print("  r <queue_id>   - Read from specific queue")
        print("  r              - Read from cached queue_id")
        print("  h, help        - Show this help")
        print("  x, quit, exit  - Exit the CLI")
    
    def _cmd_exit(self):
        """Exit the CLI"""
        if self.client:
            try:
                # Close any connections if the client has such a method
                pass
            except:
                pass
        print("Goodbye!")


def parse_brokers(broker_string: str) -> List[str]:
    """Parse comma-separated broker string into list"""
    if not broker_string:
        return []
    return [b.strip() for b in broker_string.split(',') if b.strip()]


def main():
    parser = argparse.ArgumentParser(description='Distributed Queue CLI')
    parser.add_argument('-b', '--brokers', 
                       default='localhost:9001,localhost:9002,localhost:9003',
                       help='Comma-separated list of broker addresses (host:port)')
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
