#!/usr/bin/env python3
"""
Interactive debugging CLI for the distributed queue system.
Provides commands to spawn brokers, manage clusters, and debug the system.
"""

import cmd
import sys
from typing import Optional

from .broker_manager import BrokerManager
from .proxy import Proxy
from .http_client import HttpJsonClient


class DebugCLI(cmd.Cmd):
    """Interactive debugging interface for the queue system."""
    
    intro = """
🔧 Queue System Debug CLI
=========================
Commands:
  broker     - Broker management (spawn, stop, status, etc.)
  proxy      - Proxy management (start, connect, etc.)
  system     - System operations (quick setup, cleanup, etc.)
  help       - Show detailed help
  quit/exit  - Exit the CLI
"""
    prompt = "debug> "
    
    def __init__(self):
        super().__init__()
        self.broker_manager = BrokerManager()
        self.proxy: Optional[Proxy] = None
        self.proxy_url = "http://127.0.0.1:8000"

    # =============================================================================
    # Broker Management Commands
    # =============================================================================
    
    def do_spawn_broker(self, arg):
        """Spawn a new broker.
        
        Usage: spawn_broker <address:port> [leader] [group_name]
        Examples:
          spawn_broker 127.0.0.1:8001
          spawn_broker 127.0.0.1:8001 leader
          spawn_broker 127.0.0.1:8002 follower mygroup
        """
        args = arg.split()
        if len(args) < 1:
            print("Error: Address required (format: host:port)")
            return
        
        try:
            # Parse address
            address, port = self._parse_address(args[0])
            
            # Parse role
            is_leader = len(args) >= 2 and args[1].lower() == "leader"
            
            # Parse group
            group_name = args[2] if len(args) >= 3 else "default"
            
            self.broker_manager.spawn_broker(address, port, is_leader, group_name)
            
        except Exception as e:
            print(f"Error spawning broker: {e}")
    
    def do_spawn_group(self, arg):
        """Spawn a group of brokers with replication.
        
        Usage: spawn_group <group_name> <leader_addr:port> <follower1_addr:port> [follower2_addr:port] ...
        Example: spawn_group mygroup 127.0.0.1:8001 127.0.0.1:8002 127.0.0.1:8003
        """
        args = arg.split()
        if len(args) < 3:
            print("Error: Need group name, leader address, and at least one follower address")
            return
        
        try:
            group_name = args[0]
            configs = []
            
            # First broker is leader
            leader_addr, leader_port = self._parse_address(args[1])
            configs.append((leader_addr, leader_port, True))
            
            # Rest are followers
            for addr_str in args[2:]:
                addr, port = self._parse_address(addr_str)
                configs.append((addr, port, False))
            
            self.broker_manager.spawn_group(group_name, configs)
            
        except Exception as e:
            print(f"Error spawning group: {e}")
    
    def do_stop_broker(self, arg):
        """Stop a specific broker.
        
        Usage: stop_broker <broker_name>
        Example: stop_broker broker-1
        """
        if not arg:
            print("Error: Broker name required")
            return
        
        if self.broker_manager.stop_broker(arg):
            print(f"✓ Stopped broker '{arg}'")
        else:
            print(f"❌ Broker '{arg}' not found")
    
    def do_restart_broker(self, arg):
        """Restart a specific broker.
        
        Usage: restart_broker <broker_name>
        Example: restart_broker broker-1
        """
        if not arg:
            print("Error: Broker name required")
            return
        
        if self.broker_manager.restart_broker(arg):
            print(f"✓ Restarted broker '{arg}'")
        else:
            print(f"❌ Broker '{arg}' not found")
    
    def do_crash_broker(self, arg):
        """Simulate a broker crash.
        
        Usage: crash_broker <broker_name>
        Example: crash_broker broker-1
        """
        if not arg:
            print("Error: Broker name required")
            return
        
        if self.broker_manager.simulate_crash(arg):
            print(f"💥 Simulated crash for broker '{arg}'")
        else:
            print(f"❌ Broker '{arg}' not found")
    
    def do_list_brokers(self, arg):
        """List all brokers with their status."""
        if self.broker_manager.broker_count == 0:
            print("📭 No brokers running")
            return
        
        self._display_broker_status()
    
    def do_broker_details(self, arg):
        """Show detailed information about a specific broker.
        
        Usage: broker_details <broker_name>
        Example: broker_details broker-1
        """
        if not arg:
            print("Error: Broker name required")
            return
        
        if not self.broker_manager.broker_exists(arg):
            print(f"❌ Broker '{arg}' not found")
            return
        
        self._display_broker_details(arg)
    
    def do_start_cluster(self, arg):
        """Start cluster tasks for a group.
        
        Usage: start_cluster <group_name> [proxy_url]
        Examples:
          start_cluster default
          start_cluster mygroup http://127.0.0.1:8000
        """
        args = arg.split()
        if len(args) < 1:
            print("Error: Group name required")
            return
        
        group_name = args[0]
        proxy_url = args[1] if len(args) >= 2 else self.proxy_url
        
        if self.broker_manager.start_cluster_tasks(group_name, proxy_url):
            print(f"✓ Started cluster tasks for group '{group_name}'")
        else:
            print(f"❌ Group '{group_name}' not found")

    # =============================================================================
    # Proxy Management Commands
    # =============================================================================
    
    def do_start_proxy(self, arg):
        """Start the proxy server.
        
        Usage: start_proxy [address:port] [broker_urls...]
        Examples:
          start_proxy
          start_proxy 127.0.0.1:8000
          start_proxy 127.0.0.1:8000 http://127.0.0.1:8001 http://127.0.0.1:8002
        """
        args = arg.split()
        
        try:
            # Parse proxy address
            if args:
                proxy_host, proxy_port = self._parse_address(args[0])
                self.proxy_url = f"http://{proxy_host}:{proxy_port}"
                broker_urls = args[1:] if len(args) > 1 else []
            else:
                proxy_host, proxy_port = "127.0.0.1", 8000
                self.proxy_url = "http://127.0.0.1:8000"
                broker_urls = []
            
            # Create and start proxy
            self.proxy = Proxy(broker_urls)
            self.proxy.start_http_server(proxy_host, proxy_port)
            
            print(f"✓ Started proxy at {self.proxy_url}")
            if broker_urls:
                print(f"  Connected to brokers: {broker_urls}")
            
        except Exception as e:
            print(f"Error starting proxy: {e}")
    
    def do_stop_proxy(self, arg):
        """Stop the proxy server."""
        if self.proxy:
            self.proxy.close()
            self.proxy = None
            print("✓ Stopped proxy")
        else:
            print("❌ No proxy running")
    
    def do_connect_brokers(self, arg):
        """Connect proxy to brokers in a group.
        
        Usage: connect_brokers <group_name>
        Example: connect_brokers default
        """
        if not arg:
            print("Error: Group name required")
            return
        
        if not self.proxy:
            print("❌ No proxy running. Start proxy first.")
            return
        
        if not self.broker_manager.group_exists(arg):
            print(f"❌ Group '{arg}' not found")
            return
        
        broker_urls = self.broker_manager.get_broker_urls(arg)
        if not broker_urls:
            print(f"❌ No brokers found in group '{arg}'")
            return
        
        self.proxy.update_broker_urls(broker_urls)
        print(f"✓ Connected proxy to {len(broker_urls)} brokers in group '{arg}'")
        for url in broker_urls:
            print(f"  - {url}")

    # =============================================================================
    # System Commands
    # =============================================================================
    
    def do_quick_setup(self, arg):
        """Quickly set up a complete system (brokers + proxy).
        
        Usage: quick_setup [group_name] [broker_count]
        Examples:
          quick_setup
          quick_setup mygroup 3
        """
        args = arg.split()
        group_name = args[0] if args else "default"
        broker_count = int(args[1]) if len(args) >= 2 else 3
        
        try:
            print(f"🚀 Setting up system with {broker_count} brokers in group '{group_name}'...")
            
            # Generate broker configs
            configs = []
            base_port = 8001
            
            for i in range(broker_count):
                port = base_port + i
                is_leader = (i == 0)  # First broker is leader
                configs.append(("127.0.0.1", port, is_leader))
            
            # Spawn broker group
            self.broker_manager.spawn_group(group_name, configs)
            
            # Start proxy
            self.proxy = Proxy([])
            self.proxy.start_http_server("127.0.0.1", 8000)
            
            # Connect brokers
            broker_urls = self.broker_manager.get_broker_urls(group_name)
            self.proxy.update_broker_urls(broker_urls)
            
            print(f"✅ System ready!")
            print(f"   Proxy: http://127.0.0.1:8000")
            print(f"   Brokers: {len(broker_urls)} in group '{group_name}'")
            
        except Exception as e:
            print(f"❌ Setup failed: {e}")
    
    def do_cleanup(self, arg):
        """Stop all brokers and proxy."""
        print("🧹 Cleaning up...")
        
        if self.proxy:
            self.proxy.close()
            self.proxy = None
            print("✓ Stopped proxy")
        
        self.broker_manager.stop_all_brokers()
        print("✓ Stopped all brokers")
    
    def do_status(self, arg):
        """Show overall system status."""
        print(f"\n🔍 System Status")
        print("=" * 50)
        
        # Proxy status
        if self.proxy:
            broker_urls = self.proxy.get_broker_urls()
            print(f"📡 Proxy: Running at {self.proxy_url}")
            print(f"   Connected brokers: {len(broker_urls)}")
        else:
            print("📡 Proxy: Not running")
        
        # Broker status
        broker_count = self.broker_manager.broker_count
        group_count = len(self.broker_manager.group_names)
        print(f"🖥️  Brokers: {broker_count} running in {group_count} groups")
        
        if broker_count > 0:
            print()
            self._display_broker_status()

    # =============================================================================
    # Help and Utility Commands
    # =============================================================================
    
    def do_help(self, arg):
        """Show help for commands."""
        if arg:
            super().do_help(arg)
        else:
            print("""
🔧 Debug CLI Commands:

Broker Management:
  spawn_broker <addr:port> [leader|follower] [group]  - Spawn a single broker
  spawn_group <group> <leader_addr> <follower_addr>...  - Spawn broker group
  stop_broker <name>                                   - Stop specific broker
  restart_broker <name>                                - Restart specific broker
  crash_broker <name>                                  - Simulate broker crash
  list_brokers                                         - List all brokers
  broker_details <name>                                - Show broker details
  start_cluster <group> [proxy_url]                    - Start cluster tasks

Proxy Management:
  start_proxy [addr:port] [broker_urls...]             - Start proxy server
  stop_proxy                                           - Stop proxy server
  connect_brokers <group>                              - Connect proxy to brokers

System Operations:
  quick_setup [group] [count]                          - Quick system setup
  cleanup                                              - Stop everything
  status                                               - Show system status

General:
  help [command]                                       - Show command help
  quit, exit                                           - Exit CLI
""")
    
    def do_quit(self, arg):
        """Exit the CLI."""
        print("🧹 Cleaning up before exit...")
        self.do_cleanup("")
        print("👋 Goodbye!")
        return True
    
    def do_exit(self, arg):
        """Exit the CLI."""
        return self.do_quit(arg)
    
    def default(self, line):
        """Handle unknown commands."""
        print(f"❌ Unknown command: '{line}'")
        print("Type 'help' for available commands.")

    # =============================================================================
    # Private Helper Methods
    # =============================================================================
    
    def _parse_address(self, address_str: str) -> tuple[str, int]:
        """Parse address string in format 'host:port'."""
        try:
            host, port_str = address_str.split(":")
            return host, int(port_str)
        except ValueError:
            raise ValueError(f"Invalid address format '{address_str}'. Expected 'host:port'")
    
    def _display_broker_status(self):
        """Display formatted broker status information."""
        all_status = self.broker_manager.get_all_broker_status()
        
        print(f"\n📊 Broker Status ({self.broker_manager.broker_count} brokers)")
        print("=" * 80)
        
        for group_name, brokers in all_status.items():
            if not brokers:
                continue
                
            print(f"\n📁 Group: {group_name}")
            print("-" * 40)
            
            for broker_name, status in brokers.items():
                if status.get("status") == "error":
                    print(f"  ❌ {broker_name} - Error: {status.get('error')}")
                    continue
                
                role = "🟢 Leader" if status.get("is_leader") else "🔵 Follower"
                print(f"  {role} {broker_name}")
                print(f"    URL: {status.get('url')}")
                print(f"    Queues: {status.get('queue_count', 'N/A')}, "
                      f"Messages: {status.get('message_count', 'N/A')}, "
                      f"Clients: {status.get('client_count', 'N/A')}")
                print()
    
    def _display_broker_details(self, broker_name: str):
        """Display detailed information about a specific broker."""
        status = self.broker_manager.get_broker_status(broker_name)
        
        if status.get("status") == "error":
            print(f"❌ Error getting details for {broker_name}: {status.get('error')}")
            return
        
        print(f"\n🔍 Detailed Status for {broker_name}")
        print("=" * 60)
        print(f"URL: {status.get('url')}")
        print(f"Broker ID: {status.get('broker_id')}")
        print(f"Role: {'Leader' if status.get('is_leader') else 'Follower'}")
        
        queues = status.get("queues", [])
        messages = status.get("messages", [])
        pointers = status.get("pointers", [])
        
        print(f"\nQueues ({len(queues)}):")
        for qid in queues:
            print(f"  - Queue {qid}")
        
        print(f"\nMessages ({len(messages)}):")
        for msg in messages:
            print(f"  - Queue {msg['queue_id']}: Message {msg['message_id']} = {msg['value']}")
        
        print(f"\nClient Pointers ({len(pointers)}):")
        for ptr in pointers:
            print(f"  - Client '{ptr['client_id']}' on Queue {ptr['queue_id']}: Message {ptr['message_id']}")


def main():
    """Main entry point for the debug CLI."""
    try:
        cli = DebugCLI()
        cli.cmdloop()
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except EOFError:
        print("\n👋 Goodbye!")


if __name__ == "__main__":
    main()