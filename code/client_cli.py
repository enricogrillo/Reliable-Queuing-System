#!/usr/bin/env python3
"""
Command-line interface for the queue system client.
Allows users to create queues, append values, and read messages from the terminal.
"""

import argparse
import sys
import cmd
from typing import Optional, Tuple

from .client import Client


def parse_address(address_str: str) -> Tuple[str, int]:
    """Parse address string in format 'host:port'."""
    try:
        host, port_str = address_str.split(":")
        return host, int(port_str)
    except ValueError:
        print(f"Error: Invalid address format '{address_str}'. Expected 'host:port'")
        sys.exit(1)


class InteractiveCLI(cmd.Cmd):
    """Interactive command-line interface for the queue system."""
    
    intro = """
Queue System Interactive CLI
Type 'help' for available commands, 'quit' to exit.
"""
    prompt = "queue> "
    
    def __init__(self, client: Client):
        super().__init__()
        self.client = client
        self.client_id = client.client_id
    
    def do_create_queue(self, arg):
        """Create a new queue.
        
        Usage: create_queue
        """
        try:
            queue_id = self.client.create_queue()
            print(f"Created queue: {queue_id}")
        except Exception as e:
            print(f"Error creating queue: {e}")
    
    def do_append(self, arg):
        """Append a value to a queue.
        
        Usage: append <queue_id> <value>
        Example: append 1 100
        """
        try:
            args = arg.split()
            if len(args) != 2:
                print("Error: append requires exactly 2 arguments: queue_id and value")
                return
            
            queue_id = int(args[0])
            value = int(args[1])
            
            message_id = self.client.append(queue_id, value)
            print(f"Appended {value} to queue {queue_id} (message_id: {message_id})")
        except ValueError:
            print("Error: queue_id and value must be integers")
        except Exception as e:
            print(f"Error appending to queue: {e}")
    
    def do_read(self, arg):
        """Read next message from a queue.
        
        Usage: read <queue_id> [as <client_id>]
        Examples: 
          read 1
          read 1 as bob
        """
        try:
            args = arg.split()
            if len(args) < 1:
                print("Error: read requires at least queue_id")
                return
            
            queue_id = int(args[0])
            client_id = None
            
            # Check for "as <client_id>" pattern
            if len(args) >= 3 and args[1] == "as":
                client_id = args[2]
            
            result = self.client.read(queue_id, client_id)
            
            if result is None:
                client_display = f" for client '{client_id}'" if client_id else ""
                print(f"No messages available in queue {queue_id}{client_display}")
            else:
                message_id, value = result
                client_display = f" as '{client_id}'" if client_id else ""
                print(f"Read from queue {queue_id}{client_display}: ({message_id}, {value})")
        except ValueError:
            print("Error: queue_id must be an integer")
        except Exception as e:
            print(f"Error reading from queue: {e}")
    
    def do_status(self, arg):
        """Show current client status.
        
        Usage: status
        """
        print(f"Client ID: {self.client_id}")
        print(f"Proxy URL: {self.client._proxy_url}")
    
    def do_help(self, arg):
        """Show help for commands."""
        if arg:
            # Show help for specific command
            super().do_help(arg)
        else:
            # Show general help
            print("""
Available commands:
  create_queue                    - Create a new queue
  append <queue_id> <value>       - Append value to queue
  read <queue_id> [as <client>]   - Read next message from queue
  status                          - Show current client status
  help [command]                  - Show help for command
  quit, exit                      - Exit the CLI

Examples:
  create_queue
  append 1 100
  read 1
  read 1 as bob
  status
""")
    
    def do_quit(self, arg):
        """Exit the CLI."""
        print("Goodbye!")
        return True
    
    def do_exit(self, arg):
        """Exit the CLI."""
        return self.do_quit(arg)
    
    def default(self, line):
        """Handle unknown commands."""
        print(f"Unknown command: {line}")
        print("Type 'help' for available commands.")


def run_interactive(client: Client) -> None:
    """Run the interactive CLI."""
    try:
        cli = InteractiveCLI(client)
        cli.cmdloop()
    except KeyboardInterrupt:
        print("\nGoodbye!")
    except EOFError:
        print("\nGoodbye!")


def run_single_command(client: Client, command: str, args: list) -> None:
    """Run a single command and exit."""
    try:
        if command == "create-queue":
            queue_id = client.create_queue()
            print(f"Created queue: {queue_id}")
            
        elif command == "append":
            if len(args) != 2:
                print("Error: append requires exactly 2 arguments: queue_id and value")
                sys.exit(1)
            
            queue_id = int(args[0])
            value = int(args[1])
            message_id = client.append(queue_id, value)
            print(f"Appended {value} to queue {queue_id} (message_id: {message_id})")
            
        elif command == "read":
            if len(args) < 1:
                print("Error: read requires at least queue_id")
                sys.exit(1)
            
            queue_id = int(args[0])
            client_id = None
            
            # Check for "as <client_id>" pattern
            if len(args) >= 3 and args[1] == "as":
                client_id = args[2]
            
            result = client.read(queue_id, client_id)
            
            if result is None:
                client_display = f" for client '{client_id}'" if client_id else ""
                print(f"No messages available in queue {queue_id}{client_display}")
            else:
                message_id, value = result
                client_display = f" as '{client_id}'" if client_id else ""
                print(f"Read from queue {queue_id}{client_display}: ({message_id}, {value})")
                
    except ValueError as e:
        print(f"Error: Invalid argument format - {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error executing command '{command}': {e}")
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Queue System Client CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Interactive mode
  python -m code.client_cli --proxy 127.0.0.1:8000 --client alice

  # Single command mode
  python -m code.client_cli --proxy 127.0.0.1:8000 --client alice create-queue
  python -m code.client_cli --proxy 127.0.0.1:8000 --client alice append 1 100
  python -m code.client_cli --proxy 127.0.0.1:8000 --client alice read 1

  # Read as different client
  python -m code.client_cli --proxy 127.0.0.1:8000 --client alice read 1 as bob
        """
    )
    
    parser.add_argument(
        "--proxy", 
        required=True, 
        help="Proxy address in format 'host:port' (e.g., '127.0.0.1:8000')"
    )
    parser.add_argument(
        "--client", 
        required=True, 
        help="Client ID for this session"
    )
    
    # Optional command and arguments for single command mode
    parser.add_argument(
        "command", 
        nargs="?", 
        choices=["create-queue", "append", "read"],
        help="Command to execute (if not provided, runs in interactive mode)"
    )
    parser.add_argument(
        "args", 
        nargs="*", 
        help="Arguments for the command"
    )
    
    args = parser.parse_args()
    
    # Parse proxy address
    proxy_address = parse_address(args.proxy)
    
    # Create client
    try:
        client = Client(args.client, proxy_address)
    except Exception as e:
        print(f"Error connecting to proxy at {args.proxy}: {e}")
        sys.exit(1)
    
    # Run in appropriate mode
    if args.command:
        # Single command mode
        run_single_command(client, args.command, args.args)
    else:
        # Interactive mode
        run_interactive(client)


if __name__ == "__main__":
    main()
