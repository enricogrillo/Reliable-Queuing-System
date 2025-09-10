#!/usr/bin/env python3
"""
Common CLI functionality shared between broker_spawner and client_cli
"""

import os
import socket
import sys
import json
from abc import ABC, abstractmethod

# Enable command history and line editing
try:
    import readline
    # Set up command history
    readline.parse_and_bind("tab: complete")
    readline.parse_and_bind("set editing-mode emacs")
except ImportError:
    # readline not available on some platforms (like Windows)
    readline = None


class IPManager:
    """Manages IP address aliases and discovery."""
    
    def __init__(self, config_file: str = None):
        self.aliases = {
            'loc': '127.0.0.1',
            'lan': self._discover_lan_ip()
        }
        if config_file is None:
            data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
            os.makedirs(data_dir, exist_ok=True)
            self.config_file = os.path.join(data_dir, 'ip_aliases.json')
        else:
            self.config_file = config_file
        self.custom_aliases = self._load_custom_aliases()
    
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
    
    def _load_custom_aliases(self) -> dict:
        """Load custom aliases from persistent storage."""
        aliases = {}
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            try:
                                name, ip = line.split('=', 1)
                                name, ip = name.strip(), ip.strip()
                                if ip == "REMOVED":
                                    aliases.pop(name, None)
                                else:
                                    aliases[name] = ip
                            except:
                                pass
        except Exception:
            pass
        return aliases
    
    def _save_alias(self, name: str, ip: str):
        """Append alias to persistent storage."""
        try:
            with open(self.config_file, 'a') as f:
                f.write(f"{name}={ip}\n")
        except Exception:
            pass
    
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
        self._save_alias(name, ip)
        return f"Added alias '{name}' -> {ip}"
    
    def remove_alias(self, name: str) -> str:
        """Remove custom IP alias."""
        if name in ['loc', 'lan']:
            return f"Cannot remove built-in alias '{name}'"
        if name in self.custom_aliases:
            self.custom_aliases.pop(name)
            self._save_alias(name, "REMOVED")
            return f"Removed alias '{name}'"
        return f"Alias '{name}' not found"
    
    def resolve_ip(self, addr: str) -> str:
        """Resolve alias to IP address."""
        # Refresh custom aliases from file
        self.custom_aliases = self._load_custom_aliases()
        if addr in self.aliases:
            return self.aliases[addr]
        if addr in self.custom_aliases:
            return self.custom_aliases[addr]
        return addr  # Return as-is if not an alias
    
    def show_mappings(self) -> str:
        """Show all IP mappings."""
        # Refresh custom aliases from file
        self.custom_aliases = self._load_custom_aliases()
        lines = ["IP mappings:"]
        lines.append(f"- loc: {self.aliases['loc']} (localhost)")
        lines.append(f"- lan: {self.aliases['lan']} (auto-discovered)")
        for name, ip in self.custom_aliases.items():
            lines.append(f"- {name}: {ip} (custom)")
        return "\n".join(lines)


class BaseCLI(ABC):
    """Base class for CLI applications with common functionality."""
    
    def __init__(self, history_file_name: str = ".cli_history"):
        self.ip_manager = IPManager()
        self.running = True
        self.history_file = os.path.expanduser(f"~/{history_file_name}")
        
        # Setup readline and load history
        self._setup_readline()
        self._load_history()
    
    def _setup_readline(self):
        """Setup readline for command history and completion."""
        if readline is None:
            return
            
        try:
            # Load existing history
            readline.read_history_file(self.history_file)
        except FileNotFoundError:
            pass
        except Exception:
            # Don't fail startup due to history issues
            pass
        
        # Set history length
        readline.set_history_length(1000)
        
        # Setup tab completion with basic commands
        basic_commands = ['h', 'help', 'ip', 'fi', 'a', 'ua', 'x', 'quit', 'exit']
        app_commands = self.get_command_list()
        all_commands = basic_commands + app_commands
        
        def completer(text, state):
            options = [cmd for cmd in all_commands if cmd.startswith(text)]
            if state < len(options):
                return options[state]
            return None
        
        readline.set_completer(completer)
        readline.parse_and_bind("tab: complete")
    
    def _load_history(self):
        """Load command history from file."""
        if readline is None:
            return
            
        try:
            readline.read_history_file(self.history_file)
        except FileNotFoundError:
            # No history file yet, that's fine
            pass
        except Exception:
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
        except Exception:
            # Don't fail due to history saving issues
            pass
    
    def run(self):
        """Main CLI loop."""
        print(self.get_app_banner())
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
        except Exception as e:
            print(f"Fatal error: {e}")
            if "--debug" in sys.argv:
                import traceback
                traceback.print_exc()
        finally:
            self.cleanup()
    
    def _process_command(self, command: str):
        """Process a single command."""
        parts = command.split()
        if not parts:
            return
        
        cmd = parts[0].lower()
        args = parts[1:]
        
        try:
            # Handle common commands first
            if cmd in ['h', 'help']:
                self._cmd_help()
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
                # Delegate to application-specific command handling
                if not self.handle_app_command(cmd, args):
                    print(f"Unknown command: {cmd}. Type 'h' for help.")
        except Exception as e:
            print(f"Error: {e}")
            if "--debug" in sys.argv:
                import traceback
                traceback.print_exc()
    
    # IP management commands
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
    
    def _cmd_help(self):
        """Display help information."""
        print(self.get_help_text())
        print()
        print("Common IP Management Commands:")
        print("  ip             - Show IP mappings (loc, lan, custom aliases)")
        print("  fi             - Refresh LAN IP discovery")
        print("  a <name> <ip>  - Add custom IP alias")
        print("  ua <name>      - Remove custom IP alias")
        print()
        print("Address Aliases:")
        print("  • loc          - localhost (127.0.0.1)")
        print("  • lan          - auto-discovered LAN IP")
        print("  • Custom aliases can be added with 'a' command")
        print()
        print("Utility:")
        print("  h, help        - Show this help")
        print("  x, quit, exit  - Exit the CLI")
    
    def cleanup(self):
        """Cleanup resources before exit."""
        self._save_history()
        self.handle_cleanup()
    
    # Abstract methods that must be implemented by subclasses
    @abstractmethod
    def get_app_banner(self) -> str:
        """Return the application banner/title."""
        pass
    
    @abstractmethod
    def get_help_text(self) -> str:
        """Return application-specific help text."""
        pass
    
    @abstractmethod
    def get_command_list(self) -> list:
        """Return list of application-specific commands for tab completion."""
        pass
    
    @abstractmethod
    def handle_app_command(self, cmd: str, args: list) -> bool:
        """Handle application-specific commands. Return True if handled, False if unknown."""
        pass
    
    def handle_cleanup(self):
        """Handle application-specific cleanup. Override if needed."""
        pass
