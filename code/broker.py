import socket
import threading
import json

from sqlitemanager import SqliteManager
from commands import Command

class Broker:
    def __init__(self, broker_id, host='localhost', port=5000):
        self.id = broker_id
        self.host = host
        self.port = port
        self.db = SqliteManager()
        self.command_handlers = {
            Command.REGISTER_CLIENT.value: self.handle_register_client,
            Command.CREATE_QUEUE.value: self.handle_create_queue,
            Command.APPEND_VALUE.value: self.handle_append_value,
            Command.READ_QUEUE.value: self.handle_read_queue,
            Command.LIST_QUEUES.value: self.handle_list_queues,
        }

    def start(self):
        print(f"[BROKER {self.id}] Listening on {self.host}:{self.port}")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
            server_sock.bind((self.host, self.port))
            server_sock.listen()
            while True:
                client_sock, _ = server_sock.accept()
                threading.Thread(target=self.handle_client, args=(client_sock,), daemon=True).start()

    def handle_client(self, sock):
        with sock:
            buffer = ""
            while True:
                data = sock.recv(4096)
                if not data:
                    break
                buffer += data.decode()
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    response = self.process_request(line)
                    sock.sendall((json.dumps(response) + "\n").encode())

    def process_request(self, line):
        try:
            request = json.loads(line)
            command = request.get("command")
            params = request.get("params", [])
            handler = self.command_handlers.get(command)
            if handler:
                return {"status": "ok", "result": handler(*params)}
            else:
                return {"status": "error", "message": "Unknown command"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    # Command handlers
    def handle_register_client(self, client_id):
        return self.db.register_client(client_id)

    def handle_create_queue(self, client_id, client_key):
        if not self.db.authenticate(client_id, client_key):
            raise Exception("Authentication failed")
        return self.db.create_queue(client_id)

    def handle_append_value(self, client_id, client_key, queue_id, val):
        if not self.db.authenticate(client_id, client_key):
            raise Exception("Authentication failed")
        return self.db.append_value(queue_id, val)

    def handle_read_queue(self, client_id, client_key, queue_id):
        if not self.db.authenticate(client_id, client_key):
            raise Exception("Authentication failed")
        return self.db.read_queue(queue_id)

    def handle_list_queues(self, client_id, client_key):
        if not self.db.authenticate(client_id, client_key):
            raise Exception("Authentication failed")
        return self.db.list_queues(client_id)
