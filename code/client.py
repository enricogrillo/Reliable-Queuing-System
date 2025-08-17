import socket
import json


from commands import Command


class Client:
    def __init__(self, client_id, host='localhost', port=5000):
        self.id = client_id
        self.sock = socket.create_connection((host, port))
        self.buffer = ""

    def send_command(self, command: Command, *params):
        request = {
            "command": command.value,
            "params": params
        }
        self.sock.sendall((json.dumps(request) + "\n").encode())
        return self._receive_response()

    def _receive_response(self):
        while True:
            data = self.sock.recv(4096)
            if not data:
                raise Exception("Connection closed")
            self.buffer += data.decode()
            if "\n" in self.buffer:
                line, self.buffer = self.buffer.split("\n", 1)
                response = json.loads(line)
                if response["status"] == "ok":
                    return response["result"]
                else:
                    raise Exception(response["message"])

    def close(self):
        self.sock.close()
