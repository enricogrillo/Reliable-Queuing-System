import socket

class Client:
    def __init__(self, host='localhost', port=65432):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def connect(self):
        self.sock.connect((self.host, self.port))
        print(f"Connected to broker at {self.host}:{self.port}")

    def send_message(self, message):
        self.sock.sendall(message.encode())
        print(f"Sent: {message}")

    def receive_response(self):
        response = self.sock.recv(1024).decode()
        print(f"Received: {response}")
        return response

    def close(self):
        self.sock.close()
        print("Connection closed.")




if __name__ == "__main__":
    client = Client()
    client.connect()
    client.send_message("Hello, Broker!")
    client.receive_response()
    client.close()