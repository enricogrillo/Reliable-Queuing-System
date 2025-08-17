import socket

class Broker:
    def __init__(self, host='localhost', port=65432):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self.host, self.port))
        self.sock.listen()

    def start(self):
        print(f"Broker listening on {self.host}:{self.port}")
        while True:
            conn, addr = self.sock.accept()
            print(f"Connected by {addr}")
            self.handle_client(conn)

    def handle_client(self, conn):
        with conn:
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                print(f"Received: {data.decode()}")
                response = "Message received"
                conn.sendall(response.encode())
                print(f"Sent: {response}")

    def close(self):
        self.sock.close()
        print("Broker closed.")

if __name__ == "__main__":
    broker = Broker()
    broker.start()