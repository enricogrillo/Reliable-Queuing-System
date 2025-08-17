import time
from broker import Broker
from client import Client
from commands import Command
import json

from threading import Thread

if __name__ == "__main__":
    broker = Broker(broker_id="broker-1", port=5050)
    Thread(target=broker.start, daemon=True).start()
    time.sleep(1)  # Allow the broker to start

    client = Client(client_id="clientA", port=5050)

    print("Registering...")
    client_key = client.send_command(Command.REGISTER_CLIENT, "clientA")
    print("Client key:", client_key)

    print("Creating queue...")
    queue_id = client.send_command(Command.CREATE_QUEUE, "clientA", client_key)
    print("Queue ID:", queue_id)

    print("Appending to queue...")
    client.send_command(Command.APPEND_VALUE, "clientA", client_key, queue_id, "Hello")
    client.send_command(Command.APPEND_VALUE, "clientA", client_key, queue_id, "World")

    print("Reading from queue...")
    print(client.send_command(Command.READ_QUEUE, "clientA", client_key, queue_id))  # Hello
    print(client.send_command(Command.READ_QUEUE, "clientA", client_key, queue_id))  # World
    print(client.send_command(Command.READ_QUEUE, "clientA", client_key, queue_id))  # None

    print("Listing queues...")
    print(client.send_command(Command.LIST_QUEUES, "clientA", client_key))

    client.close()
