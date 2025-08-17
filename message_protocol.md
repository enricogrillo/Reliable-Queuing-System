Client <-> Broker

- register_client(client_id) -> client_key
- create_queue(client_id, client_key) -> queue_id
- append_value(client_id, client_key, queue_id, val) -> is_success
- read_queue(client_id, client_key, queue_id) -> val
- list_queues(client_id, client_key) -> [queue_id, ...]

Broker <-> Broker

t.b.d