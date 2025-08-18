Client <-> Proxy

- create_queue(client_id) -> queue_id
- append_value(client_id, queue_id, val) -> is_success
- read_queue(client_id, queue_id) -> val

Broker <-> Broker (same group)
- relay_append(client_id, queue_id, value)
- apply_append(client_id, queue_id, value)
- relay_read(client_id, queue_id)
- apply_read(client_id, queue_id)
- send_ping(broker_id)
- ping()
- send_vote()
- who_leader()
- rec_clients() -> clients
- rec_queues() -> queues
- rec_queue_data(queue_id) -> values

Leader <-> Leader (different groups)
- 