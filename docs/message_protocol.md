// alternative: clients contact proxy, then connect directly to broker for commands
Client <-> Proxy

- create_queue(client_id) -> queue_id
- append_value(client_id, queue_id, val) -> is_success
- read_queue(client_id, queue_id) -> val

Broker <-> Broker (same group)
- relay_append(queue_id, value)
- apply_append(queue_id, value)
- relay_read(client_id, queue_id)
- apply_read(client_id, queue_id)
- send_ping(broker_id)
- ping()
- send_vote()
- who_leader() -> broker_id
- rec_clients() -> client_id[]
- rec_queues() -> queue_id[]
- rec_queue_data(queue_id) -> value[]

Leader <-> Leader (different groups)
- 