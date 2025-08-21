# Broker Architecture Documentation

## Overview

The `Broker` class is the core component of the distributed queue system, implementing a replicated message broker with leader-follower architecture. Each broker instance manages queues, messages, and client read pointers while participating in a cluster for high availability and data consistency.

## Core Architecture

### Class Hierarchy
```
HttpServerBase
    ↓
Broker
```

The broker inherits from `HttpServerBase` to provide HTTP server functionality and implements distributed consensus mechanisms for data replication.

### Key Components

1. **Data Layer**: SQLite-based persistent storage via `SqliteManager`
2. **Network Layer**: HTTP server for client and peer communication  
3. **Cluster Layer**: Leader election, heartbeat, and replication logic
4. **State Management**: Thread-safe operations with RLock

## Leadership Model

### Leader Responsibilities
- **Execute Operations**: Process client requests (create queue, append, read)
- **Coordinate Replication**: Send changes to all followers
- **Send Heartbeats**: Maintain cluster health monitoring
- **Handle Client Requests**: Only leaders accept write operations

### Follower Responsibilities  
- **Apply Replicated Operations**: Execute deterministic state changes from leader
- **Monitor Leader Health**: Watch for heartbeat timeouts
- **Participate in Elections**: Vote for new leader when needed
- **Sync with Leader**: Catch up on missed operations after failures

### Leader Election Process
```
1. Follower detects missing heartbeats (timeout: 2 seconds)
2. Query all known peers for availability  
3. Select peer with lowest broker_id as new leader
4. New leader announces itself and starts sending heartbeats
5. Followers update their leader reference
```

## Data Operations

### Write Operations (Leader Only)

#### Create Queue
```python
def create_queue(self) -> int:
    # 1. Generate unique queue_id locally
    queue_id = self._sqlite.create_queue()
    
    # 2. Replicate to all followers
    self._replicate("/apply_create_queue", {"queue_id": queue_id})
    
    # 3. Return queue_id to client
    return queue_id
```

#### Append Message
```python
def append(self, queue_id: int, value: int) -> int:
    # 1. Generate unique message_id and store locally
    message_id = self._sqlite.append_message(queue_id, value)
    
    # 2. Replicate to followers with deterministic data
    self._replicate("/apply_append", {
        "message_id": message_id,
        "queue_id": queue_id, 
        "value": value
    })
    
    # 3. Return message_id to client
    return message_id
```

#### Read Message (with Pointer Advancement)
```python
def read_for_client(self, queue_id: int, client_id: str) -> Optional[Tuple[int, int]]:
    # 1. Get client's current read pointer
    last_read = self._sqlite.get_last_read_message_id(queue_id, client_id)
    
    # 2. Find next unread message
    next_msg = self._sqlite.fetch_next_after(queue_id, last_read)
    if next_msg is None:
        return None
        
    message_id, value = next_msg
    
    # 3. Advance pointer locally
    self._sqlite.set_last_read_message_id(queue_id, client_id, message_id)
    
    # 4. Replicate pointer update to followers
    self._replicate("/apply_set_pointer", {
        "queue_id": queue_id,
        "client_id": client_id, 
        "message_id": message_id
    })
    
    # 5. Return message to client
    return message_id, value
```

### Apply Operations (Followers)

All replicated operations are idempotent and deterministic:

```python
def apply_create_queue(self, queue_id: int) -> ReplicationResult:
    # Only create if queue doesn't exist (idempotent)
    if not self._sqlite.queue_exists(queue_id):
        self._sqlite.create_queue_with_id(queue_id)
    return ReplicationResult(ok=True)

def apply_append(self, message_id: int, queue_id: int, value: int) -> ReplicationResult:
    try:
        # Try to insert with specific message_id
        self._sqlite.append_message_with_id(message_id, queue_id, value)
    except Exception:
        # Ignore if message already exists (idempotent)
        pass
    return ReplicationResult(ok=True)
```

## Replication Strategy

### Synchronous Replication
- Leader waits for followers to acknowledge before returning success
- Allows partial failures (continues if some followers succeed)
- Fails only if ALL followers are unreachable

```python
def _replicate(self, path: str, payload: dict) -> None:
    failures = []
    
    # Send to all followers
    for url in self._follower_urls:
        try:
            self._post_json(url, path, payload)
        except Exception as exc:
            failures.append(f"{url}: {exc}")
    
    # Fail only if ALL followers failed
    if failures and len(failures) == len(self._follower_urls):
        raise RuntimeError("All replication targets failed")
```

### Catch-up Mechanism
Followers periodically sync with leader to recover from failures:

```python
def _catchup_loop(self) -> None:
    while running:
        if leader_is_alive():
            # Sync all data types from leader
            self._sync_queues_from_leader(leader_url)
            self._sync_messages_from_leader(leader_url) 
            self._sync_pointers_from_leader(leader_url)
        time.sleep(5.0)
```

## HTTP API Endpoints

### Client Endpoints (Leader Only)
| Endpoint | Method | Description | Request Body |
|----------|--------|-------------|--------------|
| `/create_queue` | POST | Create new queue | `{}` |
| `/append` | POST | Add message to queue | `{"queue_id": int, "value": int}` |
| `/read` | POST | Read next message | `{"queue_id": int, "client_id": str}` |

### Replication Endpoints (All Brokers)
| Endpoint | Method | Description | Request Body |
|----------|--------|-------------|--------------|
| `/apply_create_queue` | POST | Apply queue creation | `{"queue_id": int}` |
| `/apply_append` | POST | Apply message append | `{"message_id": int, "queue_id": int, "value": int}` |
| `/apply_set_pointer` | POST | Apply pointer update | `{"queue_id": int, "client_id": str, "message_id": int}` |

### Cluster Management Endpoints
| Endpoint | Method | Description | Request Body |
|----------|--------|-------------|--------------|
| `/heartbeat` | POST | Leader heartbeat | `{"leader_id": int, "leader_url": str}` |
| `/broker_info` | POST | Get broker status | `{}` |
| `/set_follower_urls` | POST | Configure followers | `{"follower_urls": [str]}` |
| `/set_peer_urls` | POST | Configure peers | `{"peer_urls": [str]}` |

### Data Access Endpoints
| Endpoint | Method | Description | Response |
|----------|--------|-------------|----------|
| `/get_all_queues` | POST | List all queues | `{"queue_ids": [int]}` |
| `/get_all_messages` | POST | List all messages | `{"messages": [{"message_id", "queue_id", "value"}]}` |
| `/get_all_pointers` | POST | List client pointers | `{"pointers": [{"queue_id", "client_id", "message_id"}]}` |

## Cluster Management

### Heartbeat System
```python
# Leader sends heartbeats every 0.5 seconds
def _heartbeat_loop(self):
    while not stopped:
        if self.is_leader:
            for peer_url in self._peer_urls:
                self._post_json(peer_url, "/heartbeat", {
                    "leader_id": self.broker_id,
                    "leader_url": self.self_url
                })
        time.sleep(0.5)
```

### Failure Detection
```python
# Followers monitor for missing heartbeats
def _monitor_loop(self):
    while not stopped:
        if not self.is_leader:
            if time.time() - last_heartbeat > 2.0:  # timeout
                self._run_leader_election()
        time.sleep(0.5)
```

## Thread Management

The broker runs several background threads:

1. **HTTP Server Thread**: Handles incoming requests
2. **Heartbeat Thread**: Sends/receives cluster heartbeats  
3. **Monitor Thread**: Watches for leader failures
4. **Catchup Thread**: Syncs follower data with leader

### Thread Safety
- All shared state protected by `threading.RLock`
- Database operations are atomic within transactions
- Background threads use event objects for clean shutdown

## Configuration and Lifecycle

### Initialization
```python
broker = Broker(
    broker_id=1,        # Unique identifier
    ip="127.0.0.1",     # Bind address
    port=8001,          # HTTP port
    is_leader=True      # Initial leadership status
)
```

### Startup Sequence
```python
# 1. Start HTTP server
broker.start_http_server()

# 2. Configure cluster relationships  
broker.set_follower_urls(["http://127.0.0.1:8002", "http://127.0.0.1:8003"])
broker.set_peer_urls(["http://127.0.0.1:8001", "http://127.0.0.1:8002", "http://127.0.0.1:8003"])

# 3. Start cluster background tasks
broker.start_cluster_tasks(proxy_url="http://127.0.0.1:8000")
```

### Graceful Shutdown
```python
# Stop cluster tasks first
broker.stop_cluster_tasks()

# Stop HTTP server
broker.stop_http_server()  

# Close database connections
broker.close()
```

## Error Handling and Recovery

### Partial Replication Failures
- System continues operating if some followers fail
- Logs warnings for failed replications
- Failed followers can catch up when they recover

### Leader Failures
- Followers detect timeout and elect new leader
- New leader announces itself to remaining peers
- Proxy can be notified of leadership changes

### Network Partitions
- Minority partitions cannot accept writes (no leader)
- Majority partition continues with new leader
- Partitions reunite when network heals

### Data Consistency
- All operations are deterministic and idempotent
- Message IDs generated by leader ensure ordering
- Catch-up mechanism handles missing operations

## Performance Characteristics

### Throughput
- Limited by leader's processing capacity
- Replication adds latency to write operations
- Read operations served only by leader

### Scalability
- Horizontal scaling through multiple broker groups
- Each group operates independently
- Proxy load-balances across groups

### Durability
- All data persisted to SQLite before replication
- Synchronous replication ensures consistency
- Multiple replicas provide fault tolerance

## Best Practices

### Deployment
- Use odd number of brokers (3, 5, 7) for election majority
- Deploy brokers across different hosts/racks for fault tolerance
- Monitor heartbeat timeouts and adjust for network latency

### Configuration
- Set appropriate heartbeat intervals for your network
- Configure sufficient database storage for message retention
- Use persistent storage for broker data directories

### Monitoring
- Track leader election frequency
- Monitor replication lag and failures
- Alert on extended broker unavailability

### Operations
- Gracefully restart brokers to avoid unnecessary elections
- Use `BrokerManager` for coordinated broker lifecycle management
- Test failover scenarios in development environments
