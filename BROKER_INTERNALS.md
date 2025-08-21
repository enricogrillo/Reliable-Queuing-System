# Broker Internals Documentation

## Code Organization

The `Broker` class is organized into logical sections for maintainability:

```python
class Broker(HttpServerBase):
    # =============================================================================
    # Properties and Configuration
    # =============================================================================
    
    # =============================================================================  
    # Cluster Management
    # =============================================================================
    
    # =============================================================================
    # Client Operations (Leader Only) 
    # =============================================================================
    
    # =============================================================================
    # Replication Operations (Apply Commands)
    # =============================================================================
    
    # =============================================================================
    # Replication and Synchronization
    # =============================================================================
    
    # =============================================================================
    # Leader Election and Heartbeat
    # =============================================================================
    
    # =============================================================================
    # HTTP Server and Request Handling
    # =============================================================================
    
    # =============================================================================
    # Utility Methods
    # =============================================================================
```

## State Management

### Core State Variables
```python
# Identity and networking
self.broker_id: int              # Unique broker identifier
self.ip: str                     # IP address to bind to
self.port: int                   # Port to listen on
self.name: str                   # Human-readable name (broker-{id})

# Leadership and clustering
self._is_leader: bool            # Current leadership status
self._follower_urls: List[str]   # URLs of followers (leader only)
self._peer_urls: List[str]       # URLs of all peers in cluster
self._proxy_url: Optional[str]   # Proxy URL for leadership notifications

# Heartbeat and monitoring
self._last_heartbeat_ts: float   # Timestamp of last received heartbeat
self._heartbeat_interval_sec: float = 0.5   # How often to send heartbeats
self._heartbeat_timeout_sec: float = 2.0    # When to consider leader failed

# Thread coordination
self._hb_stop: threading.Event     # Signal to stop heartbeat thread
self._monitor_stop: threading.Event # Signal to stop monitor thread
self._lock: threading.RLock        # Thread synchronization
```

### Thread Safety Strategy
- **RLock Usage**: All shared state modifications protected by `self._lock`
- **Event Coordination**: Clean thread shutdown using `threading.Event`
- **Database Atomicity**: SQLite transactions ensure data consistency
- **Immutable Snapshots**: Copy lists when passing to background threads

```python
# Example of thread-safe state access
def _send_heartbeats(self, peers: List[str]) -> None:
    # peers is already a copy, safe to iterate
    for url in peers:
        try:
            self._post_json(url, "/heartbeat", {
                "leader_id": self.broker_id,
                "leader_url": self.self_url
            })
        except Exception:
            pass  # Continue with other peers
```

## Database Operations

### SQLite Integration
The broker uses `SqliteManager` for persistent storage:

```python
# Queue operations
self._sqlite.create_queue() -> int
self._sqlite.create_queue_with_id(queue_id: int) -> None
self._sqlite.queue_exists(queue_id: int) -> bool

# Message operations  
self._sqlite.append_message(queue_id: int, value: int) -> int
self._sqlite.append_message_with_id(message_id: int, queue_id: int, value: int) -> None
self._sqlite.fetch_next_after(queue_id: int, after_message_id: int) -> Optional[Tuple[int, int]]

# Client pointer operations
self._sqlite.get_last_read_message_id(queue_id: int, client_id: str) -> int
self._sqlite.set_last_read_message_id(queue_id: int, client_id: str, message_id: int) -> None

# Bulk data access (for synchronization)
self._sqlite.get_all_queue_ids() -> List[int]
self._sqlite.get_all_messages() -> List[Dict]
self._sqlite.get_all_pointers() -> List[Dict]
```

### Transaction Handling
- Each operation is atomic within SQLite transactions
- ID generation happens within transactions to ensure uniqueness
- Rollback occurs automatically on exceptions

## HTTP Request Processing

### Request Handler Architecture
```python
class BrokerHandler(JsonRequestHandler):
    def _handle_request(self, path: str, body: dict) -> None:
        # Route lookup with leadership requirements
        routes = {
            "/create_queue": (self._handle_create_queue, True),  # Requires leader
            "/apply_create_queue": (self._handle_apply_create_queue, False),  # No leadership required
            # ... more routes
        }
        
        route_info = routes.get(path)
        if not route_info:
            self._write_json(404, {"error": "not found"})
            return
            
        handler, requires_leader = route_info
        
        # Leadership check
        if requires_leader and not broker.is_leader:
            self._write_json(400, {"error": "not leader"})
            return
            
        handler(body)
```

### Error Response Patterns
```python
# Standard error responses
{"error": "not found"}           # 404 - Unknown endpoint
{"error": "not leader"}          # 400 - Leadership required
{"ok": False, "error": "..."}    # 500 - Operation failed

# Success responses
{"ok": True}                     # Simple success
{"queue_id": 123}               # Resource creation
{"message_id": 456, "value": 100}  # Data retrieval
```

## Replication Implementation

### Deterministic Operations
All replicated operations must be deterministic to ensure consistency:

```python
# ❌ Non-deterministic - generates different IDs on each broker
def bad_replicate_append(self, queue_id: int, value: int):
    message_id = self._sqlite.append_message(queue_id, value)  # Random ID!
    self._replicate("/apply_append", {"queue_id": queue_id, "value": value})

# ✅ Deterministic - leader generates ID, followers use same ID
def good_replicate_append(self, queue_id: int, value: int):
    message_id = self._sqlite.append_message(queue_id, value)  # Leader generates
    self._replicate("/apply_append", {
        "message_id": message_id,  # Followers use leader's ID
        "queue_id": queue_id,
        "value": value
    })
```

### Idempotent Apply Operations
Followers must handle duplicate operations gracefully:

```python
def apply_append(self, message_id: int, queue_id: int, value: int) -> ReplicationResult:
    try:
        with self._lock:
            try:
                # Try to insert with specific message_id
                self._sqlite.append_message_with_id(message_id, queue_id, value)
            except Exception:
                # Idempotency: ignore if message already exists
                # This handles duplicate replication attempts
                pass
        return ReplicationResult(ok=True)
    except Exception as exc:
        return ReplicationResult(ok=False, error=str(exc))
```

### Replication Error Handling
```python
def _replicate(self, path: str, payload: dict) -> None:
    failures = []
    
    # Attempt replication to all followers
    for url in list(self._follower_urls):  # Copy to avoid race conditions
        try:
            self._post_json(url, path, payload)
        except Exception as exc:
            failures.append(f"{url}: {exc}")
            print(f"Replication failed to {url}: {exc}")  # Log but continue
    
    # Decision logic for partial failures
    if failures and len(failures) == len(self._follower_urls):
        # ALL followers failed - this is critical
        raise RuntimeError(f"All replication targets failed: {'; '.join(failures)}")
    elif failures:
        # SOME followers failed - log warning but continue
        print(f"Partial replication failures (continuing): {failures}")
        # System remains available with reduced redundancy
```

## Leader Election Algorithm

### Election Trigger Conditions
```python
def _monitor_loop(self) -> None:
    while not self._monitor_stop.is_set():
        time.sleep(self._heartbeat_interval_sec)
        
        with self._lock:
            is_leader = self._is_leader
            last_hb = self._last_heartbeat_ts
            peers = list(self._peer_urls)
            proxy_url = self._proxy_url
        
        # Only followers monitor for leader failure
        if is_leader:
            continue
        
        # Check for leader timeout
        if time.time() - last_hb > self._heartbeat_timeout_sec:
            self._run_leader_election(peers, proxy_url)
```

### Consensus Algorithm
```python
def _run_leader_election(self, peers: List[str], proxy_url: Optional[str]) -> None:
    candidates = []
    
    # Query all peers (including self) for availability
    for url in peers + [self.self_url]:
        try:
            info = self._post_json(url, "/broker_info", {})
            candidates.append((int(info["broker_id"]), str(info["url"])))
        except Exception:
            # Peer is unreachable, exclude from election
            pass
    
    if not candidates:
        return  # No reachable peers
    
    # Deterministic leader selection: lowest broker_id wins
    candidates.sort(key=lambda x: x[0])
    winner_id, winner_url = candidates[0]
    
    # Check if this broker won the election
    if winner_url == self.self_url:
        self._become_leader(candidates, proxy_url)
```

### Leadership Transition
```python
def _become_leader(self, candidates: List[Tuple[int, str]], proxy_url: Optional[str]) -> None:
    with self._lock:
        # Update local state
        self._is_leader = True
        self._last_heartbeat_ts = time.time()
        
        # Set followers to all other reachable brokers
        self._follower_urls = [url for (_id, url) in candidates if url != self.self_url]
    
    # Notify proxy of leadership change (if configured)
    if proxy_url:
        try:
            self._post_json(proxy_url, "/set_leader", {"leader_url": self.self_url})
        except Exception:
            pass  # Continue even if proxy notification fails
```

## Synchronization and Recovery

### Catch-up Strategy
```python
def _catchup_loop(self) -> None:
    """Background task for followers to sync with leader."""
    while not self._monitor_stop.is_set():
        try:
            with self._lock:
                last_hb = self._last_heartbeat_ts
            
            # Only sync if leader is alive (recent heartbeat)
            if time.time() - last_hb <= self._heartbeat_timeout_sec:
                self._sync_with_leader()
            
            time.sleep(5.0)  # Check every 5 seconds
        except Exception:
            time.sleep(5.0)  # Continue after errors
```

### Data Synchronization
```python
def _sync_with_leader(self) -> None:
    try:
        # 1. Find current leader
        leader_url = self._find_current_leader()
        if not leader_url:
            return
        
        # 2. Sync all data types in order
        self._sync_queues_from_leader(leader_url)    # Create missing queues
        self._sync_messages_from_leader(leader_url)  # Add missing messages  
        self._sync_pointers_from_leader(leader_url)  # Update client pointers
        
    except Exception as exc:
        print(f"Sync failed for {self.name}: {exc}")
        # Continue - will retry on next loop iteration
```

### Conflict Resolution
The system avoids conflicts through:
- **Leader Authority**: Only leaders generate new IDs
- **Deterministic Ordering**: Message IDs establish total order
- **Idempotent Operations**: Duplicate applications are safe
- **Catch-up Reconciliation**: Followers sync missing data from leader

## Memory Management

### Resource Cleanup
```python
def close(self) -> None:
    """Clean shutdown of all broker resources."""
    # 1. Stop background tasks
    self.stop_cluster_tasks()
    
    # 2. Stop HTTP server (inherited from HttpServerBase)
    self.stop_http_server()
    
    # 3. Close database connections
    self._sqlite.close()
    
    # 4. Threading resources cleaned up automatically
    #    (Events and locks are garbage collected)
```

### Static Resource Management
```python
class Broker:
    # Track resource usage across all broker instances
    _used_ids: set[int] = set()
    _used_addresses: set[Tuple[str, int]] = set()
    
    def _ensure_unique_broker(self, broker_id: int, ip: str, port: int) -> None:
        # Allow reuse when previous broker is properly closed
        Broker._used_ids.discard(broker_id)
        Broker._used_addresses.discard((ip, port))
        
        # Reserve resources for this broker
        Broker._used_ids.add(broker_id)
        Broker._used_addresses.add((ip, port))
```

## Performance Considerations

### Critical Paths
1. **Client Write Operations**: Database write + replication network calls
2. **Heartbeat Processing**: Must be fast to avoid false timeouts
3. **Leader Election**: Should complete quickly to minimize unavailability

### Optimization Strategies
- **Connection Reuse**: HTTP client connections reused where possible
- **Async Replication**: Consider async replication for better throughput
- **Batch Operations**: Group multiple operations into single transactions
- **Read Optimization**: Could allow followers to serve read-only queries

### Monitoring Points
```python
# Key metrics to track:
# - Replication latency per follower
# - Leader election frequency
# - Database operation times
# - HTTP request/response times
# - Thread pool utilization
```

## Debugging and Troubleshooting

### Common Issues
1. **Split Brain**: Multiple leaders due to network partition
2. **Replication Lag**: Followers falling behind leader
3. **Election Storms**: Frequent leader changes
4. **Resource Leaks**: Threads or connections not cleaned up

### Debug Information
```python
# Accessible via HTTP endpoints:
GET /broker_info          # Leadership status, broker_id
GET /get_all_queues       # Queue inventory  
GET /get_all_messages     # Message inventory
GET /get_all_pointers     # Client read positions

# Internal state inspection:
broker._is_leader         # Current leadership status
broker._follower_urls     # Configured followers
broker._peer_urls         # Known peers
broker._last_heartbeat_ts # Last heartbeat time
```

### Logging Strategy
- **Info Level**: Leadership changes, replication failures
- **Debug Level**: Heartbeat activity, election attempts  
- **Error Level**: Database errors, network failures
- **Trace Level**: Individual operation execution
