# Message Exchange Sequence Diagrams

This document contains comprehensive sequence diagrams for the distributed broker system, illustrating all major message exchange patterns and communication flows.

## Table of Contents

1. [Client Cluster Discovery Flow](#1-client-cluster-discovery-flow)
2. [Write Operation with Replication](#2-write-operation-with-replication)
3. [Read Operation Flow](#3-read-operation-flow)
4. [Broker Join Cluster Process](#4-broker-join-cluster-process)
5. [Leader Election Process](#5-leader-election-process)
6. [Heartbeat Monitoring and Failure Detection](#6-heartbeat-monitoring-and-failure-detection)
7. [Complete System Overview](#7-complete-system-overview)

---

## 1. Client Cluster Discovery Flow

Shows how clients discover cluster topology and establish connections to brokers.

```mermaid
sequenceDiagram
    participant Client
    participant SeedBroker as Seed Broker
    participant Leader as Leader Broker
    participant Replica as Replica Broker

    Note over Client, Replica: Client Cluster Discovery Flow

    Client->>SeedBroker: 1. Connect to seed broker
    Client->>SeedBroker: 2. CLUSTER_QUERY<br/>{"operation": "CLUSTER_QUERY", "client_id": "C-ABC123"}
    
    SeedBroker->>Client: 3. Cluster topology response<br/>{"status": "success", "brokers": [...], "cluster_id": "...", "cluster_version": 42}
    
    Note over Client: Client learns about all brokers,<br/>their roles (leader/replica), and network addresses
    
    Client->>Leader: 4. Establish connection to leader
    Client->>Replica: 5. Establish connection to replica
    
    Note over Client, Replica: Client now has complete cluster view<br/>and can perform operations
```

**Key Messages:**
- `CLUSTER_QUERY`: Client requests cluster topology
- Response includes all broker information, roles, and network addresses
- Client establishes connections based on discovered topology

---

## 2. Write Operation with Replication

Demonstrates the complete write operation flow including replication and consensus.

```mermaid
sequenceDiagram
    participant Client
    participant Leader as Leader Broker
    participant Replica1 as Replica Broker 1
    participant Replica2 as Replica Broker 2

    Note over Client, Replica2: Write Operation with Replication (CREATE_QUEUE or APPEND)

    Client->>Leader: 1. APPEND message<br/>{"operation": "APPEND", "queue_name": "Q-123", "data": 42}
    
    Note over Leader: Leader writes to local SQLite<br/>with sequence number
    
    par Parallel Replication to All Replicas
        Leader->>Replica1: 2a. REPLICATE<br/>{"operation": "REPLICATE", "type": "APPEND_MESSAGE", "queue_id": "Q-123", "sequence_num": 15, "data": 42}
        Leader->>Replica2: 2b. REPLICATE<br/>{"operation": "REPLICATE", "type": "APPEND_MESSAGE", "queue_id": "Q-123", "sequence_num": 15, "data": 42}
    end
    
    par Majority ACK Required
        Replica1->>Leader: 3a. {"status": "success"}
        Replica2->>Leader: 3b. {"status": "success"}
    end
    
    Note over Leader: Leader waits for majority ACK<br/>⌈R/2⌉ where R = active replicas
    
    Note over Leader: Commit transaction after<br/>majority consensus achieved
    
    Leader->>Client: 4. Response<br/>{"status": "success", "sequence_num": 15}
    
    Note over Leader, Replica2: Async completion continues<br/>for remaining replicas if any
```

**Key Messages:**
- `APPEND` / `CREATE_QUEUE`: Client write operations
- `REPLICATE`: Leader-to-replica data synchronization
- Majority consensus required for commit
- Strong consistency guarantees

---

## 3. Read Operation Flow

Shows how clients read messages with position tracking and consistency guarantees.

```mermaid
sequenceDiagram
    participant Client
    participant Leader as Leader Broker
    participant SQLite as Local SQLite

    Note over Client, SQLite: Read Operation Flow (Leader-Only)

    Client->>Leader: 1. READ request<br/>{"operation": "READ", "queue_name": "Q-123", "client_id": "C-ABC123"}
    
    Note over Leader: Only leaders serve reads<br/>for strong consistency
    
    Leader->>SQLite: 2. Get client read position<br/>SELECT read_position FROM client_positions<br/>WHERE client_id='C-ABC123' AND queue_id='Q-123'
    
    SQLite->>Leader: 3. Current position (e.g., position = 14)
    
    Leader->>SQLite: 4. Fetch next message<br/>SELECT * FROM queue_data<br/>WHERE queue_id='Q-123' AND sequence_num > 14<br/>ORDER BY sequence_num LIMIT 1
    
    alt Message Available
        SQLite->>Leader: 5a. Message data<br/>{"sequence_num": 15, "data": 42}
        
        Leader->>SQLite: 6a. Update client position<br/>UPDATE client_positions<br/>SET read_position = 15<br/>WHERE client_id='C-ABC123' AND queue_id='Q-123'
        
        Leader->>Client: 7a. Success response<br/>{"status": "success", "sequence_num": 15, "data": 42}
    else No Messages
        SQLite->>Leader: 5b. No data found
        Leader->>Client: 7b. No messages response<br/>{"status": "success", "message": "no_messages"}
    end

    Note over Client, SQLite: Client position is maintained per-client<br/>and replicated to replicas for consistency
```

**Key Messages:**
- `READ`: Client read request
- Leader-only operations for strong consistency
- Per-client position tracking
- Position updates replicated to replicas

---

## 4. Broker Join Cluster Process

Illustrates how new brokers join an existing cluster and receive initial state.

```mermaid
sequenceDiagram
    participant NewBroker as New Broker
    participant SeedBroker as Seed Broker
    participant Leader as Leader Broker
    participant Replica1 as Existing Replica 1
    participant Replica2 as Existing Replica 2

    Note over NewBroker, Replica2: Broker Join Cluster Process

    NewBroker->>SeedBroker: 1. Contact seed broker for discovery
    SeedBroker->>NewBroker: 2. Current leader information
    
    NewBroker->>Leader: 3. JOIN_CLUSTER request<br/>{"operation": "JOIN_CLUSTER", "broker_id": "B-NEW001", "host": "127.0.0.1", "port": 9403, "cluster_id": "G-XYZ789"}
    
    Note over Leader: Validate cluster_id and<br/>assign replica role
    
    Leader->>NewBroker: 4. Join response with cluster state<br/>{"status": "success", "cluster_info": {...}, "data_snapshot": {...}}
    
    Note over NewBroker: New broker receives:<br/>- Complete cluster member list<br/>- All queue data and messages<br/>- Client read positions
    
    par Membership Update to All Brokers
        Leader->>Replica1: 5a. CLUSTER_UPDATE<br/>{"operation": "CLUSTER_UPDATE", "cluster_version": 44, "members": {...}}
        Leader->>Replica2: 5b. CLUSTER_UPDATE<br/>{"operation": "CLUSTER_UPDATE", "cluster_version": 44, "members": {...}}
        Leader->>NewBroker: 5c. CLUSTER_UPDATE<br/>{"operation": "CLUSTER_UPDATE", "cluster_version": 44, "members": {...}}
    end
    
    par ACK from All Brokers
        Replica1->>Leader: 6a. {"status": "success"}
        Replica2->>Leader: 6b. {"status": "success"}
        NewBroker->>Leader: 6c. {"status": "success"}
    end
    
    Note over NewBroker, Replica2: New broker is now active replica<br/>receiving heartbeats and replication
```

**Key Messages:**
- `JOIN_CLUSTER`: New broker requests cluster membership
- Complete data snapshot transfer
- `CLUSTER_UPDATE`: Cluster-wide membership synchronization
- Dynamic cluster membership management

---

## 5. Leader Election Process

Shows the complete leader election flow when a leader fails.

```mermaid
sequenceDiagram
    participant OldLeader as Failed Leader
    participant Candidate as Candidate Broker<br/>(Lowest ID Replica)
    participant Replica1 as Replica Broker 1
    participant Replica2 as Replica Broker 2
    participant Client

    Note over OldLeader, Client: Leader Election Process

    Note over OldLeader: ❌ Leader failure detected<br/>(heartbeat timeout)
    
    Note over Candidate: Detect leader failure<br/>and initiate election<br/>(lowest broker_id triggers)
    
    par Election Requests to All Replicas
        Candidate->>Replica1: 1a. ELECTION_REQUEST<br/>{"operation": "ELECTION_REQUEST", "candidate_id": "B-002", "cluster_version": 43}
        Candidate->>Replica2: 1b. ELECTION_REQUEST<br/>{"operation": "ELECTION_REQUEST", "candidate_id": "B-002", "cluster_version": 43}
    end
    
    Note over Replica1: Validate candidate eligibility<br/>- Must be a replica<br/>- Valid cluster version
    Note over Replica2: Validate candidate eligibility<br/>- Must be a replica<br/>- Valid cluster version
    
    par Vote Responses
        Replica1->>Candidate: 2a. Vote granted<br/>{"status": "granted"}
        Replica2->>Candidate: 2b. Vote granted<br/>{"status": "granted"}
    end
    
    Note over Candidate: Majority votes received<br/>Promote self to leader
    
    par Announce New Leadership
        Candidate->>Replica1: 3a. PROMOTE_TO_LEADER<br/>{"operation": "PROMOTE_TO_LEADER", "broker_id": "B-002", "cluster_version": 43}
        Candidate->>Replica2: 3b. PROMOTE_TO_LEADER<br/>{"operation": "PROMOTE_TO_LEADER", "broker_id": "B-002", "cluster_version": 43}
    end
    
    par Leadership ACK
        Replica1->>Candidate: 4a. {"status": "success"}
        Replica2->>Candidate: 4b. {"status": "success"}
    end
    
    Note over Candidate: Now the new leader<br/>Resume coordinating writes<br/>and replication
    
    Client->>Candidate: 5. Client operations resume<br/>with new leader
```

**Key Messages:**
- `ELECTION_REQUEST`: Candidate requests votes from replicas
- Vote validation and consensus
- `PROMOTE_TO_LEADER`: New leader announcement
- Automatic failover and recovery

---

## 6. Heartbeat Monitoring and Failure Detection

Demonstrates the heartbeat mechanism and how failures are detected and handled.

```mermaid
sequenceDiagram
    participant Leader as Leader Broker
    participant Replica1 as Replica Broker 1
    participant Replica2 as Replica Broker 2
    participant FailedBroker as Failed Broker

    Note over Leader, FailedBroker: Heartbeat Monitoring and Failure Detection

    loop Periodic Heartbeat Cycle (every 5 seconds)
        par Heartbeat to All Brokers
            Replica1->>Leader: 1a. HEARTBEAT<br/>{"operation": "HEARTBEAT", "broker_id": "B-001", "timestamp": 1234567890.123, "role": "replica", "cluster_version": 42}
            Replica2->>Leader: 1b. HEARTBEAT<br/>{"operation": "HEARTBEAT", "broker_id": "B-002", "timestamp": 1234567890.123, "role": "replica", "cluster_version": 42}
            FailedBroker->>Leader: 1c. HEARTBEAT<br/>{"operation": "HEARTBEAT", "broker_id": "B-003", "timestamp": 1234567890.123, "role": "replica", "cluster_version": 42}
        end
        
        par Heartbeat ACK
            Leader->>Replica1: 2a. {"status": "success"}
            Leader->>Replica2: 2b. {"status": "success"}
            Leader->>FailedBroker: 2c. {"status": "success"}
        end
    end
    
    Note over FailedBroker: ❌ Broker fails
    
    loop Next Heartbeat Cycle
        par Expected Heartbeats
            Replica1->>Leader: 3a. HEARTBEAT (✓ received)
            Replica2->>Leader: 3b. HEARTBEAT (✓ received)
            Note over FailedBroker: ❌ No heartbeat sent
        end
        
        Leader->>Replica1: 4a. {"status": "success"}
        Leader->>Replica2: 4b. {"status": "success"}
    end
    
    Note over Leader: Timeout detected for B-003<br/>(15 second election timeout)
    
    par Membership Update - Mark Failed Broker Inactive
        Leader->>Replica1: 5a. CLUSTER_UPDATE<br/>{"operation": "CLUSTER_UPDATE", "cluster_version": 43, "members": {"B-003": {"status": "inactive"}}}
        Leader->>Replica2: 5b. CLUSTER_UPDATE<br/>{"operation": "CLUSTER_UPDATE", "cluster_version": 43, "members": {"B-003": {"status": "inactive"}}}
    end
    
    par ACK Membership Update
        Replica1->>Leader: 6a. {"status": "success"}
        Replica2->>Leader: 6b. {"status": "success"}
    end
    
    Note over Leader, Replica2: Failed broker excluded from<br/>future operations and replication<br/>until it rejoins cluster
```

**Key Messages:**
- `HEARTBEAT`: Periodic health check messages
- Timeout-based failure detection
- `CLUSTER_UPDATE`: Failed broker exclusion
- Automatic cluster healing

---

## 7. Complete System Overview

Comprehensive diagram showing the interaction of all major system components and message flows.

```mermaid
sequenceDiagram
    participant C1 as Client 1
    participant C2 as Client 2
    participant L as Leader Broker
    participant R1 as Replica 1
    participant R2 as Replica 2

    Note over C1, R2: Complete System Message Flow Overview

    rect rgb(240, 248, 255)
        Note over C1, R2: 1. Client Discovery Phase
        C1->>L: CLUSTER_QUERY
        L->>C1: Cluster topology
        C2->>R1: CLUSTER_QUERY  
        R1->>C2: Cluster topology
    end

    rect rgb(240, 255, 240)
        Note over C1, R2: 2. Queue Operations Phase
        C1->>L: CREATE_QUEUE
        par Leader Replication
            L->>R1: REPLICATE (CREATE_QUEUE)
            L->>R2: REPLICATE (CREATE_QUEUE)
        end
        par Majority ACK
            R1->>L: ACK
            R2->>L: ACK
        end
        L->>C1: Success response

        C2->>L: APPEND message
        par Leader Replication
            L->>R1: REPLICATE (APPEND_MESSAGE)
            L->>R2: REPLICATE (APPEND_MESSAGE)
        end
        par Majority ACK
            R1->>L: ACK
            R2->>L: ACK
        end
        L->>C2: Success response
    end

    rect rgb(255, 240, 240)
        Note over C1, R2: 3. Health Monitoring Phase
        par Periodic Heartbeats
            R1->>L: HEARTBEAT
            R2->>L: HEARTBEAT
        end
        par Heartbeat ACK
            L->>R1: ACK
            L->>R2: ACK
        end
    end

    rect rgb(255, 255, 240)
        Note over C1, R2: 4. Read Operations Phase
        C1->>L: READ request
        Note over L: Check position, fetch data,<br/>update client position
        L->>C1: Message data
        
        par Position Replication
            L->>R1: REPLICATE (UPDATE_POSITION)
            L->>R2: REPLICATE (UPDATE_POSITION)
        end
    end

    Note over C1, R2: System provides:<br/>• Strong consistency via leader coordination<br/>• Fault tolerance via replication<br/>• Load balancing across leaders<br/>• Automatic failure detection & recovery
```

---

## Message Exchange Summary

### Client ↔ Broker Messages
| Message | Purpose | Direction |
|---------|---------|-----------|
| `CLUSTER_QUERY` | Discover cluster topology | Client → Broker |
| `CREATE_QUEUE` | Create new queue | Client → Leader |
| `APPEND` | Add message to queue | Client → Leader |
| `READ` | Read next message | Client → Leader |

### Broker ↔ Broker Messages
| Message | Purpose | Direction |
|---------|---------|-----------|
| `JOIN_CLUSTER` | Join existing cluster | New Broker → Leader |
| `HEARTBEAT` | Health check | All Brokers → Leader |
| `REPLICATE` | Data replication | Leader → Replicas |
| `ELECTION_REQUEST` | Request votes for leadership | Candidate → Replicas |
| `PROMOTE_TO_LEADER` | Announce new leader | New Leader → All |
| `CLUSTER_UPDATE` | Update cluster membership | Leader → All |
| `DATA_SYNC_REQUEST` | Request data synchronization | Broker → Leader |

### System Characteristics
- **Strong Consistency**: All operations coordinated through leaders
- **Fault Tolerance**: Majority consensus for all write operations
- **Automatic Recovery**: Leader election and failure detection
- **Load Balancing**: Multiple leaders for horizontal scaling
- **Dynamic Membership**: Brokers can join/leave at runtime

---

*Generated from the distributed broker system message exchange analysis*
