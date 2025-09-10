# Leader Election Process - Sequence Diagrams

This document contains detailed sequence diagrams for the leader election process and failure handling scenarios in the distributed broker system, based on the implementation in `leader_election.py` and `cluster_manager.py`.

## Table of Contents

1. [Normal Leader Election Process](#1-normal-leader-election-process)
2. [Candidate Failure During Election](#2-candidate-failure-during-election)
3. [Priority-Based Vote Denial](#3-priority-based-vote-denial)
4. [Network Partition Scenarios](#4-network-partition-scenarios)
5. [Election Retry Mechanisms](#5-election-retry-mechanisms)

---

## 1. Normal Leader Election Process

Shows the complete leader election flow when a leader fails, including staggered delays and candidate selection.

```mermaid
sequenceDiagram
    participant OldLeader as Failed Leader
    participant B1 as Broker-1<br/>(Lowest ID)
    participant B2 as Broker-2
    participant B3 as Broker-3

    Note over OldLeader, B3: Normal Leader Election Process

    Note over OldLeader: ❌ Leader failure detected<br/>(8 second heartbeat timeout)
    
    par All Brokers Detect Failure
        B1->>B1: _detect_failures()<br/>Remove failed leader from cluster_members
        B2->>B2: _detect_failures()<br/>Remove failed leader from cluster_members  
        B3->>B3: _detect_failures()<br/>Remove failed leader from cluster_members
    end

    par All Brokers Trigger Election
        B1->>B1: trigger_leader_election()
        B2->>B2: trigger_leader_election()
        B3->>B3: trigger_leader_election()
    end

    Note over B1, B3: Staggered delays based on broker position<br/>position * 2.0 seconds

    B1->>B1: Wait 0s (position 0)<br/>Check if election needed
    Note over B2: Wait 2s (position 1)
    Note over B3: Wait 4s (position 2)

    B1->>B1: _get_potentially_active_brokers()<br/>Quick connectivity check
    B1->>B1: Candidate selection: min([B1, B2, B3]) = B1
    B1->>B1: I am candidate, start _conduct_election()

    Note over B1: Election execution phase
    B1->>B1: _election_in_progress = True<br/>(Disable failure detection)
    B1->>B1: Create ELECTION_REQUEST<br/>{"operation": "ELECTION_REQUEST",<br/> "candidate_id": "B1",<br/> "cluster_version": version+1,<br/> "timestamp": current_time}

    B1->>B1: Vote for self: votes_received = 1

    par Send Election Requests
        B1->>B2: ELECTION_REQUEST<br/>{"candidate_id": "B1"}
        B1->>B3: ELECTION_REQUEST<br/>{"candidate_id": "B1"}
    end

    Note over B2: handle_election_request() validation:<br/>• role == REPLICA ✓<br/>• no current leader ✓<br/>• priority check passed ✓<br/>• timestamp valid ✓
    Note over B3: Same validation as B2

    par Vote Responses
        B2->>B1: Vote granted<br/>{"status": "granted"}
        B3->>B1: Vote granted<br/>{"status": "granted"}
    end

    B1->>B1: Calculate majority:<br/>votes_received = 3<br/>responsive_brokers = 3<br/>required_votes = (3//2)+1 = 2<br/>3 >= 2 ✓ Election won!

    B1->>B1: promote_to_leader()<br/>role = LEADER<br/>cluster_version += 1

    par Announce New Leadership
        B1->>B2: PROMOTE_TO_LEADER<br/>{"operation": "PROMOTE_TO_LEADER",<br/> "broker_id": "B1",<br/> "cluster_version": new_version}
        B1->>B3: PROMOTE_TO_LEADER<br/>{"operation": "PROMOTE_TO_LEADER",<br/> "broker_id": "B1",<br/> "cluster_version": new_version}
    end

    par Leadership Acknowledgment
        B2->>B1: {"status": "success"}
        B3->>B1: {"status": "success"}
    end

    B1->>B1: _election_in_progress = False<br/>Resume normal leader operations

    Note over B1, B3: Election complete<br/>B1 is now the new leader
```

**Key Messages:**
- `ELECTION_REQUEST`: Candidate requests votes from replicas
- Vote validation includes priority checks and timeouts
- `PROMOTE_TO_LEADER`: New leader announcement to all brokers
- Staggered delays prevent election storms

---

## 2. Candidate Failure During Election

Demonstrates how the system handles candidate failures and triggers backup elections.

```mermaid
sequenceDiagram
    participant B1 as Broker-1<br/>(Failed Candidate)
    participant B2 as Broker-2<br/>(Monitoring)
    participant B3 as Broker-3<br/>(Monitoring)

    Note over B1, B3: Candidate Failure During Election

    Note over B1, B3: B1 starts election as candidate

    B1->>B1: _conduct_election()<br/>Start election process
    
    par Send Election Requests
        B1->>B2: ELECTION_REQUEST<br/>{"candidate_id": "B1"}
        B1->>B3: ELECTION_REQUEST<br/>{"candidate_id": "B1"}
    end

    Note over B2, B3: Start monitoring the election
    B2->>B2: _monitor_election("B1")<br/>6 second timeout monitoring
    B3->>B3: _monitor_election("B1")<br/>6 second timeout monitoring

    Note over B1: ❌ B1 fails during election<br/>(crash or network failure)
    B1--xB2: Connection lost
    B1--xB3: Connection lost

    loop Monitor Election Progress (6 iterations, 1s each)
        Note over B2, B3: Check candidate viability each iteration
        B2->>B2: iteration i: Check if B1 in cluster_members
        B3->>B3: iteration i: Check if B1 in cluster_members
        B2->>B2: Check if leader elected yet
        B3->>B3: Check if leader elected yet
        Note over B2, B3: Sleep 1 second
    end

    Note over B2, B3: Meanwhile, failure detection removes B1
    B2->>B2: Heartbeat timeout detected<br/>Remove B1 from cluster_members
    B3->>B3: Heartbeat timeout detected<br/>Remove B1 from cluster_members

    Note over B2, B3: Monitor detects candidate removal
    B2->>B2: B1 not in cluster_members<br/>"Candidate removed, trigger backup election"
    B2->>B2: trigger_leader_election()

    B3->>B3: B1 not in cluster_members<br/>"Candidate removed, trigger backup election"
    B3->>B3: trigger_leader_election()

    Note over B2, B3: New election with B2 as candidate<br/>(B2 has lower ID than B3)

    B2->>B2: After delay: I'm the new candidate
    B2->>B2: _conduct_election()
    B3->>B3: _monitor_election("B2")

    B2->>B3: ELECTION_REQUEST<br/>{"candidate_id": "B2"}
    B3->>B2: {"status": "granted"}

    B2->>B2: Won election with majority
    B2->>B2: promote_to_leader()
    B2->>B3: PROMOTE_TO_LEADER
    B3->>B2: {"status": "success"}

    Note over B2, B3: Backup election successful<br/>B2 is now leader
```

**Key Messages:**
- Election monitoring prevents indefinite waits
- Failed candidates automatically removed from cluster
- Backup elections triggered by monitoring timeouts
- System self-heals without manual intervention

---

## 3. Priority-Based Vote Denial

Shows how the priority system prevents split votes and ensures deterministic leader selection.

```mermaid
sequenceDiagram
    participant B1 as Broker-1<br/>(Higher Priority)
    participant B2 as Broker-2<br/>(Lower Priority Candidate)
    participant B3 as Broker-3<br/>(Replica)

    Note over B1, B3: Priority-Based Vote Denial Scenario

    Note over B1, B3: B1 is temporarily slow to start election<br/>B2 starts first due to timing

    B2->>B2: trigger_leader_election()<br/>(Shorter delay due to race condition)
    B2->>B2: _conduct_election() as candidate

    par Send Election Requests
        B2->>B1: ELECTION_REQUEST<br/>{"candidate_id": "B2"}
        B2->>B3: ELECTION_REQUEST<br/>{"candidate_id": "B2"}
    end

    Note over B1: handle_election_request() validation
    B1->>B1: Validate request:<br/>• role == REPLICA ✓<br/>• no current leader ✓<br/>• Check priority candidates
    B1->>B1: Priority check:<br/>sorted_ids = [B1, B2, B3]<br/>B1 < B2 (higher priority)<br/>Check B1 heartbeat: recent ✓
    B1->>B2: Vote denied<br/>{"status": "denied",<br/> "reason": "Waiting for higher priority candidate B1"}

    Note over B3: Same priority validation
    B3->>B3: Priority check identifies B1<br/>as higher priority candidate
    B3->>B2: Vote denied<br/>{"status": "denied",<br/> "reason": "Waiting for higher priority candidate B1"}

    Note over B2: B2 election fails
    B2->>B2: votes_received = 1 (only self)<br/>required_votes = 2<br/>Election failed due to insufficient votes
    B2->>B2: _schedule_retry_election()<br/>Random delay 5-10 seconds

    Note over B1, B3: Meanwhile B1 starts proper election

    B1->>B1: trigger_leader_election()<br/>(Higher priority candidate)
    B1->>B1: _conduct_election() as candidate

    par B1 Election Requests
        B1->>B2: ELECTION_REQUEST<br/>{"candidate_id": "B1"}
        B1->>B3: ELECTION_REQUEST<br/>{"candidate_id": "B1"}
    end

    Note over B2, B3: B1 has highest priority, votes granted
    B2->>B1: Vote granted<br/>{"status": "granted"}<br/>(B1 higher priority than B2)
    B3->>B1: Vote granted<br/>{"status": "granted"}

    B1->>B1: Won election with majority<br/>promote_to_leader()

    par Leadership Announcement
        B1->>B2: PROMOTE_TO_LEADER
        B1->>B3: PROMOTE_TO_LEADER
    end

    B2->>B2: Cancel retry election<br/>(leader now exists)

    Note over B1, B3: B1 successfully elected<br/>Priority system prevented split vote
```

**Key Messages:**
- Priority system uses lexicographic broker ID ordering
- Lower ID brokers have higher priority for leadership
- Recent heartbeat check (12 second window) validates candidate viability
- Failed elections automatically retry with exponential backoff

---

## 4. Network Partition Scenarios

Illustrates how the system behaves during network partitions and majority requirements.

```mermaid
sequenceDiagram
    participant B1 as Broker-1<br/>(Partition A)
    participant B2 as Broker-2<br/>(Partition A)
    participant B3 as Broker-3<br/>(Partition B)
    participant B4 as Broker-4<br/>(Partition B)

    Note over B1, B4: Network Partition Scenario

    Note over B1, B4: Leader fails, network splits into two partitions<br/>Partition A: {B1, B2} | Partition B: {B3, B4}

    Note over B1, B2: Partition A Election
    B1->>B1: trigger_leader_election()
    B1->>B1: _conduct_election() as candidate
    B1->>B2: ELECTION_REQUEST<br/>{"candidate_id": "B1"}
    B1->>B3: ELECTION_REQUEST (fails - partition)
    B1->>B4: ELECTION_REQUEST (fails - partition)

    B2->>B1: {"status": "granted"}
    
    B1->>B1: Calculate majority:<br/>successful_contacts = [B1, B2]<br/>failed_contacts = [B3, B4]<br/>responsive_brokers = 2 + 2 = 4<br/>required_votes = (4//2)+1 = 3<br/>votes_received = 2 < 3 ❌

    B1->>B1: Election failed - insufficient votes
    B1->>B1: Handle failed brokers:<br/>Remove B3, B4 from cluster_members
    B1->>B1: Recalculate with updated cluster:<br/>responsive_brokers = 2<br/>required_votes = (2//2)+1 = 2<br/>votes_received = 2 >= 2 ✓

    B1->>B1: promote_to_leader()<br/>(Leader of partition A)
    B1->>B2: PROMOTE_TO_LEADER
    B2->>B1: {"status": "success"}

    Note over B3, B4: Partition B Election (Concurrent)
    B3->>B3: trigger_leader_election()
    B3->>B3: _conduct_election() as candidate
    B4->>B4: _monitor_election("B3")

    B3->>B4: ELECTION_REQUEST<br/>{"candidate_id": "B3"}
    B3->>B1: ELECTION_REQUEST (fails - partition)
    B3->>B2: ELECTION_REQUEST (fails - partition)

    B4->>B3: {"status": "granted"}

    B3->>B3: Handle partition failures similarly<br/>Remove unreachable brokers
    B3->>B3: promote_to_leader()<br/>(Leader of partition B)
    B3->>B4: PROMOTE_TO_LEADER
    B4->>B3: {"status": "success"}

    Note over B1, B4: Result: Two separate clusters<br/>Cluster A: B1 (Leader), B2<br/>Cluster B: B3 (Leader), B4

    rect rgb(255, 240, 240)
        Note over B1, B4: ⚠️ Split-brain condition<br/>Manual intervention required for partition healing
    end
```

**Key Messages:**
- Majority calculation initially includes all known brokers
- Failed brokers removed from cluster during election
- Each partition can elect its own leader
- Split-brain detection requires external coordination

---

## 5. Election Retry Mechanisms

Shows how failed elections are retried and eventual consistency is achieved.

```mermaid
sequenceDiagram
    participant B1 as Broker-1<br/>(Candidate)
    participant B2 as Broker-2<br/>(Replica)
    participant B3 as Broker-3<br/>(Temporarily Failed)

    Note over B1, B3: Election Retry Mechanisms

    Note over B1, B3: B3 temporarily unreachable during first election

    B1->>B1: trigger_leader_election()
    B1->>B1: _conduct_election() as candidate

    par First Election Attempt
        B1->>B2: ELECTION_REQUEST<br/>{"candidate_id": "B1"}
        B1->>B3: ELECTION_REQUEST (fails - unreachable)
    end

    B2->>B1: Vote response varies by scenario
    
    alt Scenario 1: Vote Denied (waiting for others)
        B2->>B1: {"status": "denied",<br/> "reason": "Waiting for other candidates"}
        
        B1->>B1: votes_received = 1<br/>required_votes = 2<br/>Election failed
        B1->>B1: _schedule_retry_election()
        B1->>B1: Random delay = uniform(5.0, 10.0)s
        
        Note over B1: Wait for retry delay
        B1->>B1: After delay: _still_need_leader() ✓
        B1->>B1: trigger_leader_election() (retry)
        
        Note over B1, B3: Second attempt (B3 may be recovered)
        B1->>B1: _conduct_election() (new attempt)
        B1->>B2: ELECTION_REQUEST<br/>{"candidate_id": "B1"}
        B1->>B3: ELECTION_REQUEST<br/>(may succeed if B3 recovered)
        
        B2->>B1: {"status": "granted"}
        
        alt B3 Recovered
            B3->>B1: {"status": "granted"}
            B1->>B1: Won with majority votes<br/>promote_to_leader()
        else B3 Still Failed
            B1->>B1: Handle failed broker<br/>Remove B3 from cluster
            B1->>B1: Majority achieved with remaining brokers<br/>promote_to_leader()
        end
        
    else Scenario 2: Sufficient Votes with Failed Broker
        B2->>B1: {"status": "granted"}
        
        B1->>B1: successful_contacts = [B1, B2]<br/>failed_contacts = [B3]<br/>responsive_brokers = 3<br/>required_votes = 2<br/>votes_received = 2 >= 2 ✓
        
        B1->>B1: _handle_broker_failures([B3])<br/>Remove B3 from cluster_members
        B1->>B1: _become_leader() with updated cluster
        B1->>B2: PROMOTE_TO_LEADER
        B2->>B1: {"status": "success"}
    end

    Note over B1, B2: Election successful after retry mechanism<br/>System achieves eventual consistency
```

**Key Messages:**
- `_schedule_retry_election()`: Random backoff prevents election storms
- `_still_need_leader()`: Validates retry necessity before new election
- Failed brokers automatically removed during election process
- Retry mechanism ensures eventual leader selection

---

## Message Exchange Summary

### Election-Specific Messages
| Message | Purpose | Direction | Key Fields |
|---------|---------|-----------|------------|
| `ELECTION_REQUEST` | Request votes for leadership | Candidate → Replicas | candidate_id, cluster_version, timestamp |
| Vote Response | Grant or deny vote | Replica → Candidate | status: "granted"/"denied", reason |
| `PROMOTE_TO_LEADER` | Announce new leader | New Leader → All | broker_id, cluster_version |

### Election Characteristics
- **Priority-Based**: Lower broker IDs have election priority
- **Majority Consensus**: Requires (responsive_brokers // 2) + 1 votes
- **Failure Resilient**: Automatic retry with exponential backoff
- **Split-Brain Prevention**: Majority requirements prevent dual leadership
- **Monitoring**: Non-candidates monitor election progress with timeouts
- **Staggered Timing**: Position-based delays prevent election storms

### Timing Parameters
- **Election Cooldown**: 5 seconds minimum between elections
- **Staggered Delays**: position × 2.0 seconds
- **Vote Timeout**: 15 seconds maximum for election requests  
- **Vote Cooldown**: 3 seconds minimum between votes
- **Monitor Timeout**: 6 seconds for candidate monitoring
- **Retry Delay**: Random 5-10 seconds for failed elections

---

*Generated from the leader election system implementation analysis*