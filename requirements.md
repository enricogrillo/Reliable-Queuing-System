# Requirements
Implement a distributed and reliable queuing platform where a set of brokers collaborate to offer multiple queues to multiple clients.

Queues are persistent, append only, FIFO data structures. Multiple (not necessarily every) brokers replicate the data of queues to guarantee fault tolerance in case one of them crashes.

Clients connect to brokers to create new queues, append new data (for simplicity assume that queues store integer values) on an existing queue, or read data from a queue. Each client is uniquely identified and the brokers are responsible for keeping track of the next data element each client should read.

Investigate and clarify the level of reliability offered by your system.

The project can be implemented as a real distributed application (for example, in Java) or it can be simulated using OmNet++.

Assumptions
- You may assume no partitions happening, while nodes (brokers) may fail (crash failures).
- Nodes running brokers holds a stable storage (the file system) that can be assumed to be reliable.


# Elaboration

Queues
- multiple queues to multiple clients
- persistent,
- append only,
- FIFO data structures

Brokers
- set of brokers collaborate
- Multiple (not necessarily every) brokers replicate the data of queues to guarantee fault tolerance in case one of them crashes.
- the brokers are responsible for keeping track of the next data element each client should read.

Clients
- Create new queues
- Append new data (for simplicity assume that queues store integer values) on an existing queue,
- or read data from a queue.
- Each client is uniquely identified

Assumptions
- You may assume no partitions happening, while nodes (brokers) may fail (crash failures).
- Nodes running brokers holds a stable storage (the file system) that can be assumed to be reliable

# From Cugola

- any client can read/write any queue, even if created by some other client
- brokers hold a pointer for each client, for each queue
- no authentication is needed, nor any other security feature
- focus on reliability; be as scalable as possible