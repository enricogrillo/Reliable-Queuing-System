# How to read
- attribute: TYPE
- **bold** attributes are key
- attributes with same name are foreign key in tables with more than one PK

# Broker

Client(**client_id**: TEXT)
Queue(**queue_id**: TEXT)
Reads(**client_id**: TEXT, **queue_id**: TEXT, index: INTEGER)
QueueValue(**queue_id**: TEXT, **value**: TEXT, **index**: INTEGER)

# Proxy

Broker(**broker_id**: INTEGER, ip_address: TEXT, port: INTEGER, group: INTEGER)
Queue(**queue_id**: TEXT)
Holds(**broker_id**: INTEGER, **queue_id**: TEXT)