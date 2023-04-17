# CPEN431 G12 Project #

**Group ID:** G12

**Verification Code:** 1E00C02D4639D3B8F1A94971391B73B2

**Run Command:**

To run a server independently:

```java -Xmx512m -jar A12.jar <ipAddress> <portNumber>```

To launch multiple sub-processes of the above command, there is a python script:

```python3 launch.py <numInstances>```

## Brief Description:

### Consistent Hashing:
We are using guava's consistent hash api to map hashes into node buckets. The hash function we are using for keys is MD5.
If a request is routed to another member node, that node is responsible for sending a reply to the original client
(or test client). If a node is labelled dead via our epidemic protocol, we will increment the assigned bucket number
for routing until a live node is found (circular wrap around). If the bucket assigned is the original node, the request
is processed by the application layer itself. A node knows the locations of all the other nodes (live or dead) and can route to them directly.

### Epidemic Protocol & Node Re-joins:

We implemented a hybrid push-pull epidemic protocol. The membership service periodically selects a few random nodes out of the members to pull statuses from.
Pulled timestamps are compared against a threshold to determine if the node is alive or dead. A node that was considered dead earlier due to its timestamp
can re-join the network if it responds to the ping. When a node re-joins the network, the nodes that contain its keys will redistribute the keys it should own
using multiple put requests. Key conflicts are resolved during the redistribution process.
If the pinged node is dead, the request will time out and eventually the membership service will consider the node dead through its timestamp
calculations. Furthermore, each ping request also serves as a "push" as the request being received will update the timestamp of the sender node.

### Replication:

#### PUT
We use chain replication with a head and tail node. The head node receives a put request and sends a non-blocking chain PUT
request to the next node in the computed live node window, and includes a chain count that is decremented each time.
Once the request reaches the tail node (chainCount = 0), the tailNode will immediately return the result to the client.
This maintains a replication factor of 4.

#### REMOVE
The remove operation operates identically to the PUT operation using chain replication, except that is a remove operation. 

#### GET
For GET requests the tail node will return the value it has, as it is guaranteed to be sequentially consistent.
If the tail node does not have the requested key, we attempt a Quorum Read of 3/3 nodes.

#### Node Failure and Re-join
During node failures, we maintain a replication factor of 4 keeping track of a live node "window" and appropriately copy
the data to the node joining the "window".
For node re-joins, the successor node of the failed node is responsible for copying data back to the failed node.


### Proof of Immediate Shutdown:

Line 208 in src/main/java/com/g12/CPEN431/A12/Server.java

