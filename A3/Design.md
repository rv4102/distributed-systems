# Design Choices

## Overview
This assignment involves implementing a sharded database system for a single table `StudT`, distributed across multiple shards in different server containers. The system utilizes a `Write-Ahead Logging (WAL)` mechanism to ensure consistency among shard replicas across servers. Shards are subparts of the database managing a limited number of entries, with the ability to be replicated for parallel read operations.

## Design Choices
**Logging Mechanism**:  The WAL mechanism ensures that all changes to the database are logged before they are applied to the database. This ensures that the database can be recovered to a consistent state in case of a failure. 
Each server has a separate log file `WALOG.txt` to store changes made to the database. Each entry has following format:
```
<operation> <Stud_id> <Stud_name> <Stud_age> <Seq_Number>
```
In case of delete opearation, only `<Stud_id>` is stored. The `<Seq_Number>` is used to keep track of the order of operations per shard.

**Metadata**: There is a separate container for storing mapping from `Stud_id` to `Shard_id` to `Server_id`. This metadata is accessed by both load balancer and shard manager. There are endpoints defined for updating and querying metadata.

<!-- Explain the design choices made in developing the distributed system, including:

- **Architecture:** Describe the overall architecture of the system (e.g., client-server, peer-to-peer).
- **Communication Protocol:** Specify the communication protocol used (e.g., TCP, UDP).
- **Data Storage:** Describe how data is stored and managed across the distributed system.
- **Fault Tolerance:** Explain how the system handles failures and maintains availability.
- **Scalability:** Discuss how the system scales with increasing load or data volume. -->

## Assumptions

List any assumptions made during the design and implementation of the distributed system. This could include assumptions about the environment, network, or user behavior.

## Testing

Describe the testing approach used to ensure the correctness and reliability of the distributed system. This could include unit tests, integration tests, and system tests.

## Performance Analysis

Present a performance analysis of the distributed system, including:

- **Throughput:** Measure the number of requests processed per unit of time.
- **Latency:** Measure the time taken for a request to be processed.
- **Scalability:** Evaluate how the system's performance scales with increasing load.
- **Resource Utilization:** Monitor the utilization of CPU, memory, and network resources.

## Conclusion

Summarize the key findings from the assignment, including any challenges faced and lessons learned.

## References

List any references or resources used in the development of the distributed system.