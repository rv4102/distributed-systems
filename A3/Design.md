# Design Choices

## Overview
This assignment involves implementing a sharded database system for a single table `StudT`, distributed across multiple shards in different server containers. The system utilizes a `Write-Ahead Logging (WAL)` mechanism to ensure consistency among shard replicas across servers. Shards are subparts of the database managing a limited number of entries, with the ability to be replicated for parallel read operations.

## Design Choices
**Logging Mechanism**:  The WAL mechanism ensures that all changes to the database are logged before they are applied to the database. This ensures that the database can be recovered to a consistent state in case of a failure. 
Each server has a separate log file `WALOG.txt` to store changes made to the database. Each entry has following format:
```
<operation> <Stud_id> <Stud_name> <Stud_marks> 
```
In case of delete opearation, only `<Stud_id>` is stored.

**Metadata**: There is a separate container for storing mapping from `Stud_id` to `Shard_id` to `Server_id`. This metadata is accessed by both load balancer and shard manager. There are endpoints defined for updating and querying metadata.

**WAL** 
1. Primary server on receiving a CRUD request, it writes to WAL and sends corresponding request to secondary servers.
2. Secondary servers on receiving the request, write to their WAL.
3. On recovery, a secondary server asks primary server  of respective shards for latest WAL entries.
4. When primary server dies, the secondary server with most updated log takes over and becomes primary server. 

## API Endpoints
1. **Server**
    - `POST /write` : 
        - Primary server: makes change to log and send write request to other servers
        - Secondary server: update log and write data entries in a shard
    - `PUT /update` : updates a particular data entry in a shard in a particular server container
    - `del /delete` : deletes a particular data entry (based on Stud id) in a shard in a particular server container.

2. **Load Balancer**
    - `DELETE /rm` : removes server instances in the load balance

## Steps to Run
1. Start system
```
sudo make build
sudo make up
```

2. Stop 
```
sudo make down
```

