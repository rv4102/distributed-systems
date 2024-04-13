# Design Choices

## Log File

Log-File Contains Entries for:
1. Data added (ADD shard_id, stud_id, stud_name, stud_marks)
2. Data updated (UPDATE shard_id, stud_id, stud_name, stud_marks)
3. Data deleted (DEL shard_id, stud_id)

## Additional Endpoints

### Load-Balancer

1. /get_shard_servers
    Payload = {
        "shard": "sh1"
    }

    Response Json = {
        "servers": ["Server1", "Server2"],
        "status": "success"
    }

2. 