from quart import Quart, jsonify, Response, request
from bisect import bisect_left, bisect_right
from collections import defaultdict
from utils import *
import asyncio
import logging
import random
import re
import os

app = Quart(__name__)
logging.basicConfig(level=logging.DEBUG)
shard_write_lock = defaultdict(lambda: asyncio.Lock())
SLEEP_TIME = 60
PORT = 5000


# configs server for particular schema and shards
async def config_server(serverName, schema, shards):
    app.logger.info(f"Configuring {serverName}")
    await asyncio.sleep(SLEEP_TIME)
    async with aiohttp.ClientSession() as session:
        payload = {"schema": schema, "shards": shards}
        async with session.post(f'http://{serverName}:5000/config', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False

# gets the shard data from available server       
async def get_shard_data(shard):
    print(f"Getting shard data for {shard} from available servers")
    serverName = None
    # async with metadata_lock:
    serverId = await get_server_chmap(shard, random.randint(1000000, 1000000))
    if serverId is None:
        app.logger.error(f"ServerId not found for shard {shard}")
        return None

    print(f"ServerId for shard {shard} is {serverId}")
    serverName = await get_server_from_id(serverId)
    if serverName is None:
        app.logger.error(f"ServerName not found for serverId {serverId}")
        return None
    
    # serverName = id_to_server[serverId] ###############
    async with aiohttp.ClientSession() as session:
        payload = {"shards": [shard]}
        async with session.get(f'http://{serverName}:5000/copy', json=payload) as resp:
            result = await resp.json()
            data = result.get(shard, None)
            if resp.status == 200:
                return data
            else:
                return None

# dead server restores shards from other servers
async def restore_shards(serverName, shards):
    for shard in shards:
        shard_data = await get_shard_data(shard)
        await write_shard_data(serverName, shard, shard_data)

# spawns new server if serverName is None, else spawns server with serverName
# if old server is respawned then it restores shards from other servers
# first spawns server, configures it, restores shards, then updates the required maps
async def spawn_server(serverName=None, shardList=[], schema={"columns":["Stud_id","Stud_name","Stud_marks"], "dtypes":["Number","String","Number"]}):
    newserver = False
    # serverId = server_to_id.get(serverName) ##########
    if serverName is not None:
        serverId = await get_id_from_server(serverName)
    else:
        serverId = -1

    if serverId is None:
        app.logger.error("No available servers")
        return False, ""
    
    if serverId == -1:
        newserver = True
        serverId = await get_first_available_server()
        if serverId == None:
            app.logger.error(f"No available servers")
            return False, ""
    
    if serverName == None:
        serverName = f'server{serverId}'

    containerName = serverName
    res = os.popen(f"docker run --platform=linux/amd64 --name {containerName} --network net1 --network-alias {containerName} -e SERVER_NAME={containerName} -d server").read()
    if res == "":
        app.logger.error(f"Error while spawning {containerName}")
        return False, ""
    else:
        app.logger.info(f"Spawned {containerName}")
        try:
            await config_server(serverName, schema, shardList)
            app.logger.info(f"Configured {containerName}")

            if not newserver:
                await restore_shards(serverName, shardList)
                app.logger.info(f"Restored shards for {containerName}")

            # async with metadata_lock:
            for shard in shardList:
                # shard_hash_map[shard].addServer(serverId)
                result = await add_server_to_chmap(shard, serverId)
                if not result:
                    app.logger.error(f"Error while adding server to shard for {serverName}")
                    return False, ""

                # shard_to_servers.setdefault(shard, []).append(serverName) ###################
                result = await set_server_to_shard(serverName, shard)
                if not result:
                    app.logger.error(f"Error while setting server to shard for {serverName}")
                    return False, ""
            
            # id_to_server[serverId] = serverName ############
            result = await set_id_to_server(serverName, serverId)
            if result == False:
                app.logger.error(f"Error while setting id to server for {serverName}")
                return False, ""
        
            # server_to_id[serverName] = serverId ############
            result = await set_server_to_id(serverName, serverId)
            if not result:
                app.logger.error(f"Error while setting server to id for {serverName}")
                return False, ""

            # server_to_shards[serverName] = shardList ############
            result = await set_shards_to_server(serverName, shardList)
            if not result:
                app.logger.error(f"Error while setting server to shards for {serverName}")
                return False, ""

            app.logger.info(f"Updated metadata for {containerName}")
        except Exception as e:
            app.logger.error(f"Error while spawning {containerName}, got exception {e}")
            return False, ""
        
        return True, serverName

async def remove_container(hostname):
    try:
        # serverId = server_to_id[hostname] ########
        serverId = await get_id_from_server(hostname)
        if serverId == None:
            app.logger.error(f"No available servers")
            return False, ""
        
        # shardList = server_to_shards[hostname] #############
        shardList = await get_shards_from_server(hostname)
        if shardList == None:
            app.logger.error(f"Error while getting shards from server {hostname}")
            return False

        for shard in shardList:
            # shard_hash_map[shard].removeServer(serverId)
            result = await delete_server_from_chmap(shard, serverId)
            if not result:
                app.logger.error(f"Error while removing server from shard hashmap for {hostname}")
                return False

        # del server_to_shards[hostname] ############
        result = await delete_from_server_to_shards(hostname)
        if result == False:
            app.logger.error(f"Error while deleting {hostname} from server to shards")
            return False

        result = await add_to_available_servers(serverId)
        if result == False:
            app.logger.error(f"Error while adding {serverId} to available servers")
            return False
    
        # server_to_id.pop(hostname) ############
        result = await delete_from_server_to_id(hostname)
        if result == False:
            app.logger.error(f"Error while deleting {hostname} from server to id")
            return False

        # id_to_server.pop(serverId) ##############
        result = await delete_from_id_to_server(serverId)
        if result == False:
            app.logger.error(f"Error while deleting {serverId} from id to server")
            return False
        
        ###### fetch primary_servers from metadata
        shards_needing_reelect = await get_shards_mapped_to_primary_server(hostname)
        if shards_needing_reelect == None:
            app.logger.error(f"Error while getting shards mapped to primary server {hostname}")
            return False

        # if hostname in shard_to_primary_server.values(): ############
        #     shard = [k for k, v in shard_to_primary_server.items() if v == hostname][0]
        #     shard_to_primary_server.pop(shard)
            
        #     result = await primary_elect(shard)
        #     if not result:
        #         jsonify({"message": "Error while electing primary", "status": "failure"}), 500    
        for shard in shards_needing_reelect:
            result = await delete_from_shard_to_primary_server(shard)
            if not result:
                app.logger.error(f"Error while deleting primary server for shard {shard}")
                return False
            
            # delete from shard to servers
            result = await delete_from_shard_to_servers(shard, hostname)
            if not result:
                app.logger.error(f"Error while deleting server from shard to servers for shard {shard}")
                return False
            
            result = await primary_elect(shard)
            if not result:
                app.logger.error(f"Error while electing primary server for shard {shard}")
                return False
        
        res = os.popen(f"docker stop {hostname} && docker rm {hostname}").read()
        if res == "":
            app.logger.error(f"Error while removing {hostname}")
            return False

    except Exception as e:
        app.logger.error(f"<ERROR> {e} occurred while removing hostname={hostname}")
        raise e 
    app.logger.info(f"Server with hostname={hostname} removed successfully")



# assuming 3 replicas when shard placement is not mentioned
@app.route('/init', methods=['POST'])
async def init():
    payload = await request.get_json()
    n = payload.get("N")
    schema = payload.get("schema")
    shards = payload.get("shards")
    servers = payload.get("servers")

    if not n or not schema or not shards:
        return jsonify({"message": "Invalid payload", "status": "failure"}), 400
    
    if 'columns' not in schema or 'dtypes' not in schema or len(schema['columns']) != len(schema['dtypes']) or len(schema['columns']) == 0:
        return jsonify({"message": "Invalid schema", "status": "failure"}), 400
    
    if len(shards) == 0:
        return jsonify({"message": "Invalid shards or servers", "status": "failure"}), 400

    prefix_shard_sizes = await get_prefix_shard_sizes_ds()
    if prefix_shard_sizes is None:
        return jsonify({"message": "Error while getting prefix shard sizes", "status": "failure"}), 500
    prefix_shard_sizes = [0]

    shardT = shards

    for shard in shards:
        prefix_shard_sizes.append(prefix_shard_sizes[-1] + shard["Shard_size"])

    result = await set_prefix_shard_sizes(prefix_shard_sizes)
    if not result:
        return jsonify({"message": "Error while setting prefix shard sizes", "status": "failure"}), 500

    ###########
    spawned_servers = []

    # *2 would also work fine
    tasks = []
    shards = shards*3
    if not servers:
        for i in range(n):
            nshards = len(shards)//n
            tasks.append(spawn_server(None, shards[i:i+nshards], schema))
        servers = {}

    for server, shardList in servers.items():
        tasks.append(spawn_server(server, shardList, schema))

    results = await asyncio.gather(*tasks)
    for result in results:
        if result[0]:
            spawned_servers.append(result[1])
    
    
    shard_to_servers = await get_shard_to_servers()
    if shard_to_servers is None:
        return jsonify({"message": "Error while getting shard to servers map", "status": "failure"}), 500
    
    for shard, serverlist in shard_to_servers.items():
        # get a random primary server from serverlist
        primary_server = random.choice(serverlist)

        # shard_to_primary_server[shard] = primary_server ############
        result = await set_shard_to_primary_server(shard, primary_server)
        if not result:
            return jsonify({"message": "Error while setting primary", "status": "failure"}), 500

        result = await set_primary(shard, primary_server)
        if not result:
            return jsonify({"message": "Error while setting primary", "status": "failure"}), 500


    for dict in shardT:
        primary_server = await get_primary_server(dict['Shard_id'])
        if primary_server is None:
            return jsonify({"message": "Error while getting primary server", "status": "failure"}), 500
        
        dict['Primary'] = primary_server

    result = await set_shardT(shardT)
    if not result:
        return jsonify({"message": "Error while setting shardT", "status": "failure"}), 500

    if len(spawned_servers) == 0:
        return jsonify({"message": "No servers spawned", "status": "failure"}), 500
    
    if len(spawned_servers) != n:
        return jsonify({"message": f"Spawned only {spawned_servers} servers", "status": "success"}), 200
    
    return jsonify({"message": "Configured Database", "status": "success"}), 200


@app.route('/status', methods=['GET'])
async def status():
    servers = await get_server_to_shards_ds()
    if servers is None:
        return jsonify({"message": "Error while getting server to shards map", "status": "failure"}), 500

    shards = await get_shardT_ds()
    if shards is None:
        return jsonify({"message": "Error while getting shardT", "status": "failure"}), 500
    shards = shards[0] # yeh thora sa hack hai

    N = len(servers)
    return jsonify({"N": N, "shards": shards, "servers": servers, "status": "success"}), 200


# if new_shards are empty, then we are just increasing replication factor
@app.route('/add', methods=['POST'])
async def add_servers():
    payload = await request.get_json()
    n = payload.get("n")
    new_shards = payload.get("new_shards")
    servers = payload.get("servers")
    
    if not n or not servers:
        return jsonify({"message": "Invalid payload", "status": "failure"}), 400
    
    if n!=len(servers):
        return jsonify(message=f"<Error> Number of new servers {n} is not equal to newly added instances {len(new_shards)}", status="failure"), 400
    
    current_servers = await get_servers()
    if current_servers is None:
        return jsonify({"message": "Error while getting current servers", "status": "failure"}), 500
    
    for server in servers:
        if server in current_servers:
            return jsonify(message=f"<ERROR> {server} already exists", status="failure"), 400
        
    if not new_shards:
        new_shards = []

    prefix_shard_sizes = await get_prefix_shard_sizes_ds()
    if prefix_shard_sizes is None:
        return jsonify({"message": "Error while getting prefix shard sizes", "status": "failure"}), 500
    prefix_shard_sizes = prefix_shard_sizes[0] # yeh thora hack hai    
    
    shardT = await get_shardT_ds()
    if shardT is None:
        return jsonify({"message": "Error while getting shardT", "status": "failure"}), 500
    shardT = shardT[0] # yeh thora sa hack hai

    for shardData in new_shards:
        shard_size = shardData["Shard_size"]
        shardT.append(shardData)
        prefix_shard_sizes.append(prefix_shard_sizes[-1] + shard_size)

    result = await set_prefix_shard_sizes(prefix_shard_sizes)
    if not result:
        return jsonify({"message": "Error while setting prefix shard sizes", "status": "failure"}), 500

    spawned_servers = []
    tasks = []
    for server, shardList in servers.items():
        if not re.match(r'^[a-zA-Z0-9][a-zA-Z0-9_.-]*$', server):
            tasks.append(spawn_server(None, shardList))
        else:
            tasks.append(spawn_server(server, shardList))

    results = await asyncio.gather(*tasks)
    for result in results:
        if result[0]:
            spawned_servers.append(result[1])
    
    shard_to_servers = await get_shard_to_servers()
    if shard_to_servers is None:
        return jsonify({"message": "Error while getting shard to servers map", "status": "failure"}), 500
    
    for shard, serverlist in shard_to_servers.items():
        primary_server = await get_primary_server(shard)
        if primary_server is None:
            return jsonify({"message": "Error while getting primary server", "status": "failure"}), 500
        
        # if shard in shard_to_primary_server.keys():
        #     continue
        if primary_server != "": # primary server is already set
            continue

        # get a random primary server from serverlist
        primary_server = random.choice(serverlist)
        # shard_to_primary_server[shard] = primary_server
        result = await set_shard_to_primary_server(shard, primary_server)
        if not result:
            return jsonify({"message": "Error while setting primary", "status": "failure"}), 500
        
        result = await set_primary(shard, primary_server)
        if not result:
            return jsonify({"message": "Error while setting primary", "status": "failure"}), 500

    print(shardT)
    for dict in shardT:
        # dict['Primary'] = shard_to_primary_server[dict['Shard_id']]
        primary_server = await get_primary_server(dict['Shard_id'])
        if primary_server is None:
            return jsonify({"message": "Error while getting primary server", "status": "failure"}), 500
        
        dict['Primary'] = primary_server

    result = await set_shardT(shardT)
    if not result:
        return jsonify({"message": "Error while setting shardT", "status": "failure"}), 500
    

    if len(spawned_servers) == 0:
        return jsonify({"message": "No servers spawned", "status": "failure"}), 500
    
    return jsonify({"message": f"Add {', '.join(spawned_servers)} servers", "status": "success"}), 200


@app.route('/rm', methods=['DELETE'])
async def remove_servers():
    payload = await request.get_json()
    n = payload.get("n")
    servers = payload.get("servers")
    
    if len(servers) > n:
        return jsonify({"message": "Invalid payload", "status": "failure"}), 400
    
    if not n or not servers:
        return jsonify({"message": "Invalid payload", "status": "failure"}), 400

    current_servers = await get_servers()
    if current_servers is None:
        return jsonify({"message": "Error while getting current servers", "status": "failure"}), 500

    for server in servers:
        if server not in current_servers:
            return jsonify(message=f"<ERROR> {server} is not a valid server name", status="failure"), 400

    random_cnt = n - len(servers)
    remove_keys = []
    try:
        for server in servers:
            await remove_container(hostname=server)
        if random_cnt > 0 :
            remove_keys = random.sample(current_servers, random_cnt)
            for server in remove_keys:
                await remove_container(hostname=server)
    except Exception as e:
        return jsonify(message=f"<ERROR> {e} occurred while removing", status="failure"), 400
    
    remove_keys.extend(servers)

    shardT = await get_shardT_ds()
    if shardT is None:
        return jsonify({"message": "Error while getting shardT", "status": "failure"}), 500
    shardT = shardT[0] # yeh thora sa hack hai
    
    for dict in shardT:
        # dict['Primary'] = shard_to_primary_server[dict['Shard_id']]
        primary_server = await get_primary_server(dict['Shard_id'])
        if primary_server is None:
            return jsonify({"message": "Error while getting primary server", "status": "failure"}), 500
        
        dict['Primary'] = primary_server

    result = await set_shardT(shardT)
    if not result:
        return jsonify({"message": "Error while setting shardT", "status": "failure"}), 500
    

    server_count = await get_available_servers_count()
    if server_count is None:
        return jsonify({"message": "Error while getting available servers count", "status": "failure"}), 500
    
    return jsonify({"message": {"N": server_count, "servers": remove_keys}, "status": "success"}), 200


@app.route('/read', methods=['POST'])
async def read():
    payload = await request.get_json()
    stud_id = payload.get("Stud_id")
    if not stud_id:
        return jsonify({"message": "Invalid payload", "status": "failure"}), 400
    
    low = stud_id.get("low")
    high = stud_id.get("high")

    if not low or not high:
        return jsonify({"message": "Invalid payload", "status": "failure"}), 400
    
    prefix_shard_sizes = await get_prefix_shard_sizes_ds()
    if prefix_shard_sizes is None:
        return jsonify({"message": "Error while getting prefix shard sizes", "status": "failure"}), 500
    prefix_shard_sizes = prefix_shard_sizes[0] # yeh thora hack hai 
    
    shardT = await get_shardT_ds()
    if shardT is None:
        return jsonify({"message": "Error while getting shardT", "status": "failure"}), 500
    shardT = shardT[0] # yeh thora sa hack hai
    
    lower_shard_index = bisect_right(prefix_shard_sizes, low)
    upper_shard_index = bisect_left(prefix_shard_sizes, high+1)
    lower_shard_index -= 1
    shardIndex = lower_shard_index
    shards_queried = [shard["Shard_id"] for shard in shardT[lower_shard_index:upper_shard_index]]
    
    data = []

    for shard in shards_queried:
        # serverId = shard_hash_map[shard].getServer(random.randint(100000, 1000000))
        serverId = await get_server_chmap(shard, random.randint(100000, 1000000))
        if serverId is None:
            app.logger.error(f"ServerId not found for shard {shard}")
            return jsonify({"message": "Error while getting server ID", "status": "failure"}), 500

        # server = id_to_server[serverId]
        serverName = await get_server_from_id(serverId)
        if serverName is None:
            app.logger.error(f"ServerName not found for serverId {serverId}")
            return jsonify({"message": "Error while getting server name", "status": "failure"}), 500

        async with aiohttp.ClientSession() as session:
            spayload = {"shard": shard , "Stud_id": {"low": max(low, prefix_shard_sizes[shardIndex]), "high": min(high, prefix_shard_sizes[shardIndex]+shardT[shardIndex]["Shard_size"]-1)}}
            app.logger.info(f"Reading from {serverName} for shard {shard} with payload {spayload}")
            async with session.post(f'http://{serverName}:5000/read', json=spayload) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    data.extend(result.get("data", []))
                else:
                    app.logger.error(f"Error while reading from {serverName} for shard {shard}")
                    return jsonify({"message": "Error while reading", "status": "failure"}), 500

            shardIndex += 1
                
    return jsonify({"shards_queried": shards_queried, "data": data, "status": "success"}), 200


@app.route('/write', methods=['POST'])
async def write():
    payload = await request.get_json()
    data = payload.get("data")
    if not data:
        return jsonify({"message": "Invalid payload", "status": "failure"}), 400
    
    shards_to_data = {}
    prefix_shard_sizes = await get_prefix_shard_sizes_ds()
    if prefix_shard_sizes is None:
        return jsonify({"message": "Error while getting prefix shard sizes", "status": "failure"}), 500
    prefix_shard_sizes = prefix_shard_sizes[0] # yeh thora hack hai 
    
    shardT = await get_shardT_ds()
    if shardT is None:
        return jsonify({"message": "Error while getting shardT", "status": "failure"}), 500
    shardT = shardT[0] # yeh thora sa hack hai
    
    # can be optimised instead of binary search, by sorting wrt to Stud_id
    for record in data:
        stud_id = record.get("Stud_id")
        shardIndex = bisect_right(prefix_shard_sizes, stud_id)
        if shardIndex == 0 or (shardIndex == len(prefix_shard_sizes) and stud_id >= prefix_shard_sizes[-1]+shardT[-1]["Shard_size"]):
            return jsonify({"message": "Invalid Stud_id", "status": "failure"}), 400
        shard = shardT[shardIndex-1]["Shard_id"]
        shards_to_data[shard] = shards_to_data.get(shard, [])
        shards_to_data[shard].append(record)

    for shard, data in shards_to_data.items():
        print(f"Writing to shard {shard} with data {data}")
        async with shard_write_lock[shard]:
            async with aiohttp.ClientSession() as session:
                # primary_server = shard_to_primary_server[shard]
                primary_server = await get_primary_server(shard)
                if primary_server is None:
                    return jsonify({"message": "Error while getting primary server", "status": "failure"}), 500
                
                print(f"Primary server for shard {shard} is {primary_server}")
                payload = {"shard": shard, "data": data}
                async with session.post(f'http://{primary_server}:5000/write', json=payload) as resp:
                    if resp.status != 200:
                        app.logger.error(f"Error while writing to {primary_server} for shard {shard}")
                        return jsonify({"message": "Error while writing", "status": "failure"}), 500
                                
    return jsonify({"message": f"{len(data)} Data entries added", "status": "success"}), 200


@app.route('/update', methods=['PUT'])
async def update():
    payload = await request.get_json()
    stud_id = payload.get("Stud_id")
    data = payload.get("data")
    if not stud_id or not data:
        return jsonify({"message": "Invalid payload", "status": "failure"}), 400
    
    stud_name = data.get("Stud_name")
    stud_marks = data.get("Stud_marks")

    if not stud_name and not stud_marks:
        return jsonify({"message": "Invalid payload", "status": "failure"}), 400
    
    prefix_shard_sizes = await get_prefix_shard_sizes_ds()
    if prefix_shard_sizes is None:
        return jsonify({"message": "Error while getting prefix shard sizes", "status": "failure"}), 500
    prefix_shard_sizes = prefix_shard_sizes[0] # yeh thora hack hai 
    
    shardT = await get_shardT_ds()
    if shardT is None:
        return jsonify({"message": "Error while getting shardT", "status": "failure"}), 500
    shardT = shardT[0] # yeh thora sa hack hai
    
    shardIndex = bisect_right(prefix_shard_sizes, stud_id)
    shardIndex -= 1

    shard = shardT[shardIndex]["Shard_id"]
    async with shard_write_lock[shard]:
        async with aiohttp.ClientSession() as session:
            # primary_server = shard_to_primary_server[shard]
            primary_server = await get_primary_server(shard)
            if primary_server is None:
                return jsonify({"message": "Error while getting primary server", "status": "failure"}), 500

            print(f"Primary server for shard {shard} is {primary_server} in update")
            payload = {"shard": shard, "Stud_id": stud_id, "data": data}
            async with session.put(f'http://{primary_server}:5000/update', json=payload) as resp:
                if resp.status != 200:
                    app.logger.error(f"Error while updating entry in {primary_server} for shard {shard}")
                    return jsonify({"message": "Error while updating", "status": "failure"}), 500

    return jsonify({"message": f"Data entry for Stud_id: {stud_id} updated", "status": "success"}), 200


@app.route('/del', methods=['DELETE'])
async def delete():
    payload = await request.get_json()
    stud_id = payload.get("Stud_id")
    if not stud_id:
        return jsonify({"message": "Invalid payload", "status": "failure"}), 400
    
    prefix_shard_sizes = await get_prefix_shard_sizes_ds()
    if prefix_shard_sizes is None:
        return jsonify({"message": "Error while getting prefix shard sizes", "status": "failure"}), 500
    prefix_shard_sizes = prefix_shard_sizes[0] # yeh thora hack hai 
    
    shardT = await get_shardT_ds()
    if shardT is None:
        return jsonify({"message": "Error while getting shardT", "status": "failure"}), 500
    shardT = shardT[0] # yeh thora sa hack hai
    
    shardIndex = bisect_right(prefix_shard_sizes, stud_id)
    shardIndex -= 1

    shard = shardT[shardIndex]["Shard_id"]
    async with shard_write_lock[shard]:
        async with aiohttp.ClientSession() as session:
            # primary_server = shard_to_primary_server[shard]
            primary_server = await get_primary_server(shard)
            if primary_server is None:
                return jsonify({"message": "Error while getting primary server", "status": "failure"}), 500

            print(f"Primary server for shard {shard} is {primary_server} in delete")
            payload = {"shard": shard, "Stud_id": stud_id}
            async with session.delete(f'http://{primary_server}:5000/del', json=payload) as resp:
                if resp.status != 200:
                    app.logger.error(f"Error while deleting from {primary_server} for shard {shard}")
                    return jsonify({"message": "Error while deleting", "status": "failure"}), 500

    return jsonify({"message": f"Data entry with Stud_id: {stud_id} removed from all replicas", "status": "success"}), 200    


@app.before_serving
async def startup():
    app.logger.info("Starting the Load Balancer")

@app.after_serving
async def cleanup():
    app.logger.info("Stopping the Load Balancer")


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT, debug=False)