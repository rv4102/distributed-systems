from quart import Quart, jsonify, Response, request
from collections import defaultdict
from utils import *
import random
import asyncio
import logging
import os

app = Quart(__name__)
logging.basicConfig(level=logging.DEBUG)
SHARD_MANAGER_IMAGE_NAME = "shardmanager"
PORT = 5000

# configs server for particular schema and shards
async def config_server(serverName, schema, shards):
    app.logger.info(f"Configuring {serverName}")

    while not await check_heartbeat(serverName, log = False):
        await asyncio.sleep(2)

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

    
# checks periodic heartbeat of server
async def check_heartbeat(serverName, log = True):
    try:
        if log:
            app.logger.info(f"Checking heartbeat of {serverName}")
        async with aiohttp.ClientSession(trust_env=True) as client_session:
            async with client_session.get(f'http://{serverName}:5000/heartbeat') as resp:
                return resp.status == 200
    except Exception as e:
        if log:
            app.logger.error(f"Error while checking heartbeat of {serverName}: {e}")
        return False


async def periodic_heatbeat_check(interval=2):
    app.logger.info("Starting periodic heartbeat check")
    while True:
        print("Checking heartbeat")
        # server_to_id_temp=copy.deepcopy(server_to_id)
        server_to_id = await get_server_to_id_ds()
        if server_to_id == None:
            app.logger.error(f"Error while getting server to id")
            return False

        deadServerList=[]

        tasks = [check_heartbeat(serverName) for serverName in server_to_id.keys()]
        results = await asyncio.gather(*tasks)
        results = zip(server_to_id.keys(),results)

        for serverName, isUp in results:
            if isUp == False:
                app.logger.error(f"Server {serverName} is down")
                shardList = [] 

                server_to_shards = await get_server_to_shards_ds()
                if server_to_shards == None:
                    app.logger.error(f"Error while getting server to shards")
                    return False
    
                for shard in server_to_shards[serverName]:
                    shardList.append(shard)

                    server_id = server_to_id[serverName]

                    # shard_hash_map[shard].removeServer(server_id)
                    result = await delete_server_from_chmap(shard, server_id)
                    if not result:
                        app.logger.error(f"Error while removing server from shard for {serverName}")
                        return False

                deadServerList.append(serverName)
                result = await delete_from_server_to_shards(serverName)
                if not result:
                    app.logger.error(f"Error while deleting server to shards for {serverName}")
                    return False
                            
        
        for serverName in deadServerList:
            print(f"Spawning {serverName}")
            await spawn_server(serverName, shardList)

            shards_needing_reelect = await get_shards_mapped_to_primary_server(serverName)
            if shards_needing_reelect == None:
                app.logger.error(f"Error while getting shards mapped to primary server {serverName}")
                return False

            for shard in shards_needing_reelect:
                print(f"Electing new primary for shard {shard}")

                result = await delete_from_shard_to_primary_server(shard)
                if not result:
                    app.logger.error(f"Error while deleting primary server for shard {shard}")
                    return False

                async with aiohttp.ClientSession(trust_env=True) as client_session:
                    payload = {'shard': shard}
                    async with client_session.get(f'http://{SHARD_MANAGER_IMAGE_NAME}:5000/primary_elect', json=payload) as resp:
                        if resp.status != 200:
                            app.logger.error(f"Error while electing primary for shard {shard}")
                            return False
    
        await asyncio.sleep(interval)


@app.route('/primary_elect', methods=['GET'])
async def primary_elect():
    payload = await request.get_json()
    shard = payload.get('shard')

    if not shard:
        return jsonify({"message": "Invalid payload", "status": "failure"}), 400
    
    # fetch the server with most updated log entry
    candidates = await get_shard_servers(shard)
    if candidates is None:
        return jsonify({"message": "No replicas found for shard", "status": "failure"}), 400
    
    print(candidates)
    
    # call an endpoint on each server to get number of entries corresponding to shard
    async with aiohttp.ClientSession() as session:
        tasks = []
        for server in candidates:
            payload = {"shard": shard}
            task = asyncio.create_task(session.get(f'http://{server}:5000/get_log_count', json=payload))
            tasks.append(task)
        results = await asyncio.gather(*tasks)

    entries = []
    for result in results:
        if result.status != 200:
            app.logger.error(f"Error while fetching entries from server, got status {result.status}")
            continue
        entries.append(await result.json())
    
    print(entries)

    # api returns as {"server_name": count}
    max_entries = -1
    primary_server = None
    for entry in entries:
        server_name = entry.get('server_name')
        logcount = entry.get('logcount')
        if logcount > max_entries:
            max_entries = logcount
            primary_server = server_name

    # shard_to_primary_server[shard] = primary_server
    result = await set_shard_to_primary_server(shard, primary_server)
    if not result:
        return jsonify({"message": "Error while setting primary server", "status": "failure"}), 500

    # call api to set primary on elected server
    async with aiohttp.ClientSession() as session:
        payload = {"shard": shard}
        async with session.post(f'http://{primary_server}:5000/set_primary', json=payload) as resp:
            if resp.status == 200:
                return jsonify({"shard": shard, "primary_server": primary_server, "status": "success"}), 200
            else:
                return jsonify({"message": "Error while setting primary", "status": "failure"}), 500  


@app.before_serving
async def startup():
    app.logger.info("Starting the Shard manager")
    loop = asyncio.get_event_loop()
    loop.create_task(periodic_heatbeat_check())


@app.after_serving
async def cleanup():
    app.logger.info("Stopping the Shard Manager")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT, debug=False)