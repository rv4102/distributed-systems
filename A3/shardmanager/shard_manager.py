from bisect import bisect_left, bisect_right
from collections import defaultdict
import random
import re
from typing import Dict
from quart import Quart, jsonify, Response, request
import asyncio
import aiohttp
import os
import logging
import copy
from consistent_hashing import ConsistentHashMap

app = Quart(__name__)
logging.basicConfig(level=logging.DEBUG)
PORT = 5000
SLEEP_TIME = 60

available_servers = []
server_to_id = {}
id_to_server = {}
shard_to_servers = {}
servers_to_shard = {}
prefix_shard_sizes = []
shardT = []
shard_to_primary_server = {}
# clientSession = aiohttp.ClientSession() # will optimise this after every other thing works fine

shard_hash_map:Dict[str, ConsistentHashMap] = defaultdict(ConsistentHashMap)
shard_write_lock = defaultdict(lambda: asyncio.Lock())
metadata_lock = asyncio.Lock()

async def get_data():
    async with aiohttp.ClientSession() as session:
        async with session.post('http://metadata:5000/get_server_to_id') as resp:
            if resp.status == 200:
                result = await resp.json()
                global server_to_id
                server_to_id = result.get('server_to_id')
        async with session.post('http://metadata:5000/get_id_to_server') as resp:
            if resp.status == 200:
                result = await resp.json()
                global id_to_server
                id_to_server = result.get('id_to_server')
        async with session.post('http://metadata:5000/get_shard_to_servers') as resp:
            if resp.status == 200:
                result = await resp.json()
                global shard_to_servers
                shard_to_servers = result.get('shard_to_servers')
        async with session.post('http://metadata:5000/get_servers_to_shard') as resp:
            if resp.status == 200:
                result = await resp.json()
                global servers_to_shard
                servers_to_shard = result.get('servers_to_shard')
        async with session.post('http://metadata:5000/get_available_servers') as resp:
            if resp.status == 200:
                result = await resp.json()
                global available_servers
                available_servers = result.get('available_servers')
        async with session.post('http://metadata:5000/get_prefix_shard_sizes') as resp:
            if resp.status == 200:
                result = await resp.json()
                global prefix_shard_sizes
                prefix_shard_sizes = result.get('prefix_shard_sizes')
        async with session.post('http://metadata:5000/get_shardT') as resp:
            if resp.status == 200:
                result = await resp.json()
                global shardT
                shardT = result.get('shardT')
        async with session.post('http://metadata:5000/get_shard_to_primary_server') as resp:
            if resp.status == 200:
                result = await resp.json()
                global shard_to_primary_server
                shard_to_primary_server = result.get('shard_to_primary_server')

async def set_data():
    async with aiohttp.ClientSession() as session:
        payload = {"server_to_id": server_to_id}
        async with session.post('http://metadata:5000/set_server_to_id', json=payload) as resp:
            if resp.status != 200:
                app.logger.error(f"Error while setting server_to_id")
        payload = {"id_to_server": id_to_server}
        async with session.post('http://metadata:5000/set_id_to_server', json=payload) as resp:
            if resp.status != 200:
                app.logger.error(f"Error while setting id_to_server")
        payload = {"shard_to_servers": shard_to_servers}
        async with session.post('http://metadata:5000/set_shard_to_servers', json=payload) as resp:
            if resp.status != 200:
                app.logger.error(f"Error while setting shard_to_servers")
        payload = {"servers_to_shard": servers_to_shard}
        async with session.post('http://metadata:5000/set_servers_to_shard', json=payload) as resp:
            if resp.status != 200:
                app.logger.error(f"Error while setting servers_to_shard")
        payload = {"available_servers": available_servers}
        async with session.post('http://metadata:5000/set_available_servers', json=payload) as resp:
            if resp.status != 200:
                app.logger.error(f"Error while setting available_servers")
        payload = {"prefix_shard_sizes": prefix_shard_sizes}
        async with session.post('http://metadata:5000/set_prefix_shard_sizes', json=payload) as resp:
            if resp.status != 200:
                app.logger.error(f"Error while setting prefix_shard_sizes")
        payload = {"shardT": shardT}
        async with session.post('http://metadata:5000/set_shardT', json=payload) as resp:
            if resp.status != 200:
                app.logger.error(f"Error while setting shardT")
        payload = {"shard_to_primary_server": shard_to_primary_server}
        async with session.post('http://metadata:5000/set_shard_to_primary_server', json=payload) as resp:
            if resp.status != 200:
                app.logger.error(f"Error while setting shard_to_primary_server")

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
    async with metadata_lock:
        serverId = shard_hash_map[shard].getServer(random.randint(1000000, 1000000))
        print(f"ServerId for shard {shard} is {serverId}")
        serverName = id_to_server[serverId]
    async with aiohttp.ClientSession() as session:
        payload = {"shards": [shard]}
        async with session.get(f'http://{serverName}:5000/copy', json=payload) as resp:
            result = await resp.json()
            data = result.get(shard, None)
            if resp.status == 200:
                return data
            else:
                return None

# writes the shard data into server
async def write_shard_data(serverName, shard, data):
    async with aiohttp.ClientSession() as session:
        payload = {"shard": shard, "curr_idx": 1, "data": data}
        async with session.post(f'http://{serverName}:5000/write', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False

# dead server restores shards from other servers
async def restore_shards(serverName, shards):
    for shard in shards:
        shard_data = await get_shard_data(shard)
        await write_shard_data(serverName, shard, shard_data)

# spawns new server if serverName is None, else spawns server with serverName
# if old server is respawned then it restores shards from other servers
# first spawns server, configures it, restores shards, then updates the required maps
async def spawn_server(serverName=None, shardList=[], schema={"columns":["Stud_id","Stud_name","Stud_marks"], "dtypes":["Number","String","Number"]}):
    await get_data()
    global available_servers

    newserver = False
    serverId = server_to_id.get(serverName)
    if serverId == None:
        newserver = True
        async with metadata_lock:
            serverId = available_servers.pop(0)
    if serverName == None:
        serverName = f'server{serverId}'

    containerName = serverName
    res = os.popen(f"docker run --name {containerName} --network net1 --network-alias {containerName} -e SERVER_NAME={containerName} -d server").read()
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

            async with metadata_lock:
                for shard in shardList:
                    shard_hash_map[shard].addServer(serverId)
                    shard_to_servers.setdefault(shard, []).append(serverName)
                
                id_to_server[serverId] = serverName
                server_to_id[serverName] = serverId
                servers_to_shard[serverName] = shardList

            app.logger.info(f"Updated metadata for {containerName}")
            await set_data()    
        except Exception as e:
            app.logger.error(f"Error while spawning {containerName}, got exception {e}")
            return False, ""
        
        return True, serverName
    
# checks periodic heartbeat of server
async def check_heartbeat(serverName):
    try:
        app.logger.info(f"Checking heartbeat of {serverName}")
        async with aiohttp.ClientSession(trust_env=True) as client_session:
            async with client_session.get(f'http://{serverName}:5000/heartbeat') as resp:
                if resp.status == 200:
                    return True
                else:
                    return False
    except Exception as e:
        app.logger.error(f"Error while checking heartbeat of {serverName}: {e}")
        return False

async def periodic_heatbeat_check(interval=2):
    app.logger.info("Starting periodic heartbeat check")
    await get_data()
    while True:
        server_to_id_temp=copy.deepcopy(server_to_id)
        deadServerList=[]
        tasks = [check_heartbeat(serverName) for serverName in server_to_id_temp.keys()]
        results = await asyncio.gather(*tasks)
        results = zip(server_to_id_temp.keys(),results)

        for serverName, isUp in results:
            if isUp == False:
                app.logger.error(f"Server {serverName} is down")
                shardList = []  
                for shard in servers_to_shard[serverName]:
                    shardList.append(shard)
                    shard_hash_map[shard].removeServer(server_to_id[serverName])
                deadServerList.append(serverName)
                del servers_to_shard[serverName]
                if serverName in shard_to_primary_server.values():
                    shard = [k for k, v in shard_to_primary_server.items() if v == serverName][0]
                    shard_to_primary_server.pop(shard)
                    async with aiohttp.ClientSession(trust_env=True) as client_session:
                        payload = {'shard': shard}
                        async with client_session.get('http://shardmanager:5000/primary_elect', json=payload) as resp:
                            if resp.status != 200:
                                app.logger.error(f"Error while electing primary for shard {shard}")
                                return False

        for serverName in deadServerList:
            await spawn_server(serverName, shardList)
    
        await set_data()
        await asyncio.sleep(interval)


@app.route('/primary_elect', methods=['GET'])
async def primary_elect():
    await get_data()
    payload = await request.get_json()
    shard = payload.get('shard')
    if not shard:
        return jsonify({"message": "Invalid payload", "status": "failure"}), 400
    print("here1")
    # fetch the server with most updated log entry
    candidates = shard_to_servers.get(shard, [])
    if not candidates:
        return jsonify({"message": "No replicas found for shard", "status": "failure"}), 400
    print("here2")
    # call an endpoint on each server to get number of entries corresponding to shard
    async with aiohttp.ClientSession() as session:
        tasks = []
        for server in candidates:
            json = {"shard": shard}
            task = asyncio.create_task(session.get(f'http://{server}:5000/get_log_count', json=json))
            tasks.append(task)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        entries = []
        print("results await ho gaye")
        for result in results:
            if isinstance(result, Exception):
                app.logger.error(f"Error while fetching entries from server, got exception {result}")
                continue
            if result.status != 200:
                app.logger.error(f"Error while fetching entries from server, got status {result.status}")
                continue
            entries.append(await result.json())
        # api returns as {"server_name": count}
        print(entries)
        max_entries = -1
        primary_server = None
        for server_name, log_count in entries.items():
            if log_count > max_entries:
                max_entries = log_count
                primary_server = server_name
        global shard_to_primary_server
        print(f"shard {shard} ka primary server is {primary_server}")
        shard_to_primary_server[shard] = primary_server
        
    await set_data()

    print("here3")
    # call api to set primary on elected server
    async with aiohttp.ClientSession() as session:
        print("sending payload")
        payload = {"shard": shard}
        async with session.post(f'http://{primary_server}:5000/set_primary', json=payload) as resp:
            print("response aaya")
            if resp.status == 200:
                return jsonify({"shard": shard, "primary_server": primary_server, "status": "success"}), 200
            else:
                return jsonify({"message": "Error while setting primary", "status": "failure"}), 500  

@app.before_serving
async def startup():
    app.logger.info("Starting the Shard manager")
    global available_servers
    available_servers = [i for i in range(100000, 1000000)]
    random.shuffle(available_servers)
    loop = asyncio.get_event_loop()
    loop.create_task(periodic_heatbeat_check())

@app.after_serving
async def cleanup():
    app.logger.info("Stopping the Shard Manager")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT, debug=False)