from quart import Quart, jsonify, request
from mysql.connector.errors import Error
from consistent_hashing import ConsistentHashMap
from collections import defaultdict
from typing import Dict
import asyncio
import random
# import aiohttp
# import os

app = Quart(__name__)

server_to_id = {}
id_to_server = {}

shard_to_servers = {}
server_to_shards = {}

available_servers = []
prefix_shard_sizes = []
shardT = []

shard_to_primary_server = {}
shard_hash_map:Dict[str, ConsistentHashMap] = defaultdict(ConsistentHashMap)
metadata_lock = asyncio.Lock()

# s1 7 
# s2 8 
# s3 6 dead
# s4 10 primary


########################## Get Functions ##########################

@app.route('/get_server_chmap', methods=['GET'])
async def get_server_from_chmap():
    payload = await request.get_json()
    shard = payload.get('shard')
    request_id = payload.get('request_id')

    global shard_hash_map
    server = shard_hash_map[shard].getServer(request_id)
    return jsonify({"server_id": server, "status": "success"}), 200

@app.route('/get_server_from_id', methods=['GET'])
async def get_server_from_id():
    payload = await request.get_json()
    server_id = payload.get('server_id')

    global id_to_server
    server_name = id_to_server.get(server_id)
    return jsonify({"server_name": server_name, "status": "success"}), 200

@app.route('/get_id_from_server', methods=['GET'])
async def get_id_from_server():
    payload = await request.get_json()
    server_name = payload.get('server_name')

    global server_to_id
    server_id = server_to_id.get(server_name, -1)
    return jsonify({"server_id": server_id, "status": "success"}), 200

@app.route('/get_first_available_server', methods=['GET'])
async def get_first_available_server():
    global available_servers
    async with metadata_lock:
        server_id = available_servers.pop(0)
    return jsonify({"server_id": server_id, "status": "success"}), 200

@app.route('/get_shards_from_server', methods=['GET'])
async def get_shards_from_server():
    payload = await request.get_json()
    server_name = payload.get('server_name')

    global server_to_shards
    shard_list = server_to_shards.get(server_name)
    return jsonify({"shard_list": shard_list, "status": "success"}), 200
   
@app.route('/get_primary_servers', methods=['GET'])
async def get_primary_servers():
    global shard_to_primary_server
    servers = shard_to_primary_server.values()
    return jsonify({"primary_servers": servers, "status": "success"}), 200

@app.route('/get_prefix_shard_sizes_ds', methods=['GET'])
async def get_prefix_shard_sizes_ds():
    global prefix_shard_sizes
    return jsonify({"prefix_shard_sizes": prefix_shard_sizes, "status": "success"}), 200

@app.route('/get_shardT_ds', methods=['GET'])
async def get_shardT_ds():
    global shardT
    return jsonify({"shardT": shardT, "status": "success"}), 200

@app.route('/get_shards_mapped_to_primary_server', methods=['GET'])
async def get_shards_mapped_to_primary_server():
    payload = await request.get_json()
    server_name = payload.get('server_name')

    global shard_to_primary_server
    shards = [shard for shard, server in shard_to_primary_server.items() if server == server_name]
    return jsonify({"shards": shards, "status": "success"}), 200

@app.route('/get_primary_server', methods=["GET"])
async def get_primary_server():
    payload = await request.get_json()
    shard = payload.get('shard')

    global shard_to_primary_server
    server = shard_to_primary_server.get(shard, "")
    return jsonify({"server": server, "status": "success"}), 200

@app.route('/get_server_to_id_ds', methods=['GET'])
async def get_shard_to_id_ds():
    global server_to_id
    return jsonify({"server_to_id": server_to_id, "status": "success"}), 200

@app.route('/get_available_servers_ds', methods=['GET'])
async def get_available_servers_ds():
    global available_servers
    return jsonify({"available_servers": available_servers, "status": "success"}), 200

@app.route('/get_shard_servers', methods=['GET'])
async def get_shard_servers():
    payload = await request.get_json()
    shard = payload.get('shard')
    if not shard:
        return jsonify({"message": "Invalid payload", "status": "failure"}), 400
    
    global shard_to_servers
    servers = shard_to_servers.get(shard, [])
    return jsonify({"shard": shard, "servers": servers, "status": "success"}), 200


########################## Set / Delete Functions ##########################

@app.route('/add_server_chmap', methods=['POST'])
async def add_server_to_chmap():
    payload = await request.get_json()
    server_id = payload.get('server_id')
    shard = payload.get('shard')

    global shard_hash_map
    shard_hash_map[shard].addServer(server_id)
    return jsonify({"message": "Server added to chmap", "status": "success"}), 200

@app.route('/remove_server_chmap', methods=['POST'])
async def remove_server_from_chmap():
    payload = await request.get_json()
    server_id = payload.get('server_id')
    shard = payload.get('shard')

    global shard_hash_map
    shard_hash_map[shard].removeServer(server_id)
    return jsonify({"message": "Server removed from chmap", "status": "success"}), 200

@app.route('/set_server_to_shard', methods=['POST'])
async def set_server_to_shard():
    payload = await request.get_json()
    server_name = payload.get('server_name')
    shard = payload.get('shard')

    global shard_to_servers
    async with metadata_lock:
        shard_to_servers.setdefault(shard, []).append(server_name)

    return jsonify({"message": "Server registered to shard", "status": "success"}), 200

@app.route('/set_shards_to_server', methods=['POST'])
async def set_shards_to_server():
    payload = await request.get_json()
    shard_list = payload.get('shard_list')
    server_name = payload.get('server_name')

    global server_to_shards
    async with metadata_lock:
        server_to_shards[server_name] = shard_list
    return jsonify({"message": "Shard to servers mapping set", "status": "success"}), 200

@app.route('/set_id_to_server', methods=['POST'])
async def set_id_to_server():
    payload = await request.get_json()
    server_name = payload.get('server_name')
    server_id = payload.get('server_id')

    global id_to_server
    async with metadata_lock:
        id_to_server[server_id] = server_name
    return jsonify({"message": "Id to server mapping set", "status": "success"}), 200

@app.route('/set_server_to_id', methods=['POST'])
async def set_server_to_id():
    payload = await request.get_json()
    server_name = payload.get('server_name')
    server_id = payload.get('server_id')

    global server_to_id
    async with metadata_lock:
        server_to_id[server_name] = server_id
    return jsonify({"message": "Server to id mapping set", "status": "success"}), 200

@app.route('/delete_from_server_to_shards', methods=['POST'])
async def delete_from_server_to_shards():
    payload = await request.get_json()
    server_name = payload.get('server_name')

    global server_to_shards
    async with metadata_lock:
        del server_to_shards[server_name]
    return jsonify({"message": "Shard deleted from server", "status": "success"}), 200

@app.route('/add_to_available_servers', methods=['POST'])
async def add_to_available_servers():
    payload = await request.get_json()
    server_id = payload.get('server_id')

    global available_servers
    async with metadata_lock:
        available_servers.append(server_id)
    return jsonify({"message": "Server added to available servers", "status": "success"}), 200

@app.route('/delete_from_server_to_id', methods=['POST'])
async def delete_from_server_to_id():
    payload = await request.get_json()
    server_name = payload.get('server_name')

    global server_to_id
    async with metadata_lock:
        del server_to_id[server_name]
    return jsonify({"message": "Server deleted from server to id", "status": "success"}), 200

@app.route('/delete_from_id_to_server', methods=['POST'])
async def delete_from_id_to_server():
    payload = await request.get_json()
    server_id = payload.get('server_id')

    global id_to_server
    async with metadata_lock:
        del id_to_server[server_id]
    return jsonify({"message": "Server deleted from id to server", "status": "success"}), 200

@app.route('/set_prefix_shard_sizes', methods=['POST'])
async def set_prefix_shard_sizes():
    payload = await request.get_json()
    prefix_shard_sizes_new = payload.get('prefix_shard_sizes')

    global prefix_shard_sizes
    async with metadata_lock:
        prefix_shard_sizes.clear()
        prefix_shard_sizes.append(prefix_shard_sizes_new)
    return jsonify({"message": "Prefix shard sizes set", "status": "success"}), 200

@app.route('/set_shardT', methods=['POST'])
async def set_shardT():
    payload = await request.get_json()
    shardT_new = payload.get('shardT')

    global shardT
    async with metadata_lock:
        shardT.clear()
        shardT.append(shardT_new)
    return jsonify({"message": "ShardT set", "status": "success"}), 200

@app.route('/delete_from_shard_to_primary_server', methods=['POST'])
async def delete_from_shard_to_primary_server():
    payload = await request.get_json()
    shard = payload.get('shard')

    global shard_to_primary_server
    async with metadata_lock:
        del shard_to_primary_server[shard]
    return jsonify({"message": "Shard deleted from primary server", "status": "success"}), 200

@app.route('/set_shard_to_primary_server', methods=['POST'])
async def set_shard_to_primary_server():
    payload = await request.get_json()
    shard = payload.get('shard')
    server_name = payload.get('server_name')

    global shard_to_primary_server
    async with metadata_lock:
        shard_to_primary_server[shard] = server_name
    return jsonify({"message": "Primary server set", "status": "success"}), 200

@app.route('/delete_from_shard_to_servers', methods=['POST'])
async def delete_from_shard_to_servers():
    payload = await request.get_json()
    shard = payload.get('shard')
    server_name = payload.get('server_name')

    global shard_to_servers
    async with metadata_lock:
        shard_to_servers[shard].remove(server_name)
    return jsonify({"message": "Shard deleted from servers", "status": "success"}), 200










@app.route('/get_shard_to_servers', methods=['GET'])
async def get_shard_to_servers():
    global shard_to_servers
    return jsonify({"shard_to_servers": shard_to_servers, "status": "success"}), 200

@app.route('/get_servers', methods=['GET'])
async def get_servers():
    global server_to_id
    servers = list(server_to_id.keys())
    return jsonify({"servers": servers, "status": "success"}), 200



@app.route('/get_id_to_server', methods=['GET'])
async def get_id_to_server():
    global id_to_server
    return jsonify({"id_to_server": id_to_server, "status": "success"}), 200

@app.route('/get_server_to_shards_ds', methods=['GET'])
async def get_server_to_shards_ds():
    global server_to_shards
    return jsonify({"server_to_shards": server_to_shards, "status": "success"}), 200

@app.route('/get_available_servers_count', methods=['GET'])
async def get_available_servers_count():
    global server_to_id
    global available_servers
    count = len(server_to_id)
    return jsonify({"available_servers_count": count, "status": "success"}), 200










@app.route('/get_shard_to_primary_server', methods=['GET'])
async def get_shard_to_primary_server():
    global shard_to_primary_server
    return jsonify({"shard_to_primary_server": shard_to_primary_server, "status": "success"}), 200



@app.route('/set_shard_to_servers', methods=['POST'])
async def set_shard_to_servers():
    payload = request.get_json()
    shard_to_servers_new = payload.get('shard_to_servers')

    global shard_to_servers
    for shard, servers in shard_to_servers_new.items():
        shard_to_servers[shard] = servers
    return jsonify({"message": "Shard to servers mapping set", "status": "success"}), 200

@app.route('/set_server_to_shards', methods=['POST'])
async def set_server_to_shards():
    payload = request.get_json()
    server_to_shards_new = payload.get('server_to_shards')

    global server_to_shards
    for server, shards in server_to_shards_new.items():
        server_to_shards[server] = shards
    return jsonify({"message": "Servers to shard mapping set", "status": "success"}), 200

@app.route('/set_available_servers', methods=['POST'])
async def set_available_servers():
    global available_servers
    payload = await request.get_json()
    available_servers_new = payload.get('available_servers')
    available_servers.clear()
    available_servers.append(available_servers_new)
    return jsonify({"message": "Available servers set", "status": "success"}), 200






@app.errorhandler(Exception)
def handle_exception(e):
    app.logger.error(f"Exception: {e}")
    if isinstance(e, Error):
        return jsonify({"message": e.msg, "status": "error"}), 400
    else:
        return jsonify({"message": "Internal server Error: check params", "status": "error"}), 500



@app.before_request
async def startup():
    app.logger.info("Starting metadata server...")
    global available_servers
    available_servers = [i for i in range(100000, 1000000)]
    random.shuffle(available_servers)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)