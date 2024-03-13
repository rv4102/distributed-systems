from consistent_hashing import ConsistentHashMap
from quart import Quart, jsonify, Response, request
import asyncio
import aiohttp
import docker
import random
import os

app = Quart(__name__)
client = docker.from_env()
server_id_to_hostname = {}
server_hostname_to_id = {}
server_to_shards = {}
shard_to_data = {}
network = "n1"
image = "serv"
schema = {}
async def check_heartbeat(server_name = None):
    url = f'http://{server_name}:5000/heartbeat'
    try:
        async with aiohttp.ClientSession() as client_session:
            async with client_session.get(url) as response:
                if response.status == 200:
                    return True
                else:
                    return False
    except Exception as e:
        return False
## Abhi bas copy pasted hai isko theek karna hoga
async def periodic_server_monitor(interval = 1):
    while True:
        dead_servers = []
        tasks = [check_heartbeat(server_name) for server_name in server_hostname_to_id.keys()]
        results = await asyncio.gather(*tasks)
        results = zip(server_hostname_to_id.keys(), results)
        for server_name, result in results:
            if result == False:
                server_id = server_hostname_to_id[server_name]
                ch.remove_server(server_id)
                dead_servers.append(server_name)
        
        for server_name in dead_servers:
            server_id = server_hostname_to_id[server_name]

            try:
                res = client.containers.run(image=image, name=server_name, network=network, detach=True, environment={'SERV_ID': server_id})
            except Exception as e:
                print(e)

            ch.add_server(server_id)
        
        await asyncio.sleep(interval)

@app.route('/init', methods=['POST'])
async def init(payload = None):
    # NOTHING DONE ABOUT CONSISTENT HASHMAP
    payload = await request.get_json()
    num_servers = payload['N']
    servers = payload['servers']
    shards = payload['shards']
    global schema
    schema = payload['schema']
    if num_servers is None or servers is None or shards is None or schema is None:
        response = jsonify(message = "Invalid Payload, something is missing", status = 'failure')
        response.status_code = 400
        return response
    if num_servers != len(servers):
        response = jsonify(message = "Number of servers does not match N", status = 'failure')
        response.status_code = 400
        return response
    for shard_data in shards:
        shard_to_data[shard_data["Shard_id"]] = shard_data 
    for server_name, shard_list in servers.items():
        if server_name not in server_hostname_to_id:
            server_id = random.randint(100000, 999999)
            server_id_to_hostname[server_id] = server_name
            server_hostname_to_id[server_name] = server_id
            server_to_shards[server_name] = shard_list
            try:
                res = client.containers.run(image=image, name=server_name, network=network, detach=True, environment={'SERV_ID': server_id})
            except Exception as e:
                print(e)
                response = jsonify(message = '<Error> Failed to spawn new docker container', 
                                status = 'failure')
                response.status_code = 400
                return response
            url = f'http://{server_name}:5000/config'
            payload = {}
            payload['schema'] = schema
            payload['shards'] = shard_list
            try:
                async with aiohttp.ClientSession() as client_session:
                    async with client_session.post(url,json = payload) as response:
                        response_json = await response.json()
                        print(response_json)
            except Exception as e:
                print(e)
                response = jsonify(message = '<Error> Failed to configure server', status = 'failure')
                response.status_code = 400
                return response
                        # return Response(content, status=response.status, headers = dict(response.headers))
    response = jsonify(message = "Configured Database", status = 'success')
    response.status_code = 200
    return response
@app.route('/status', methods=['GET'])
async def status(payload = None):
    response_content = {}
    response_content['N'] = len(server_hostname_to_id)
    response_content['servers'] = server_to_shards
    response_content['shards'] = shard_to_data.values()
    response = jsonify(message = response_content, status = 'success')
    response.status_code = 200
    return response


@app.route('/add', methods=['POST'])
async def add(payload = None):
    #checks lagana hai
    payload = await request.get_json()
    num_new_servers = payload['n']  
    new_shards = payload['new_shards']
    servers = payload['servers']
    for shard_data in new_shards:
        shard_to_data[shard_data["Shard_id"]] = shard_data
    # if less than n toh randomly init serverid
    for server_name,shard_list in servers:
        # if already exists
        if server_name not in server_hostname_to_id:
            server_id = random.randint(100000, 999999)
            server_id_to_hostname[server_id] = server_name
            server_hostname_to_id[server_name] = server_id
            server_to_shards[server_name] = shard_list
            try:
                res = client.containers.run(image=image, name=server_name, network=network, detach=True, environment={'SERV_ID': server_id})
            except Exception as e:
                print(e)
                response = jsonify(message = '<Error> Failed to spawn new docker container', 
                                status = 'failure')
                response.status_code = 400
                return response
            url = f'http://{server_name}:5000/config'
            payload = {}
            payload['schema'] = schema
            payload['shards'] = shard_list
            try:
                async with aiohttp.ClientSession() as client_session:
                    async with client_session.post(url,json = payload) as response:
                        response_json = await response.json()
                        print(response_json)
            except Exception as e:
                print(e)
                response = jsonify(message = '<Error> Failed to configure server', status = 'failure')
                response.status_code = 400
                return response
    response_json = {}
    response_json['N'] = len(server_hostname_to_id)
    response_json['message'] = "Added new servers"# idhar list of servers bhi bhejna hai
    response_json['status'] = 'successful'
    response = jsonify(message = response_json, status = 'success')
    response.status_code = 200
    return response


@app.route('/rm', methods=['PUT'])
async def rm(payload = None):
    payload = await request.get_json()
    

@app.route('/read', methods=['POST'])
async def read(payload = None):
    payload = await request.get_json()

@app.route('/write', methods=['POST'])
async def write(payload = None):
    payload = await request.get_json()

@app.route('/update', methods=['PUT'])
async def update(payload = None):
    payload = await request.get_json()

@app.route('/del', methods=['PUT'])
async def delete(payload = None):
    payload = await request.get_json()




if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)