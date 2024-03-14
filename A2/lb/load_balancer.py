from consistent_hashing import ConsistentHashMap
from quart import Quart, jsonify, Response, request
import asyncio
import aiohttp
import docker
import random

app = Quart(__name__)

# locks and mutexes
locks = asyncio.Lock()
resource = 0
readers_waiting = 0
writers_waiting = 0
write_in_progress = False
writer_priority = True

# other globals
num_servers = 0
num_shards = 0
schema = None

client = docker.from_env()
ch = ConsistentHashMap()
server_id_to_hostname = {}
server_hostname_to_id = {}
server_to_shards = {}
shards_to_server = {}
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
    payload = await request.get_json()
    global num_servers
    num_servers = payload['N']
    servers = payload['servers']
    shards = payload['shards']
    global num_shards
    num_shards = len(shards)
    global schema
    schema = payload['schema']

    # error checking
    if num_servers is None or servers is None or shards is None or schema is None:
        response = jsonify(message = "Invalid Payload, something is missing", status = 'failure')
        response.status_code = 400
        return response
    
    if num_servers != len(servers):
        response = jsonify(message = "Number of servers does not match N", status = 'failure')
        response.status_code = 400
        return response
    
    # update global data structures
    for shard_data in shards:
        shard_to_data[shard_data["Shard_id"]] = shard_data 

    for server_name, shard_list in servers.items():
        if server_name not in server_hostname_to_id:
            server_id = random.randint(100000, 999999)
            server_id_to_hostname[server_id] = server_name
            server_hostname_to_id[server_name] = server_id

        server_to_shards[server_name] = shard_list
        for shard in shard_list:
            if shard not in shards_to_server:
                shards_to_server[shard] = []
            shards_to_server[shard].append(server_name)

        try:
            res = client.containers.run(image=image, name=server_name, network=network, detach=True, environment={'SERV_ID': server_id})
        except Exception as e:
            print(e)
            response = jsonify(message = '<Error> Failed to spawn new docker container', status = 'failure')
            response.status_code = 400
            return response
            
        url = f'http://{server_name}:5000/config'
        payload = {}
        payload['schema'] = schema
        payload['shards'] = shard_list

        try:
            async with aiohttp.ClientSession() as client_session:
                async with client_session.post(url, json = payload) as response:
                    response_json = await response.json()
                    print(response_json)

                    if response.status != 200:
                        response = jsonify(message = f'<Error> Failed to configure server {server_name}', status = 'failure')
                        response.status_code = 400
                        return response

        except Exception as e:
            print(e)
            response = jsonify(message = '<Error> Failed to configure server', status = 'failure')
            response.status_code = 400
            return response

    response = jsonify(message = "Configured Database", status = 'success')
    response.status_code = 200
    return response


@app.route('/status', methods=['GET'])
async def status(payload = None):
    response_content = {}
    response_content['N'] = len(server_hostname_to_id)
    response_content['servers'] = server_to_shards
    response_content['shards'] = shard_to_data.values()
    response_content['schema'] = schema

    response = jsonify(message = response_content, status = 'success')
    response.status_code = 200
    return response


@app.route('/add', methods=['POST'])
async def add(payload = None):
    #checks lagana hai
    payload = await request.get_json()
    num_new_servers = payload['n']
    num_servers += num_new_servers

    new_shards = payload['new_shards']
    num_shards += len(new_shards)

    servers = payload['servers']

    for shard_data in new_shards:
        shard_to_data[shard_data["Shard_id"]] = shard_data

    # if less than n toh randomly init serverid
    message = "Add "
    for server_name, shard_list in servers:
        # if already exists
        if server_name not in server_hostname_to_id:
            server_id = random.randint(100000, 999999)
            server_id_to_hostname[server_id] = server_name
            server_hostname_to_id[server_name] = server_id
            
        server_to_shards[server_name] = shard_list
        for shard in shard_list:
            if shard not in shards_to_server:
                shards_to_server[shard] = []
            shards_to_server[shard].append(server_name)

        try:
            res = client.containers.run(image=image, name=server_name, network=network, detach=True, environment={'SERV_ID': server_id})
        except Exception as e:
            print(e)
            response = jsonify(message = '<Error> Failed to spawn new docker container', status = 'failure')
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

                    if response.status != 200:
                        response = jsonify(message = f'<Error> Failed to configure server {server_name}', status = 'failure')
                        response.status_code = 400
                        return response

        except Exception as e:
            print(e)
            response = jsonify(message = '<Error> Failed to configure server', status = 'failure')
            response.status_code = 400
            return response
        
        message += server_name + ", "
    message = message[:-2]
    
    response_json = {}
    response_json['N'] = len(server_hostname_to_id)
    response_json['message'] = message
    response_json['status'] = 'successful'

    response = jsonify(message = response_json, status = 'success')
    response.status_code = 200
    return response


@app.route('/rm', methods=['DELETE'])
async def rm(payload = None):
    payload = await request.get_json()
    num_rm_servers = payload['n']
    rm_servers = payload['servers']

    if num_rm_servers is None or rm_servers is None:
        response = jsonify(message = '<Error> Invalid payload', status = 'failure')
        response.status_code = 400
        return response

    # if there are more servers in rm_servers than the number of servers in the network, then throw error
    if num_rm_servers < len(rm_servers):
        response = jsonify(message = '<Error> Length of hostname list is more than removable instances', status = 'failure')
        response.status_code = 400
        return response
    
    # if there is a server in rm_servers that is not present in containers, then throw error
    containers = server_hostname_to_id.keys()
    for server in rm_servers:
        if server not in containers:
            response = jsonify(message = '<Error> Server not found', status = 'failure')
            response.status_code = 400
            return response

    # if there are not enough servers in rm_servers, then randomly select servers from containers
    if num_rm_servers > len(rm_servers):
        other_servers = list(set(containers) - set(rm_servers))
        num_needed = num_rm_servers - len(rm_servers)

        # randomly select num_needed servers from other_servers
        rm_servers += random.sample(other_servers, num_needed)

    for server in rm_servers:
        # remove the server from the consistent hash map
        server_id = server_hostname_to_id[server]
        ch.remove_server(server_id)
        server_id_to_hostname.pop(server_id)
        server_hostname_to_id.pop(server)

        # remove the docker container
        try:
            container = client.containers.get(server)
            container.stop()
            container.remove()
        except Exception as e:
            print(e)
            response = jsonify(message = '<Error> Failed to remove docker container', 
                        status = 'failure')
            response.status_code = 400
            return response

    containers = server_hostname_to_id.keys()

    message = {
        'N': len(containers),
        'servers': list(containers)
    }

    response = jsonify(message = message, status = 'successful')
    response.status_code = 200
    return response


@app.route('/read', methods=['POST'])
async def read(payload = None):
    payload = await request.get_json()

    global resource, readers_waiting, writers_waiting, write_in_progress, writer_priority
    await lock.acquire()
    while write_in_progress or (writer_priority and writers_waiting > 0):
        readers_waiting += 1
        lock.release()
        await asyncio.sleep(0.1)
        await lock.acquire()
        readers_waiting -= 1
    lock.release()

    # critical section for reader

    response = jsonify(message = '.', status = 'success')
    response.status_code = 200
    return response

@app.route('/write', methods=['POST'])
async def write(payload = None):
    payload = await request.get_json()

    global resource, readers_waiting, writers_waiting, write_in_progress, writer_priority
    await lock.acquire()
    writers_waiting += 1
    while write_in_progress or (readers_waiting > 0):
        lock.release()
        await asyncio.sleep(0.1)
        await lock.acquire()
    writers_waiting -= 1
    write_in_progress = True
    lock.release()

    # critical section for reader

    resource += 1
    write_in_progress = False

    response = jsonify(message = '.', status = 'success')
    response.status_code = 200
    return response

@app.route('/update', methods=['PUT'])
async def update(payload = None):
    payload = await request.get_json()

@app.route('/del', methods=['DELETE'])
async def delete(payload = None):
    payload = await request.get_json()




if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)