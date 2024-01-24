from consistent_hashing import ConsistentHashMap
from quart import Quart, jsonify, Response
import asyncio
import aiohttp
import docker
import random
import os

network = "n1"
image = "serv"
app = Quart(__name__)
ch = None
client = docker.from_env()
server_id_to_hostname = {}
server_hostname_to_id = {}

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

@app.route('/rep', methods=['GET'])
def rep():
    containers = server_hostname_to_id.keys()

    message = {
        'N': len(containers)-1,
        'replicas': list(containers)
    }

    response = jsonify(message = message, status = 'successful')
    response.status_code = 200
    return response


@app.route('/add', methods=['POST'])
def add(payload = None):
    num_new_servers = payload['n']
    new_servers = payload['hostnames']

    if num_new_servers is None or new_servers is None:
        response = jsonify(message = '<Error> Invalid payload', status = 'failure')
        response.status_code = 400
        return response

    if num_new_servers < len(new_servers):
        response = jsonify(message = '<Error> Length of hostname list does not match number of newly added instances', status = 'failure')
        response.status_code = 400
        return response
    
    # if there are more servers in new_servers than the number of server names specified, then generate random names for the rest
    if num_new_servers > len(new_servers):
        for i in range(num_new_servers - len(new_servers)):
            server_id = random.randint(100000, 999999)
            server_name = f'serv_{server_id}'
            new_servers.append(server_name)
            server_hostname_to_id[server_name] = server_id
            server_id_to_hostname[server_id] = server_name

    for server_name in new_servers:
        server_id = server_hostname_to_id[server_name]
        if server_id is None:
            server_id = random.randint(100000, 999999)
            
        try:
            res = client.containers.run(image=image, name=server_name, network=network, detach=True, environment={'SERV_ID': server_id})
        except Exception as e:
            print(e)
            response = jsonify(message = '<Error> Failed to spawn new docker container', 
                            status = 'failure')
            response.status_code = 400
            return response

        # add the new servers to the consistent hash map
        ch.add_server(server_id)
        server_id_to_hostname[server_id] = server_name
        server_hostname_to_id[server_name] = server_id

    print("added containers")

    return rep


@app.route('/rm', methods=['DELETE'])
def remove(payload = None):
    num_rm_servers = payload['n']
    rm_servers = payload['hostnames']

    if num_rm_servers is None or rm_servers is None:
        response = jsonify(message = '<Error> Invalid payload', 
                    status = 'failure')
        response.status_code = 400
        return response

    # if there are more servers in rm_servers than the number of servers in the network, then throw error
    if num_rm_servers < len(rm_servers):
        response = jsonify(message = '<Error> Length of hostname list is more than removable instances', 
                    status = 'failure')
        response.status_code = 400
        return response
    
    # if there is a server in rm_servers that is not present in containers, then throw error
    containers = server_hostname_to_id.keys()
    for server in rm_servers:
        if server not in containers:
            response = jsonify(message = '<Error> Server not found', 
                        status = 'failure')
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

    return rep


@app.route('/<path:path>', methods=['GET'])
async def get(path='home'):
    if not (path == 'home' or path == 'heartbeat'):
        response = jsonify(message = '<Error> Invalid path', status = 'failure')
        response.status_code = 404
        return response
    
    if len(server_id_to_hostname) == 0:
        response = jsonify(message = '<Error> No servers created', status = 'failure')
        response.status_code = 400
        return response

    request_id = random.randint(100000, 999999)    
    server_id = ch.get_server(request_id=request_id)
    server = server_id_to_hostname[server_id]

    # send the request to the server instance
    url = f'http://{server}:5000/{path}'
    try:
        async with aiohttp.ClientSession() as client_session:
            async with client_session.get(url) as response:
                content = await response.read()
                return Response(content, status=response.status, headers = dict(response.headers))

    except Exception as e:
        response = jsonify(message = f"{str(e)} Error in handling request", status = "failure")
        response.status_code = 400
        return response

@app.before_serving
async def startup():
    loop = asyncio.get_event_loop()
    loop.create_task(periodic_server_monitor())

if __name__ == '__main__':
    ch = ConsistentHashMap(int(os.environ['NUM_SERV']), 
                           int(os.environ['NUM_VIRT_SERV']), 
                           int(os.environ['SLOTS']))

    for i in range(int(os.environ['NUM_SERV'])):
        server_id = random.randint(100000, 999999)
        server_name = f'serv_{server_id}'
        server_id_to_hostname[server_id] = server_name
        server_hostname_to_id[server_name] = server_id
        ch.add_server(server_id)

    app.run(host='0.0.0.0', port=5000)

    