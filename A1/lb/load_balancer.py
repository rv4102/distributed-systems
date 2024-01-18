from consistent_hashing import ConsistentHashMap
from flask import Flask, jsonify, redirect, request
import docker
import random

app = Flask(__name__)
ch = ConsistentHashMap()
client = docker.from_env()
network = "a1_default"
image = "serv"

server_id_to_hostname = {}
server_hostname_to_id = {}

@app.route('/rep', methods=['GET'])
def rep():
    # get all containers in the docker internal network
    containers = client.containers.list(filters={'network':network})
    
    message = {
        'N': len(containers),
        'replicas': [container.name for container in containers]
    }

    response = jsonify({'message': message, 'status': 'successful'})
    response.status_code = 200
    return response


@app.route('/add', methods=['POST'])
def add():
    # get the request data
    data = request.get_json()
    num_new_servers = data['n']
    new_servers = data['hostnames']

    if num_new_servers != len(new_servers):
        response = jsonify({'message': '<Error> Length of hostname list does not match number of newly added instances', 
                    'status': 'failure'})
        response.status_code = 400
        return response

    for server in new_servers:
        server_id = random.randint(100000, 999999)

        # spawn docker containers for the new servers
        try:
            res = client.containers.run(image=image, name=server, network=network, detach=True, environment={'SERV_ID': server_id})
        except Exception as e:
            print(e)
            response = jsonify({'message': '<Error> Failed to spawn new docker container', 
                        'status': 'failure'})
            response.status_code = 400
            return response

        # add the new servers to the consistent hash map
        ch.add_server(server_id)
        server_id_to_hostname[server_id] = server
        server_hostname_to_id[server] = server_id

    print("added containers")

    # get the updated list of containers
    containers = client.containers.list(filters={'network':network})

    message = {
        'N': len(containers),
        'replicas': [container.name for container in containers]
    }

    print("sending response")

    response = jsonify({'message': message, 'status': 'successful'})
    response.status_code = 200
    return response


@app.route('/rm', methods=['DELETE'])
def remove():
    # get the request data
    data = request.get_json()
    num_rm_servers = data['n']
    rm_servers = data['hostnames']

    if num_rm_servers < len(rm_servers):
        response = jsonify({'message': '<Error> Length of hostname list is more than removable instances', 
                    'status': 'failure'})
        response.status_code = 400
        return response

    if num_rm_servers > len(rm_servers):
        containers = client.containers.list(filters={'network':network})
        containers = [container.name for container in containers]
        other_servers = list(set(containers) - set(rm_servers))
        num_needed = num_rm_servers - len(rm_servers)

        # randomly select num_needed servers from other_servers
        rm_servers += random.sample(other_servers, num_needed)

    for server in rm_servers:
        # remove the server from the consistent hash map
        if server not in server_hostname_to_id:
            response = jsonify({'message': '<Error> Server not found', 
                        'status': 'failure'})
            response.status_code = 400
            return response
        
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
            response = jsonify({'message': '<Error> Failed to remove docker container', 
                        'status': 'failure'})
            response.status_code = 400
            return response

    # get the updated list of containers
    containers = client.containers.list(filters={'network':network})
    message = {
        'N': len(containers),
        'replicas': [container.name for container in containers]
    }

    response = jsonify({'message': message, 'status': 'successful'})
    response.status_code = 200
    return response


@app.route('/<path:path>', methods=['GET'])
def get(path='home'):
    if not (path == 'home' or path == 'heartbeat'):
        response = jsonify({'message': '<Error> Invalid path', 'status': 'failure'})
        response.status_code = 400
        return response
    
    if len(server_id_to_hostname) == 0:
        response = jsonify({'message': '<Error> No servers created', 'status': 'failure'})
        response.status_code = 400
        return response

    # generate a 6 digit random number
    request_id = random.randint(100000, 999999)
    
    # get the server instance to handle this request
    server_id = ch.get_server(request_id=request_id)
    server = server_id_to_hostname[server_id]

    # send the request to the server instance
    url = f'http://{server}:5000/{path}'
    return redirect(url)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)