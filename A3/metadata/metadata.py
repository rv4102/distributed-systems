from flask import Flask, jsonify, request
from mysql.connector.errors import Error
import aiohttp
import os

app = Flask(__name__)

shard_to_log_count = {}

server_to_id = {}
id_to_server = {}

shard_to_servers = {}
servers_to_shard = {}

available_servers = []
prefix_shard_sizes = []
shardT = []

@app.route('/get_log_count', methods=['GET'])
def get_log_count():
    payload = request.get_json()
    shard = payload.get('shard')
    return jsonify({"count": shard_to_log_count[shard], "status": "success"}), 200

@app.route('/set_log_count', methods=['POST'])
def set_log_count():
    payload = request.get_json()
    shard = payload.get('shard')
    count = payload.get('count')
    shard_to_log_count[shard] = count
    return jsonify({"message": f"Log count for {shard} set to {count}", "status": "success"}), 200



# s1 7 
# s2 8 
# s3 6 dead
# s4 10 primary

@app.route('/get_server_to_id', methods=['GET'])
def get_shard_to_id():
    return jsonify({"server_to_id": server_to_id, "status": "success"}), 200

@app.route('/get_id_to_server', methods=['GET'])
def get_id_to_server():
    return jsonify({"id_to_server": id_to_server, "status": "success"}), 200

@app.route('/get_shard_to_servers', methods=['GET'])
def get_shard_to_servers():
    return jsonify({"shard_to_servers": shard_to_servers, "status": "success"}), 200

@app.route('/get_servers_to_shard', methods=['GET'])
def get_servers_to_shard():
    return jsonify({"servers_to_shard": servers_to_shard, "status": "success"}), 200

@app.route('/get_available_servers', methods=['GET'])
def get_available_servers():
    return jsonify({"available_servers": available_servers, "status": "success"}), 200

@app.route('/get_prefix_shard_sizes', methods=['GET'])
def get_prefix_shard_sizes():
    return jsonify({"prefix_shard_sizes": prefix_shard_sizes, "status": "success"}), 200

@app.route('/get_shardT', methods=['GET'])
def get_shardT():
    return jsonify({"shardT": shardT, "status": "success"}), 200


@app.route('/set_server_to_id', methods=['POST'])
def set_server_to_id():
    payload = request.get_json()
    server_to_id_new = payload.get('server_to_id')
    for server_name, server_id in server_to_id_new.items():
        server_to_id[server_name] = int(server_id)
    return jsonify({"message": "Server to id mapping set", "status": "success"}), 200

@app.route('/set_id_to_server', methods=['POST'])
def set_id_to_server():
    payload = request.get_json()
    id_to_server_new = payload.get('id_to_server')
    for server_id, server_name in id_to_server_new.items():
        id_to_server[int(server_id)] = server_name
    return jsonify({"message": "Id to server mapping set", "status": "success"}), 200

@app.route('/set_shard_to_servers', methods=['POST'])
def set_shard_to_servers():
    payload = request.get_json()
    shard_to_servers_new = payload.get('shard_to_servers')
    for shard, servers in shard_to_servers_new.items():
        shard_to_servers[shard] = servers
    return jsonify({"message": "Shard to servers mapping set", "status": "success"}), 200

@app.route('/set_servers_to_shard', methods=['POST'])
def set_servers_to_shard():
    payload = request.get_json()
    servers_to_shard_new = payload.get('servers_to_shard')
    for server, shards in servers_to_shard_new.items():
        servers_to_shard[server] = shards
    return jsonify({"message": "Servers to shard mapping set", "status": "success"}), 200

@app.route('/set_available_servers', methods=['POST'])
def set_available_servers():
    payload = request.get_json()
    available_servers_new = payload.get('available_servers')
    available_servers.clear()
    available_servers.append(available_servers_new)
    return jsonify({"message": "Available servers set", "status": "success"}), 200

@app.route('/set_prefix_shard_sizes', methods=['POST'])
def set_prefix_shard_sizes():
    payload = request.get_json()
    prefix_shard_sizes_new = payload.get('prefix_shard_sizes')
    prefix_shard_sizes.clear()
    prefix_shard_sizes.append(prefix_shard_sizes_new)
    return jsonify({"message": "Prefix shard sizes set", "status": "success"}), 200


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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)