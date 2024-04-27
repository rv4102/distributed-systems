from flask import Flask, jsonify, request
from mysql.connector.errors import Error
from SQLHandler import SQLHandler
import aiohttp
import os

app = Flask(__name__)
sql = SQLHandler()

primary_shards = [] 
logfile = {}
shard_to_logcount = {}

METADATA_IMAGE_NAME = "metadata"
WAL = "./LOGS/WALOG.txt"
server_name = os.environ['SERVER_NAME']



def sendlog(shard):
    return
async def get_shard_servers(shard_id):
    async with aiohttp.ClientSession() as session:
        payload = {"shard": shard_id}
        async with session.get(f'http://{METADATA_IMAGE_NAME}:5000/get_shard_servers', json=payload) as resp:
            if resp.status == 200:
                payload = await resp.json()
                return payload.get('servers')
            else:
                return None

async def write_shard_data(serverName, shard, data):
    async with aiohttp.ClientSession() as session:
        payload = {"shard": shard, "data": data}
        async with session.post(f'http://{serverName}:5000/write', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False

async def update_shard_data(serverName, shard, Stud_id, data):
    async with aiohttp.ClientSession() as session:
        payload = {"shard": shard, "Stud_id": Stud_id, "data": data}
        async with session.put(f'http://{serverName}:5000/update', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False

async def delete_shard_data(serverName, shard, Stud_id):
    async with aiohttp.ClientSession() as session:
        payload = {"shard": shard, "Stud_id": Stud_id}
        async with session.delete(f'http://{serverName}:5000/del', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False

@app.route('/config', methods=['POST'])
def configure_server():
    payload = request.get_json()
    schema = payload.get('schema')
    shards = payload.get('shards')

    if not schema or not shards:
        return jsonify({"message": "Invalid payload", "status": "error"}), 400

    if 'columns' not in schema or 'dtypes' not in schema:
        return jsonify({"message": "Invalid schema", "status": "error"}), 400

    if len(schema['columns']) != len(schema['dtypes']) or len(schema['columns']) == 0:
        return jsonify({"message": "No columns or dtypes", "status": "error"}), 400
    
    for shard in shards:
        sql.UseDB(dbname=shard)
        sql.CreateTable(tabname='studT', columns=schema['columns'], dtypes=schema['dtypes'], prikeys=['Stud_id'])

    return jsonify({"message": f"{', '.join(f'{server_name}:{shard}' for shard in shards)} configured", "status": "success"}), 200

@app.route('/copy', methods=['GET'])
def copy_data():
    payload = request.get_json()
    shards = payload.get('shards')

    if not shards:
        return jsonify({"message": "Invalid payload", "status": "error"}), 400

    response_data = {}
    for shard in shards:
        if not sql.hasDB(dbname=shard):
            return jsonify({"message": f"{server_name}:{shard} NOT FOUND", "status": "error"}), 404
        sql.UseDB(dbname=shard)
        response_data[shard] = sql.Select(table_name='studT')
        sendlog(shard)
    response_data["status"] = "success"

    return jsonify(response_data), 200


@app.route('/read', methods=['POST'])
def read_data():
    print("Read route")
    payload = request.get_json()
    shard = payload.get('shard')
    stud_id = payload.get('Stud_id')

    if not shard or not stud_id or 'low' not in stud_id or 'high' not in stud_id:
        return jsonify({"message": "Invalid payload", "status": "error"}), 400

    if not sql.hasDB(dbname=shard):
        return jsonify({"message": f"{server_name}:{shard} not found", "status": "error"}), 404

    sql.UseDB(dbname=shard)

    response_data = {"data": sql.Select(table_name="studT", col="stud_id", low=stud_id['low'], high=stud_id['high']), "status": "success"}

    return jsonify(response_data), 200

@app.route('/write', methods=['POST'])
async def write_data():
    global server_name

    payload = request.get_json()
    shard = payload.get('shard') #"sh1"
    data = payload.get('data')  #[{"Stud_id": 2255, "Stud_name": "GHI", "Stud_marks": 27}]

    if not shard or not data:
        return jsonify({"message": "Invalid payload", "status": "error"}), 400
    
    if not sql.hasDB(dbname=shard):
        return jsonify({"message": f"{server_name}:{shard} not found", "status": "error"}), 404
    
    if shard not in logfile:
        logfile[shard] = []

    logfile[shard].append(["WRITE", data, len(logfile[shard]) + 1])

    file = open(WAL, "a")
    for entry in data:
        file.write(f"WRITE {shard}, {entry['Stud_id']}, {entry['Stud_name']}, {entry['Stud_marks']}\n")

    shard_to_logcount[shard] = shard_to_logcount.get(shard, 0) + len(data)
    file.close()
    
    if shard in primary_shards:
        print(f"{server_name} is a primary server for shard {shard}")
        servers = await get_shard_servers(shard)
        if servers is None:
            return jsonify({"message": f"Error in getting servers for {shard}", "status": "error"}), 500
        for server in servers:
            if server == server_name:
                print("same server found")
                continue
            response = await write_shard_data(server, shard, data) 
            if response is False:
                return jsonify({"message": f"Error in writing data to {server}", "status": "error"}), 500 


    sql.UseDB(dbname=shard)
    sql.Insert(table_name='studT', rows=data)

    response_data = {"message": "Data entries written to database", "status": "success"}
    return jsonify(response_data), 200


@app.route('/update', methods=['PUT'])
async def update_data():
    payload = request.get_json()
    shard = payload.get('shard')
    Stud_id = payload.get('Stud_id')
    data = payload.get('data')

    if not shard or Stud_id is None or not data:
        return jsonify({"message": "Invalid payload", "status": "error"}), 400

    if not sql.hasDB(dbname=shard):
        return jsonify({"message": f"{server_name}:{shard} not found", "status": "error"}), 404

    if shard not in logfile:
        logfile[shard] = []

    logfile[shard].append(["UPDATE", data, len(logfile[shard]) + 1])
    file = open(WAL, "a")
    file.write(f"UPDATE {shard}, {Stud_id}, {data['Stud_name']}, {data['Stud_marks']}\n")
    file.close()

    shard_to_logcount[shard] = shard_to_logcount.get(shard, 0) + 1
    if shard in primary_shards:
        servers = await get_shard_servers(shard)
        if servers is None:
            return jsonify({"message": f"Error in getting servers for {shard}", "status": "error"}), 500
        for server in servers:
            if server == server_name:
                print("Same server found in update")
                continue
            response = await update_shard_data(server, shard, Stud_id, data)
            if response is False:
                return jsonify({"message": f"Error in writing data to {server}", "status": "error"}), 500
    sql.UseDB(dbname=shard)
    if not sql.Exists(table_name='studT', col="Stud_id", val=Stud_id):
        return jsonify({"message": f"Data entry for Stud_id:{Stud_id} not found", "status": "error"}), 404
    
    sql.Update(table_name='studT', col="Stud_id", val=Stud_id, data=data)
    response_data = { "message": f"Data entry for Stud_id:{Stud_id} updated", "status": "success" }
    return jsonify(response_data), 200


@app.route('/del', methods=['DELETE'])
async def delete_data():
    payload = request.get_json()
    shard = payload.get('shard')
    Stud_id = payload.get('Stud_id')

    if not shard or Stud_id is None:
        return jsonify({"message": "Invalid payload", "status": "error"}), 400

    if not sql.hasDB(dbname=shard):
        return jsonify({"message": f"{server_name}:{shard} not found", "status": "error"}), 404

    if shard not in logfile:
        logfile[shard] = []

    logfile[shard].append(["DELETE", {"Stud_id": Stud_id}, len(logfile[shard]) + 1])

    file = open(WAL, "a")
    file.write(f"DELETE {shard}, {Stud_id}\n")
    file.close()

    shard_to_logcount[shard] = shard_to_logcount.get(shard, 0) + 1
    if shard in primary_shards:
        servers = await get_shard_servers(shard)
        if servers is None:
            return jsonify({"message": f"Error in getting servers for {shard}", "status": "error"}), 500
        for server in servers:
            if server == server_name:
                continue
            response = await delete_shard_data(server, shard, Stud_id)
            if response is False:
                return jsonify({"message": f"Error in writing data to {server}", "status": "error"}), 500

    sql.UseDB(dbname=shard)
    if not sql.Exists(table_name='studT', col="Stud_id", val=Stud_id):
        return jsonify({"message": f"Data entry for Stud_id:{Stud_id} not found", "status": "error"}), 404
    sql.Delete(table_name='studT', col="Stud_id", val=Stud_id)

    response_data = {"message": f"Data entry with Stud_id:{Stud_id} removed", "status": "success"}

    return jsonify(response_data), 200

@app.route('/get_log_count', methods=['GET'])
def get_log_count():
    payload = request.get_json()
    shard = payload.get('shard')

    return jsonify({"server_name": server_name, "logcount": shard_to_logcount.get(shard, 0), "status": "success"}), 200

@app.route('/get_log', methods=['GET'])
def get_log():
    payload = request.get_json()
    shard = payload.get('shard')
    return jsonify({"log": logfile[shard], "status": "success"}), 200

@app.route('/set_primary', methods=['POST'])
def set_primary():
    print("hello")
    payload = request.get_json()
    shard = payload.get('shard')
    global primary_shards
    primary_shards.append(shard)
    return jsonify({"message": f"Primary shards set to {', '.join(primary_shards)}", "status": "success"}), 200

@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    return '', 200


@app.errorhandler(Exception)
def handle_exception(e):
    app.logger.error(f"Exception: {e}")
    if isinstance(e, Error):
        return jsonify({"message": e.msg, "status": "error"}), 400
    else:
        return jsonify({"message": "Internal server Error: check params", "status": "error"}), 500

@app.before_request
async def startup():
    app.logger.info("Starting Server")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)