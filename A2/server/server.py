import os
from quart import Quart, jsonify, Response, request
import pymysql

app = Quart(__name__)
connection = pymysql.connect(host='localhost',
                            user='root',
                            password='root',
                            database='db',
                            cursorclass=pymysql.cursors.DictCursor)

@app.route('/config', methods=['POST'])
async def config(payload = None):
    payload = await request.get_json()
    schema = payload['schema']
    shards = payload['shards']

    # error checking

    # setup shards

    # response
    serv_id = os.environ.get("SERV_ID")
    message = ""
    for shard in shards:
        if shard == shards[-1]:
            message += serv_id + f":{shard} configured"
        else:
            message += serv_id + f":{shard}, "

    response = jsonify(message = message, status = 'success')
    response.status_code = 200
    return response

@app.route('/heartbeat', methods=['GET'])
async def heartbeat():
    return {}, 200

@app.route('/copy', methods=['GET'])
async def copy(payload = None):
    payload = await request.get_json()
    shards = payload['shards']

    # error checking

    # copy shard data
    for shard in shards:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM %s"
            cursor.execute(sql, shard)
            result = cursor.fetchall()
            

@app.route('/home', methods=['GET'])
async def home():
    # get server id using dotenv
    serv_id = os.environ.get("SERV_ID")
    response = {"message": f"Hello from Server: {serv_id}",
                "status": "successful"}
    return response, 200


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)