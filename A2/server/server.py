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

    columns = schema['columns']
    dtypes = schema['dtypes']
    
    for i in range(len(dtypes)):
        if dtypes[i] == 'Number':
            dtypes[i] = 'INT'
        elif dtypes[i] == 'String':
            dtypes[i] = 'VARCHAR(255)'

    # error checking

    # setup shards
    with connection.cursor() as cursor:
        for shard in shards:
            query = f"CREATE TABLE IF NOT EXISTS {shard} ("
            for i in range(len(columns)):
                query += f"{columns[i]} {dtypes[i]}, "
            query = query[:-2] + ")"

            cursor.execute(query)
    connection.commit()

    # response
    # serv_id = os.environ.get("SERV_ID")
    serv_id = "server1"
    message = ""
    for shard in shards:
        message += serv_id + f":{shard}, "
    message = message[:-2] + " configured"

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
    message = {}
    for shard in shards:
        with connection.cursor() as cursor:
            query = f"SELECT * FROM {shard}"
            cursor.execute(query)
            result = cursor.fetchall()
        connection.commit()

        message[shard] = result
    message['status'] = "success"
    
    # response
    response = jsonify(message)
    response.status_code = 200
    return response

@app.route('/read', methods=['POST'])
async def read(payload = None):
    payload = await request.get_json()
    shard = payload['shard']
    Stud_id = payload['Stud_id']

    # error checking

    # read data
    low_idx = Stud_id['low']
    high_idx = Stud_id['high']

    with connection.cursor() as cursor:
        query = f"SELECT * FROM {shard} WHERE Stud_id BETWEEN {low_idx} AND {high_idx}"
        cursor.execute(query)
        result = cursor.fetchall()
    connection.commit()
    
    # response
    message = result
    response = jsonify(data = message, status = 'success')
    response.status_code = 200
    return response

@app.route('/write', methods=['POST'])
async def write(payload = None):
    payload = await request.get_json()
    shard = payload['shard']
    curr_idx = payload['curr_idx']
    data = payload['data']

    # error checking

    # write data
    with connection.cursor() as cursor:
        for row in data:
            query = f"INSERT INTO {shard} (Stud_id, Stud_name, Stud_marks) VALUES ('{row['Stud_id']}', '{row['Stud_name']}', '{row['Stud_marks']}')"
            cursor.execute(query)
    connection.commit()
    
    # response
    message = "Data entries added"
    curr_idx = curr_idx + len(data)
    response = jsonify(message = message, current_idx = curr_idx, status = 'success')
    response.status_code = 200
    return response

@app.route('/update', methods=['PUT'])
async def update(payload = None):
    payload = await request.get_json()
    shard = payload['shard']
    Stud_id = payload['Stud_id']
    data = payload['data']

    # error checking

    # update data
    with connection.cursor() as cursor:
        query = f"UPDATE {shard} SET Stud_name = '{data['Stud_name']}', Stud_marks = '{data['Stud_marks']}' WHERE Stud_id = '{Stud_id}'"
        cursor.execute(query)
    connection.commit()
    
    # response
    message = f"Data entry for Stud_id:{Stud_id} updated"
    response = jsonify(message = message, status = 'success')
    response.status_code = 200
    return response

@app.route('/del', methods=['DELETE'])
async def delete(payload = None):
    payload = await request.get_json()
    shard = payload['shard']
    Stud_id = payload['Stud_id']

    # error checking

    # delete data
    with connection.cursor() as cursor:
        sql = "DELETE FROM %s WHERE Stud_id = %s"
        cursor.execute(sql, (shard, Stud_id))
    connection.commit()
    
    # response
    message = f"Data entry for Stud_id: {Stud_id} removed"
    response = jsonify(message = message, status = 'success')
    response.status_code = 200
    return response


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)