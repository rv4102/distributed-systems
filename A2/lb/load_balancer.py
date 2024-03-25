from consistent_hashing import ConsistentHashMap
from quart import Quart, jsonify, Response, request
import asyncio
import aiohttp
import docker
import random
import pymysql

app = Quart(__name__)
client = docker.from_env()
connection = pymysql.connect(host='localhost',
                            user='root',
                            password='root',
                            database='db',
                            cursorclass=pymysql.cursors.DictCursor)

# locks and mutexes
locks = {}
resource = {}
readers_waiting = {}
writers_waiting = {}
write_in_progress = {}
writer_priority = {}

# other globals
num_servers = 0
num_shards = 0
schema = {}
consistent_hash_maps = {}

# server_to_shards = {}
# shards_to_server = {}
# shard_to_data = {}

network = "n1"
image = "serv"

# def get_shard_id(stud_id):
#     for shard_data in shard_to_data:
#         stud_id_low = shard_data["Stud_id_low"]
#         shard_size = shard_data["Shard_size"]
#         if stud_id >= stud_id_low and stud_id < stud_id_low + shard_size:
#             return shard_data["Shard_id"]
#     return None  # If no shard matches

# async def before_critical_write(shard_id):
#     global resource, readers_waiting, writers_waiting, write_in_progress, writer_priority
#     await locks[shard_id].acquire()
#     writers_waiting[shard_id] += 1
#     while write_in_progress[shard_id] or (readers_waiting[shard_id] > 0):
#         locks[shard_id].release()
#         await asyncio.sleep(0.1)
#         await locks[shard_id].acquire()
#     writers_waiting[shard_id] -= 1
#     write_in_progress[shard_id] = True
#     locks[shard_id].release()
#     return

# async def after_critical_write(shard_id):
#     async with locks[shard_id]:
#         write_in_progress[shard_id] = False
#     return

# logn wala implement kar dena uske liye dictionary ko array banana padega
def get_shard_id(stud_id):
    with connection.cursor() as cursor:
        cursor.execute("SELECT MAX(Stud_id_low) FROM ShardT WHERE Stud_id_low <= %s", (stud_id))
        temp = cursor.fetchone()
        connection.commit()
    
    # for shard_id, shard_info in shard_to_data.items():
    #     stud_id_low = shard_info["Stud_id_low"]
    #     shard_size = shard_info["Shard_size"]
    #     if stud_id_low <= stud_id < stud_id_low + shard_size:
    #         return shard_id
    # return None  # If student ID doesn't belong to any shard
    
    if temp is None:
        return None
    return temp['Shard_id']

# async def check_heartbeat(server_name = None):
#     url = f'http://{server_name}:5000/heartbeat'
#     try:
#         async with aiohttp.ClientSession() as client_session:
#             async with client_session.get(url) as response:
#                 if response.status == 200:
#                     return True
#                 else:
#                     return False
#     except Exception as e:
#         return False

# ## Abhi bas copy pasted hai isko theek karna hoga
# async def periodic_server_monitor(interval = 1):
#     while True:
#         dead_servers = []
#         tasks = [check_heartbeat(server_name) for server_name in server_hostname_to_id.keys()]
#         results = await asyncio.gather(*tasks)
#         results = zip(server_hostname_to_id.keys(), results)
#         for server_name, result in results:
#             if result == False:
#                 server_id = server_hostname_to_id[server_name]
#                 ch.remove_server(server_id)
#                 dead_servers.append(server_name)
        
#         for server_name in dead_servers:
#             server_id = server_hostname_to_id[server_name]

#             try:
#                 res = client.containers.run(image=image, name=server_name, network=network, detach=True, environment={'SERV_ID': server_id})
#             except Exception as e:
#                 print(e)

#             ch.add_server(server_id)
        
#         await asyncio.sleep(interval)

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
        shard_id = shard_data['Shard_id']
        consistent_hash_maps[shard_id] = ConsistentHashMap()

        stud_id_low = shard_data["Stud_id_low"]
        shard_size = shard_data["Shard_size"]

        # insert into ShardT
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO ShardT (Shard_id, Stud_id_low, Shard_size, valid_idx) VALUES (%s, %s, %s, %s)", (shard_id, stud_id_low, shard_size, 0))
            connection.commit()
    
    for server_name, shard_list in servers.items():
        # # not sure if needed ######################
        # if server_name not in server_hostname_to_id:
        #     server_id = random.randint(100000, 999999)
        #     server_id_to_hostname[server_id] = server_name
        #     server_hostname_to_id[server_name] = server_id

        # insert shard_id to server_id mapping into MapT
        server_id = None
        for shard_id in shard_list:
            if server_id is None:
                server_id = consistent_hash_maps[shard_id].get_id_from_name(server_name)
            with connection.cursor() as cursor:
                cursor.execute("INSERT INTO MapT (Shard_id, Server_id) VALUES (%s, %s)", (shard_id, server_id))
                connection.commit()

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
    # response_content = {}

    # get number of servers from MapT
    with connection.cursor() as cursor:
        cursor.execute("SELECT DISTINCT Server_id FROM MapT")
        temp = cursor.fetchall()
        connection.commit()
    # response_content['N'] = len(temp)

    # retrieve shard data from ShardT
    with connection.cursor() as cursor:
        cursor.execute("SELECT Shard_id, Stud_id_low, Shard_size FROM ShardT")
        shard_data = cursor.fetchall()
        connection.commit()
    # response_content['shards'] = shard_data

    # retrieve data from MapT
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM MapT")
        temp = cursor.fetchall()
        connection.commit()
    server_map = {}
    for row in temp:
        shard_id = row['Shard_id']
        server_id = row['Server_id']
        if server_id not in server_map:
            server_map[server_id] = []
        server_map[server_id].append(shard_id)
    # response_content['servers'] = server_map

    # response_content['schema'] = schema

    response = jsonify(N = len(temp), schema = schema, shards = shard_data, servers = server_map, status = 'success')
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
        shard_id = shard_data['Shard_id']
        consistent_hash_maps[shard_id] = ConsistentHashMap()

        stud_id_low = shard_data["Stud_id_low"]
        shard_size = shard_data["Shard_size"]

        # insert into ShardT
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO ShardT (Shard_id, Stud_id_low, Shard_size, valid_idx) VALUES (%s, %s, %s, %s)", (shard_id, stud_id_low, shard_size, 0))
            connection.commit()

    # if less than n toh randomly init serverid
    message = "Add "
    for server_name, shard_list in servers:
        # # if already exists
        # if server_name not in server_hostname_to_id:
        #     server_id = random.randint(100000, 999999)
        #     server_id_to_hostname[server_id] = server_name
        #     server_hostname_to_id[server_name] = server_id
        server_id = None
        for shard_id in shard_list:
            if server_id is None:
                server_id = consistent_hash_maps[shard_id].get_id_from_name(server_name)
            with connection.cursor() as cursor:
                cursor.execute("INSERT INTO MapT (Shard_id, Server_id) VALUES (%s, %s)", (shard_id, server_id))
                connection.commit()

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
    # get number of servers from MapT
    with connection.cursor() as cursor:
        cursor.execute("SELECT DISTINCT Server_id FROM MapT")
        temp = cursor.fetchall()
        connection.commit()
    response_json['N'] = len(temp)
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
    
    # retrieve the servers from MapT
    with connection.cursor() as cursor:
        cursor.execute("SELECT DISTINCT Server_id FROM MapT")
        temp = cursor.fetchall()
        connection.commit()
    containers = {}
    for row in temp:
        server_id = row['Server_id']
        containers[server_id] = server_id    
    
    # if there is a server in rm_servers that is not present in containers, then throw error
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

    for server_name in rm_servers:
        # identify the shards present in the server from MapT
        with connection.cursor() as cursor:
            cursor.execute("SELECT Shard_id FROM MapT WHERE Server_id = %s", (server_name))
            shard_data = cursor.fetchall()
            cursor.execute("DELETE FROM MapT WHERE Server_id = %s", (server_name))
            connection.commit()
        
        # remove the server from each shard's hash map
        for row in shard_data:
            shard_id = row['Shard_id']
            ch = consistent_hash_maps[shard_id]

            # server_id = server_hostname_to_id[server_name]
            ch.remove_server(server_name)

        # # remove the server_name from the consistent hash map
        # server_id = server_hostname_to_id[server_name]
        ch.remove_server(server_name)
        # server_id_to_hostname.pop(server_id)
        # server_hostname_to_id.pop(server_name)

        # remove the docker container
        try:
            container = client.containers.get(server_name)
            container.stop()
            container.remove()
        except Exception as e:
            print(e)
            response = jsonify(message = '<Error> Failed to remove docker container', 
                        status = 'failure')
            response.status_code = 400
            return response

    # retrieve the servers from MapT
    with connection.cursor() as cursor:
        cursor.execute("SELECT DISTINCT Server_id FROM MapT")
        temp = cursor.fetchall()
        connection.commit()
    containers = []
    for row in temp:
        server_id = row['Server_id']
        containers.append(server_id)

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
    stud_id = payload['Stud_id']
    low_idx = stud_id['low']
    high_idx = stud_id['high']

    # identify the shard_ids from the student_ids in the payload
    shard_ids = []
    with connection.cursor() as cursor:
        cursor.execute("SELECT Shard_id, Stud_id_low, Shard_size FROM ShardT")
        temp = cursor.fetchall()
        connection.commit()
    
    for row in temp:
        Stud_id_low = row['Stud_id_low']
        Stud_id_high = Stud_id_low + row['Shard_size']
        shard_id = row['Shard_id']

        if low_idx >= Stud_id_low and high_idx < Stud_id_high:
            shard_ids.append(shard_id)
    
    # retrieve data from the shards
    data = []
    for shard_id in shard_ids:
        global resource, readers_waiting, writers_waiting, write_in_progress, writer_priority
        await locks[shard_id].acquire()
        while write_in_progress[shard_id] or (writer_priority and writers_waiting[shard_id] > 0):
            readers_waiting[shard_id] += 1
            locks[shard_id].release()
            await asyncio.sleep(0.1)
            await locks[shard_id].acquire()
            readers_waiting[shard_id] -= 1

        # critical section for reader
        request_id = random.randint(100000, 999999)
        ch = consistent_hash_maps[shard_id]
        server_name = ch.get_server(request_id)

        url = f'http://{server_name}:5000/read'
        payload = {}
        payload['shard'] = shard_id
        payload['Stud_id'] = {'low': low_idx, 'high': high_idx}

        try:
            async with aiohttp.ClientSession() as client_session:
                async with client_session.post(url, json = payload) as response:
                    response_json = await response.json()
                    print(response_json)

                    if response.status != 200:
                        response = jsonify(message = f'<Error> Failed to read from server {server_name}', status = 'failure')
                        response.status_code = 400
                        locks[shard_id].release() # release the lock
                        return response
            
        except Exception as e:
            print(e)
            response = jsonify(message = '<Error> Failed to read from server', status = 'failure')
            response.status_code = 400
            locks[shard_id].release() # release the lock
            return response
        
        data += response_json['data']
        locks[shard_id].release() # release the lock

    response = jsonify(shards_queried = shard_ids, data = data, status = 'success')
    response.status_code = 200
    return response

@app.route('/write', methods=['POST'])
async def write(payload = None):
    payload = await request.get_json()
    data = payload['data']

    # group writes for each shard
    shard_id_to_data = {}
    for entry in data:
        stud_id = entry['Stud_id']
        shard_id = get_shard_id(stud_id)
        if shard_id is None:
            response = jsonify(message = '<Error> Student ID not found', status = 'failure')
            response.status_code = 400
            return response
        
        if shard_id not in shard_id_to_data:
            shard_id_to_data[shard_id] = []
        shard_id_to_data[shard_id].append(entry)

    for shard_id, data in shard_id_to_data.items():
        # acquire the lock
        global resource, readers_waiting, writers_waiting, write_in_progress, writer_priority
        await locks[shard_id].acquire()
        writers_waiting[shard_id] += 1
        while write_in_progress[shard_id] or (readers_waiting[shard_id] > 0):
            locks[shard_id].release()
            await asyncio.sleep(0.1)
            await locks[shard_id].acquire()
        writers_waiting[shard_id] -= 1
        write_in_progress[shard_id] = True

        # critical section for write
        with connection.cursor() as cursor:
            cursor.execute("SELECT Server_id FROM MapT WHERE Shard_id = %s", (shard_id))
            temp = cursor.fetchall()
            cursor.execute("SELECT valid_idx FROM ShardT WHERE Shard_id = %s", (shard_id))
            valid_idx = cursor.fetchone()['valid_idx']
            connection.commit()
        
        for row in temp:
            server_name = row['Server_id']
            url = f'http://{server_name}:5000/write'
            payload = {}
            payload['shard'] = shard_id
            payload['curr_idx'] = valid_idx
            payload['data'] = data

            try:
                async with aiohttp.ClientSession() as client_session:
                    async with client_session.post(url, json = payload) as response:
                        response_json = await response.json()
                        print(response_json)

                        if response.status != 200:
                            response = jsonify(message = f'<Error> Failed to write to server {server_name}', status = 'failure')
                            response.status_code = 400
                            write_in_progress[shard_id] = False
                            locks[shard_id].release()
                            return response

            except Exception as e:
                print(e)
                response = jsonify(message = '<Error> Failed to write to server', status = 'failure')
                response.status_code = 400
                write_in_progress[shard_id] = False
                locks[shard_id].release()
                return response

        # update the valid_idx in ShardT
        with connection.cursor() as cursor:
            cursor.execute("SELECT valid_idx FROM ShardT WHERE Shard_id = %s", (shard_id))
            temp = cursor.fetchone()
            valid_idx = temp['valid_idx']
            cursor.execute("UPDATE ShardT SET valid_idx = %s WHERE Shard_id = %s", (valid_idx + len(data), shard_id))
            connection.commit()

        # release the lock
        write_in_progress[shard_id] = False
        locks[shard_id].release()

    response = jsonify(message = f'{len(data)} Data entries added', status = 'success')
    response.status_code = 200
    return response

    # await before_critical_write(shard)

    # # critical section for write
    # with connection.cursor() as cursor:
    #     cursor.execute("SELECT Server_id FROM MapT WHERE Shard_id = %s", (shard))
    #     temp = cursor.fetchall()
    #     connection.commit()
    
    # for row in temp:
    #     server_name = row['Server_id']
    #     url = f'http://{server_name}:5000/write'
    #     payload = {}
    #     payload['shard'] = shard
    #     payload['data'] = data

    #     try:
    #         async with aiohttp.ClientSession() as client_session:
    #             async with client_session.post(url, json = payload) as response:
    #                 response_json = await response.json()
    #                 print(response_json)

    #                 if response.status != 200:
    #                     response = jsonify(message = f'<Error> Failed to write to server {server_name}', status = 'failure')
    #                     response.status_code = 400
    #                     after_critical_write(shard)
    #                     return response

    #     except Exception as e:
    #         print(e)
    #         response = jsonify(message = '<Error> Failed to write to server', status = 'failure')
    #         response.status_code = 400
    #         after_critical_write(shard)
    #         return response

    # after_critical_write(shard)

@app.route('/update', methods=['PUT'])
async def update(payload = None):
    payload = await request.get_json()
    stud_id = payload['Stud_id']
    data = payload['data']
    shard_id = get_shard_id(stud_id)

    if shard_id is None:
        response = jsonify(message = '<Error> Student ID not found', status = 'failure')
        response.status_code = 400
        return response
    
    # acquire the lock
    global resource, readers_waiting, writers_waiting, write_in_progress, writer_priority
    await locks[shard_id].acquire()
    writers_waiting[shard_id] += 1
    while write_in_progress[shard_id] or (readers_waiting[shard_id] > 0):
        locks[shard_id].release()
        await asyncio.sleep(0.1)
        await locks[shard_id].acquire()
    writers_waiting[shard_id] -= 1
    write_in_progress[shard_id] = True

    # critical section for write
    with connection.cursor() as cursor:
        cursor.execute("SELECT Server_id FROM MapT WHERE Shard_id = %s", (shard_id))
        temp = cursor.fetchall()
        connection.commit()
        
    for row in temp:
        server_name = row['Server_id']
        url = f'http://{server_name}:5000/update'
        payload = {}
        payload['shard'] = shard_id
        payload['Stud_id'] = stud_id
        payload['data'] = data

        try:
            async with aiohttp.ClientSession() as client_session:
                async with client_session.post(url, json = payload) as response:
                    response_json = await response.json()
                    print(response_json)

                    if response.status != 200:
                        response = jsonify(message = f'<Error> Failed to write to server {server_name}', status = 'failure')
                        response.status_code = 400
                        write_in_progress[shard_id] = False
                        locks[shard_id].release()
                        return response

        except Exception as e:
            print(e)
            response = jsonify(message = '<Error> Failed to write to server', status = 'failure')
            response.status_code = 400
            write_in_progress[shard_id] = False
            locks[shard_id].release()
            return response

    # # update the valid_idx in ShardT
    # with connection.cursor() as cursor:
    #     cursor.execute("SELECT valid_idx FROM ShardT WHERE Shard_id = %s", (shard_id))
    #     temp = cursor.fetchone()
    #     valid_idx = temp['valid_idx']
    #     cursor.execute("UPDATE ShardT SET valid_idx = %s WHERE Shard_id = %s", (valid_idx + len(data), shard_id))
    #     connection.commit()

    # release the lock
    write_in_progress[shard_id] = False
    locks[shard_id].release()

    response = jsonify(message = f'Data entry for Stud_id: {stud_id} updated', status = 'success')
    response.status_code = 200
    return response
    
    # await before_critical_write(shard_id)
    
    # # critical section for write
    # with connection.cursor() as cursor:
    #     cursor.execute("SELECT Server_id FROM MapT WHERE Shard_id = %s", (shard_id))
    #     temp = cursor.fetchall()
    #     connection.commit()

    # for row in temp:
    #     server_name = row['Server_id']
    #     url = f'http://{server_name}:5000/update'
    #     payload = {}
    #     payload['shard'] = shard_id
    #     payload['Stud_id'] = stud_id
    #     payload['data'] = data

    #     try:
    #         async with aiohttp.ClientSession() as client_session:
    #             async with client_session.put(url, json = payload) as response:
    #                 response_json = await response.json()
    #                 print(response_json)

    #                 if response.status != 200:
    #                     response = jsonify(message = f'<Error> Failed to update server {server_name}', status = 'failure')
    #                     response.status_code = 400
    #                     after_critical_write(shard_id)
    #                     return response

    #     except Exception as e:
    #         print(e)
    #         response = jsonify(message = '<Error> Failed to update server', status = 'failure')
    #         response.status_code = 400
    #         after_critical_write(shard_id)
    #         return response

    # after_critical_write(shard_id)

    # response = jsonify(message = f'Data entry for Stud_id{stud_id} updated', status = 'success')
    

@app.route('/del', methods=['DELETE'])
async def delete(payload = None):
    payload = await request.get_json()
    stud_id = payload['Stud_id']
    shard_id = get_shard_id(stud_id)

    if shard_id is None:
        response = jsonify(message = '<Error> Student ID not found', status = 'failure')
        response.status_code = 400
        return response

    # acquire the lock
    global resource, readers_waiting, writers_waiting, write_in_progress, writer_priority
    await locks[shard_id].acquire()
    writers_waiting[shard_id] += 1
    while write_in_progress[shard_id] or (readers_waiting[shard_id] > 0):
        locks[shard_id].release()
        await asyncio.sleep(0.1)
        await locks[shard_id].acquire()
    writers_waiting[shard_id] -= 1
    write_in_progress[shard_id] = True

    # critical section for write
    with connection.cursor() as cursor:
        cursor.execute("SELECT Server_id FROM MapT WHERE Shard_id = %s", (shard_id))
        temp = cursor.fetchall()
        connection.commit()
        
    for row in temp:
        server_name = row['Server_id']
        url = f'http://{server_name}:5000/del'
        payload = {}
        payload['shard'] = shard_id
        payload['Stud_id'] = stud_id

        try:
            async with aiohttp.ClientSession() as client_session:
                async with client_session.post(url, json = payload) as response:
                    response_json = await response.json()
                    print(response_json)

                    if response.status != 200:
                        response = jsonify(message = f'<Error> Failed to write to server {server_name}', status = 'failure')
                        response.status_code = 400
                        write_in_progress[shard_id] = False
                        locks[shard_id].release()
                        return response

        except Exception as e:
            print(e)
            response = jsonify(message = '<Error> Failed to write to server', status = 'failure')
            response.status_code = 400
            write_in_progress[shard_id] = False
            locks[shard_id].release()
            return response

    # # update the valid_idx in ShardT
    # with connection.cursor() as cursor:
    #     cursor.execute("SELECT valid_idx FROM ShardT WHERE Shard_id = %s", (shard_id))
    #     temp = cursor.fetchone()
    #     valid_idx = temp['valid_idx']
    #     cursor.execute("UPDATE ShardT SET valid_idx = %s WHERE Shard_id = %s", (valid_idx + len(data), shard_id))
    #     connection.commit()

    # release the lock
    write_in_progress[shard_id] = False
    locks[shard_id].release()

    response = jsonify(message = f'Data entry with Stud_id:{stud_id} removed from all replicas', status = 'success')
    response.status_code = 200
    return response
    
    # await before_critical_write(shard_id)

    # # critical section for write
    # with connection.cursor() as cursor:
    #     cursor.execute("SELECT Server_id FROM MapT WHERE Shard_id = %s", (shard_id))
    #     temp = cursor.fetchall()
    #     connection.commit()
    
    # for row in temp:
    #     server_name = row['Server_id']
    #     url = f'http://{server_name}:5000/del'
    #     payload = {}
    #     payload['shard'] = shard_id
    #     payload['Stud_id'] = stud_id

    #     try:
    #         async with aiohttp.ClientSession() as client_session:
    #             async with client_session.delete(url, json = payload) as response:
    #                 response_json = await response.json()
    #                 print(response_json)

    #                 if response.status != 200:
    #                     response = jsonify(message = f'<Error> Failed to delete server {server_name}', status = 'failure')
    #                     response.status_code = 400
    #                     after_critical_write(shard_id)
    #                     return response

    #     except Exception as e:
    #         print(e)
    #         response = jsonify(message = '<Error> Failed to delete server', status = 'failure')
    #         response.status_code = 400
    #         after_critical_write(shard_id)
    #         return response
    # after_critical_write(shard_id)

    # response = jsonify(message = f'Data entry with Stud_id{stud_id} removed', status = 'success')

if __name__ == '__main__':
    # initialise the tables MapT and ShardT
    with connection.cursor() as cursor:
        cursor.execute("CREATE TABLE IF NOT EXISTS MapT (Shard_id INT, Server_id INT)")
        cursor.execute("CREATE TABLE IF NOT EXISTS ShardT (Shard_id INT PRIMARY KEY, Stud_id_low INT, Shard_size INT, valid_idx INT)")
        connection.commit()

    app.run(host='0.0.0.0', port=5000)