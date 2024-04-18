import aiohttp

METADATA_IMAGE_NAME = "metadata"


########################## Get Functions ##########################
async def get_server_chmap(shard, request_id):
    payload = {"shard": shard, "request_id": request_id}
    async with aiohttp.ClientSession() as session:
        async with session.get(f'http://{METADATA_IMAGE_NAME}:5000/get_server_chmap', json=payload) as resp:
            if resp.status == 200:
                result = await resp.json()
                return result.get('server_id')
            else:
                return None
            
async def get_server_from_id(server_id):
    payload = {"server_id": server_id}
    async with aiohttp.ClientSession() as session:
        async with session.get(f'http://{METADATA_IMAGE_NAME}:5000/get_server_from_id', json=payload) as resp:
            if resp.status == 200:
                result = await resp.json()
                return result.get('server_name')
            else:
                return None

async def get_id_from_server(server_name):
    payload = {"server_name": server_name}
    async with aiohttp.ClientSession() as session:
        async with session.get(f'http://{METADATA_IMAGE_NAME}:5000/get_id_from_server', json=payload) as resp:
            if resp.status == 200:
                result = await resp.json()
                return result.get('server_id')
            else:
                return None

async def get_first_available_server():
    async with aiohttp.ClientSession() as session:
        async with session.get(f'http://{METADATA_IMAGE_NAME}:5000/get_first_available_server') as resp:
            if resp.status == 200:
                result = await resp.json()
                return result.get('server_id')
            else:
                return None
            
async def get_shards_from_server(server_name):
    payload = {"server_name": server_name}
    async with aiohttp.ClientSession() as session:
        async with session.get(f'http://{METADATA_IMAGE_NAME}:5000/get_shards_from_server', json=payload) as resp:
            if resp.status == 200:
                result = await resp.json()
                return result.get('shard_list')
            else:
                return None
            
async def get_primary_servers():
    async with aiohttp.ClientSession() as session:
        async with session.get(f'http://{METADATA_IMAGE_NAME}:5000/get_primary_servers') as resp:
            if resp.status == 200:
                result = await resp.json()
                return result.get('primary_servers')
            else:
                return None

async def get_prefix_shard_sizes_ds():
    async with aiohttp.ClientSession() as session:
        async with session.get(f'http://{METADATA_IMAGE_NAME}:5000/get_prefix_shard_sizes_ds') as resp:
            if resp.status == 200:
                result = await resp.json()
                return result.get('prefix_shard_sizes')
            else:
                return None
            
async def get_servers():
    async with aiohttp.ClientSession() as session:
        async with session.get(f'http://{METADATA_IMAGE_NAME}:5000/get_servers') as resp:
            if resp.status == 200:
                result = await resp.json()
                return result.get('servers')
            else:
                return None

async def get_server_to_shards_ds():
    async with aiohttp.ClientSession() as session:
        async with session.get(f'http://{METADATA_IMAGE_NAME}:5000/get_server_to_shards_ds') as resp:
            if resp.status == 200:
                result = await resp.json()
                return result.get('server_to_shards')
            else:
                return None

async def get_available_servers_count():
    async with aiohttp.ClientSession() as session:
        async with session.get(f'http://{METADATA_IMAGE_NAME}:5000/get_available_servers_count') as resp:
            if resp.status == 200:
                result = await resp.json()
                return result.get('available_servers_count')
            else:
                return None
            
async def get_shardT_ds():
    async with aiohttp.ClientSession() as session:
        async with session.get(f'http://{METADATA_IMAGE_NAME}:5000/get_shardT_ds') as resp:
            if resp.status == 200:
                result = await resp.json()
                return result.get('shardT')
            else:
                return None
            
async def get_shard_to_servers():
    async with aiohttp.ClientSession() as session:
        async with session.get(f'http://{METADATA_IMAGE_NAME}:5000/get_shard_to_servers') as resp:
            if resp.status == 200:
                result = await resp.json()
                return result.get('shard_to_servers')
            else:
                return None

async def get_shards_mapped_to_primary_server(server_name):
    payload = {"server_name": server_name}
    async with aiohttp.ClientSession() as session:
        async with session.get(f'http://{METADATA_IMAGE_NAME}:5000/get_shards_mapped_to_primary_server', json=payload) as resp:
            if resp.status == 200:
                result = await resp.json()
                return result.get('shards')
            else:
                return None
            
async def get_primary_server(shard):
    payload = {"shard": shard}
    async with aiohttp.ClientSession() as session:
        async with session.get(f'http://{METADATA_IMAGE_NAME}:5000/get_primary_server', json=payload) as resp:
            if resp.status == 200:
                result = await resp.json()
                return result.get('server')
            else:
                return None

async def get_server_to_id_ds():
    async with aiohttp.ClientSession() as session:
        async with session.get(f'http://{METADATA_IMAGE_NAME}:5000/get_server_to_id_ds') as resp:
            if resp.status == 200:
                result = await resp.json()
                return result.get('server_to_id')
            else:
                return None

async def get_available_servers_ds():
    async with aiohttp.ClientSession() as session:
        async with session.get(f'http://{METADATA_IMAGE_NAME}:5000/get_available_servers_ds') as resp:
            if resp.status == 200:
                result = await resp.json()
                return result.get('available_servers')
            else:
                return None
            
async def get_shard_servers(shard):
    payload = {"shard": shard}
    async with aiohttp.ClientSession() as session:
        async with session.get(f'http://{METADATA_IMAGE_NAME}:5000/get_shard_servers', json=payload) as resp:
            if resp.status == 200:
                result = await resp.json()
                return result.get('servers')
            else:
                return None

########################## Set / Delete Functions ##########################
async def add_server_chmap(shard, server_id):
    payload = {"server_id": server_id, "shard": shard}
    async with aiohttp.ClientSession() as session:
        async with session.post(f'http://{METADATA_IMAGE_NAME}:5000/add_server_chmap', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False

async def remove_server_chmap(shard, server_id):
    payload = {"server_id": server_id, "shard": shard}
    async with aiohttp.ClientSession() as session:
        async with session.post(f'http://{METADATA_IMAGE_NAME}:5000/remove_server_chmap', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False
            
async def set_server_to_shard(server_name, shard):
    payload = {"server_name": server_name, "shard": shard}
    async with aiohttp.ClientSession() as session:
        async with session.post(f'http://{METADATA_IMAGE_NAME}:5000/set_server_to_shard', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False

async def set_shards_to_server(server_name, shard_list):
    payload = {"shard_list": shard_list, "server_name": server_name}
    async with aiohttp.ClientSession() as session:
        async with session.post(f'http://{METADATA_IMAGE_NAME}:5000/set_shards_to_server', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False

async def set_id_to_server(server_name, server_id):
    payload = {"server_name": server_name, "server_id": server_id}
    async with aiohttp.ClientSession() as session:
        async with session.post(f'http://{METADATA_IMAGE_NAME}:5000/set_id_to_server', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False

async def set_server_to_id(server_name, server_id):
    payload = {"server_name": server_name, "server_id": server_id}
    async with aiohttp.ClientSession() as session:
        async with session.post(f'http://{METADATA_IMAGE_NAME}:5000/set_server_to_id', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False

async def delete_from_server_to_shards(server_name):
    payload = {"server_name": server_name}
    async with aiohttp.ClientSession() as session:
        async with session.post(f'http://{METADATA_IMAGE_NAME}:5000/delete_from_server_to_shards', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False

async def add_to_available_servers(server_id):
    payload = {"server_id": server_id}
    async with aiohttp.ClientSession() as session:
        async with session.post(f'http://{METADATA_IMAGE_NAME}:5000/add_to_available_servers', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False

async def delete_from_server_to_id(server_name):
    payload = {"server_name": server_name}
    async with aiohttp.ClientSession() as session:
        async with session.post(f'http://{METADATA_IMAGE_NAME}:5000/delete_from_server_to_id', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False

async def delete_from_id_to_server(server_id):
    payload = {"server_id": server_id}
    async with aiohttp.ClientSession() as session:
        async with session.post(f'http://{METADATA_IMAGE_NAME}:5000/delete_from_id_to_server', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False

async def set_prefix_shard_sizes(prefix_shard_sizes):
    payload = {"prefix_shard_sizes": prefix_shard_sizes}
    async with aiohttp.ClientSession() as session:
        async with session.post(f'http://{METADATA_IMAGE_NAME}:5000/set_prefix_shard_sizes', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False

async def set_shardT(shardT):
    payload = {"shardT": shardT}
    async with aiohttp.ClientSession() as session:
        async with session.post(f'http://{METADATA_IMAGE_NAME}:5000/set_shardT', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False

async def set_shard_to_primary_server(shard, server_name):
    payload = {"shard": shard, "server_name": server_name}
    async with aiohttp.ClientSession() as session:
        async with session.post(f'http://{METADATA_IMAGE_NAME}:5000/set_shard_to_primary_server', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False

async def set_shard_to_servers(shard_to_servers):
    payload = {"shard_to_servers": shard_to_servers}
    async with aiohttp.ClientSession() as session:
        async with session.post(f'http://{METADATA_IMAGE_NAME}:5000/set_shard_to_servers', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False

async def set_server_to_shards(server_to_shards):
    payload = {"server_to_shards": server_to_shards}
    async with aiohttp.ClientSession() as session:
        async with session.post(f'http://{METADATA_IMAGE_NAME}:5000/set_server_to_shards', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False

async def set_available_servers(available_servers):
    payload = {"available_servers": available_servers}
    async with aiohttp.ClientSession() as session:
        async with session.post(f'http://{METADATA_IMAGE_NAME}:5000/set_available_servers', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False

async def write_shard_data(serverName, shard, data):
    async with aiohttp.ClientSession() as session:
        payload = {"shard": shard, "data": data}
        async with session.post(f'http://{serverName}:5000/write', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False

async def primary_elect(shard):
    payload = {'shard': shard}
    async with aiohttp.ClientSession(trust_env=True) as client_session:
        async with client_session.get('http://shardmanager:5000/primary_elect', json=payload) as resp:
            if resp.status != 200:
                return False
            return True

async def set_primary(shard, server_name):
    payload = {"shard": shard}
    async with aiohttp.ClientSession() as session:
        async with session.post(f'http://{server_name}:5000/set_primary', json=payload) as resp:
            if resp.status != 200:
                return False
            return True

async def delete_from_shard_to_primary_server(shard):
    payload = {"shard": shard}
    async with aiohttp.ClientSession() as session:
        async with session.post(f'http://{METADATA_IMAGE_NAME}:5000/delete_from_shard_to_primary_server', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False

async def set_shard_to_primary_server(shard, server_name):
    payload = {"shard": shard, "server_name": server_name}
    async with aiohttp.ClientSession() as session:
        async with session.post(f'http://{METADATA_IMAGE_NAME}:5000/set_shard_to_primary_server', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False
            
# writes the shard data into server
async def write_shard_data(serverName, shard, data):
    async with aiohttp.ClientSession() as session:
        payload = {"shard": shard, "data": data}
        async with session.post(f'http://{serverName}:5000/write', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False
            
async def delete_from_shard_to_servers(shard, server_name):
    payload = {"shard": shard, "server_name": server_name}
    async with aiohttp.ClientSession() as session:
        async with session.post(f'http://{METADATA_IMAGE_NAME}:5000/delete_from_shard_to_servers', json=payload) as resp:
            if resp.status == 200:
                return True
            else:
                return False