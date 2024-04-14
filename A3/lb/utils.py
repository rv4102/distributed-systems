from bisect import bisect_left, bisect_right
from collections import defaultdict
import random
import re
from typing import Dict
from quart import Quart, jsonify, Response, request
import asyncio
import aiohttp
import os
import logging
import copy
from consistent_hashing import ConsistentHashMap

app = Quart(__name__)
logging.basicConfig(level=logging.DEBUG)

async def get_data():
    async with aiohttp.ClientSession() as session:
        async with session.post('http://metadata:5000/get_server_to_id') as resp:
            if resp.status == 200:
                result = await resp.json()
                global server_to_id
                server_to_id = result.get('server_to_id')
        async with session.post('http://metadata:5000/get_id_to_server') as resp:
            if resp.status == 200:
                result = await resp.json()
                global id_to_server
                id_to_server = result.get('id_to_server')
        async with session.post('http://metadata:5000/get_shard_to_servers') as resp:
            if resp.status == 200:
                result = await resp.json()
                global shard_to_servers
                shard_to_servers = result.get('shard_to_servers')
        async with session.post('http://metadata:5000/get_servers_to_shard') as resp:
            if resp.status == 200:
                result = await resp.json()
                global servers_to_shard
                servers_to_shard = result.get('servers_to_shard')
        async with session.post('http://metadata:5000/get_available_servers') as resp:
            if resp.status == 200:
                result = await resp.json()
                global available_servers
                available_servers = result.get('available_servers')
        async with session.post('http://metadata:5000/get_prefix_shard_sizes') as resp:
            if resp.status == 200:
                result = await resp.json()
                global prefix_shard_sizes
                prefix_shard_sizes = result.get('prefix_shard_sizes')
        async with session.post('http://metadata:5000/get_shardT') as resp:
            if resp.status == 200:
                result = await resp.json()
                global shardT
                shardT = result.get('shardT')
        async with session.post('http://metadata:5000/get_shard_to_primary_server') as resp:
            if resp.status == 200:
                result = await resp.json()
                global shard_to_primary_server
                shard_to_primary_server = result.get('shard_to_primary_server')

async def set_data():
    async with aiohttp.ClientSession() as session:
        payload = {"server_to_id": server_to_id}
        async with session.post('http://metadata:5000/set_server_to_id', json=payload) as resp:
            if resp.status != 200:
                app.logger.error(f"Error while setting server_to_id")
        payload = {"id_to_server": id_to_server}
        async with session.post('http://metadata:5000/set_id_to_server', json=payload) as resp:
            if resp.status != 200:
                app.logger.error(f"Error while setting id_to_server")
        payload = {"shard_to_servers": shard_to_servers}
        async with session.post('http://metadata:5000/set_shard_to_servers', json=payload) as resp:
            if resp.status != 200:
                app.logger.error(f"Error while setting shard_to_servers")
        payload = {"servers_to_shard": servers_to_shard}
        async with session.post('http://metadata:5000/set_servers_to_shard', json=payload) as resp:
            if resp.status != 200:
                app.logger.error(f"Error while setting servers_to_shard")
        payload = {"available_servers": available_servers}
        async with session.post('http://metadata:5000/set_available_servers', json=payload) as resp:
            if resp.status != 200:
                app.logger.error(f"Error while setting available_servers")
        payload = {"prefix_shard_sizes": prefix_shard_sizes}
        async with session.post('http://metadata:5000/set_prefix_shard_sizes', json=payload) as resp:
            if resp.status != 200:
                app.logger.error(f"Error while setting prefix_shard_sizes")
        payload = {"shardT": shardT}
        async with session.post('http://metadata:5000/set_shardT', json=payload) as resp:
            if resp.status != 200:
                app.logger.error(f"Error while setting shardT")
        payload = {"shard_to_primary_server": shard_to_primary_server}
        async with session.post('http://metadata:5000/set_shard_to_primary_server', json=payload) as resp:
            if resp.status != 200:
                app.logger.error(f"Error while setting shard_to_primary_server")