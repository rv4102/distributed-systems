import requests
import aiohttp
import asyncio
import random
import re
import matplotlib.pyplot as plt

def extract_serv_id(input_string):
    match = re.search(r"Hello from Server: (\S+)", input_string)
    if match:
        return match.group(1)  # This is the server ID
    else:
        return None
def add(n = 1, hostnames = ["s1"]):
    r = requests.post('http://localhost:5000/add', json={"n": n, "hostnames": hostnames})
    print(r.json())

def rm(n = 1, hostnames = ["s1"]):
    r = requests.delete('http://localhost:5000/rm', json={"n": n, "hostnames": hostnames})
    print(r.json())

async def request(session, url):
    # Example of an async GET request
    async with session.get(url) as response:
        return await response.json()


async def main():
    url = 'http://localhost:5000/home'
    responses = None
    dic = {}
    async with aiohttp.ClientSession() as session:
        tasks = []
        for _ in range(1000):
            # generate a random url from urls list
            tasks.append(request(session, url))
        responses = await asyncio.gather(*tasks)
        print(responses)
    for response in responses:
        message = response['message']
        serv_id = extract_serv_id(message)
        print(f"Server ID: {serv_id}")
        if serv_id in dic:
            dic[serv_id] += 1
        else:
            dic[serv_id] = 1
    
    print(dic)
    # plot the histogram of server ids
    plt.bar(dic.keys(), dic.values(), color='g')
    # save it to a file
    # label axis
    plt.xlabel('Server ID')
    plt.ylabel('Number of requests')

    plt.savefig("stats.png")

    plt.show()

if __name__ == '__main__':
    asyncio.run(main())
