import requests

def add(n = 1, hostnames = ["s1"]):
    r = requests.post('http://localhost:5000/add', json={"n": n, "hostnames": hostnames})
    print(r.json())

def rm(n = 1, hostnames = ["s1"]):
    r = requests.delete('http://localhost:5000/rm', json={"n": n, "hostnames": hostnames})
    print(r.json())

def rep():
    r = requests.get('http://localhost:5000/rep')
    print(r.json())

def home():
    r = requests.get('http://localhost:5000/home')
    print(r.json())

def heartbeat():
    r = requests.get('http://localhost:5000/heartbeat')
    print(r.json())

if __name__ == '__main__':
    # rm(1, ["s3"])
    # add(3, ["s1", "s2", "s3"])
    home()
