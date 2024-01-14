import os
from flask import Flask

app = Flask(__name__)

@app.route('/home', methods=['GET'])
def home():
    SERV_ID = os.environ.get("SERV_ID")
    response = {"message": f"Hello from Server: {SERV_ID}",
                "status": "successful"}
    return response, 200

@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    return "", 200

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)