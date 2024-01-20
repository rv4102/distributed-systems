import os
from flask import Flask
# from dotenv import load_dotenv

app = Flask(__name__)
# load_dotenv()  # take environment variables from .env.

@app.route('/home', methods=['GET'])
def home():
    # get server id using dotenv
    serv_id = os.environ.get("SERV_ID")
    response = {"message": f"Hello from Server: {serv_id}",
                "status": "successful"}
    return response, 200

@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    return "", 200

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)