## Server

In order to run the server code, we need to set the environment variable `SERV_ID`\
Required build command : `docker build -t <image name> .`\
Required run command : `sudo docker run -p 5000:5000 -e SERV_ID="<server id>" <image name>`