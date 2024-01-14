FROM ubuntu:22.04
WORKDIR /app
COPY . .
RUN apt update
RUN apt install -y python3 python3-pip
RUN python3 -m pip install -r requirements.txt
EXPOSE 5000
CMD ["python3", "server.py"]