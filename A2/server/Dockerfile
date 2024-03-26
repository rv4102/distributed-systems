FROM mysql:8.0-debian
WORKDIR /app
COPY . .
RUN apt-get update -y
ENV PYTHONUNBUFFERED=1
RUN apt-get install -y python3 python3-pip
RUN python3 -m pip install --break-system-packages --no-cache-dir -r requirements.txt
# RUN echo "SERV_ID=$(bash -c 'echo \""$RANDOM"\"')" >> .env
ENV MYSQL_ROOT_PASSWORD=root
EXPOSE 5000
CMD ["python3", "server.py"]