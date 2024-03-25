FROM mysql:latest
WORKDIR /app
COPY . .
RUN microdnf install -y yum
RUN yum update -y
RUN yum install -y python3 python3-pip gcc
RUN python3 -m pip install -r requirements.txt
# RUN echo "SERV_ID=$(bash -c 'echo \""$RANDOM"\"')" >> .env
ENV MYSQL_ROOT_PASSWORD=root
EXPOSE 5000
CMD ["python3", "server.py"]