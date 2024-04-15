FROM mysql:8.0-debian

COPY deploy.sh /always-initdb.d/
WORKDIR /app
COPY ./requirements.txt .
COPY ./custom-entry.sh .

ENV PYTHONUNBUFFERED=1
RUN apt-get update
RUN apt-get install -y python3
RUN  apt-get install -y python3-pip

RUN pip install --break-system-packages --upgrade pip
RUN pip install --break-system-packages -r requirements.txt 

# this line is moved here so that changes in source .py file do not cause python packages to be reinstalled and cached version can be used
COPY . .
WORKDIR /app

RUN chmod +x /app/custom-entry.sh

RUN adduser --disabled-password --gecos '' newuser \
    && adduser newuser sudo \
    && echo '%sudo ALL=(ALL:ALL) ALL' >> /etc/sudoers
RUN mkdir -p /app/LOGS
RUN chown newuser /app/LOGS
USER newuser

ENV MYSQL_ROOT_PASSWORD="abc"

EXPOSE 5000

ENTRYPOINT ["/app/custom-entry.sh"]
CMD ["mysqld"]