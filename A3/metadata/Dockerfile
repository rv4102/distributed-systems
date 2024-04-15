FROM ubuntu:22.04

WORKDIR /app
COPY ./requirements.txt .

ENV PYTHONUNBUFFERED=1
RUN apt-get update
RUN apt-get install -y python3 python3-pip
RUN python3 -m pip install -r requirements.txt

# this line is moved here so that changes in source .py file do not cause python packages to be reinstalled and cached version can be used
COPY . .
WORKDIR /app

# RUN echo "SERV_ID=$(bash -c 'echo \""$RANDOM"\"')" >> .env
EXPOSE 5000
CMD ["python3", "metadata.py"]