build:
	docker compose build

up:
	docker compose up lb

down:
	docker compose down

clean:
	# sudo docker ps -aq | xargs sudo docker rm -vf
	# # sudo docker images -aq| xargs sudo docker rmi -f
	docker stop lb metadata shardmanager Server0 Server1 Server2
	docker rm lb metadata shardmanager Server0 Server1 Server2
	docker network rm net1
