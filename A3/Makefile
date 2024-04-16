build:
	docker compose build

up:
	docker compose up metadata lb shardmanager

down:
	docker compose down

clean:
	-docker stop $$(docker ps -a | grep -i server | awk '{print $$1}');
	-docker rm $$(docker ps -a | grep -i server | awk '{print $$1}');
	-docker stop lb metadata shardmanager;
	-docker rm lb metadata shardmanager;
	-docker network rm net1;
