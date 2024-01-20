build:
	docker compose build

run_lb:
	docker compose up lb

run_servers:
	curl --json '{"n": 3, "hostnames": ["s1", "s2", "s3"]}' http://localhost:5000/add

stop:
	for container in $$(docker ps -q); do docker stop $$container; done
	docker system prune -f

rm:
	for image in $$(docker images -q); do docker rmi $$image; done
	docker system prune -f