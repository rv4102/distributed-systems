## Customizable Load-Balancer

1. Run `make build` to build the docker images. This calls docker compose to build the server and load-balancer images.
2. Run `make run_lb` to run the load balancer. This starts up a load balancer instance at http://localhost:5000
    1. Supported endpoints: `/home, /heartbeat, /rep, /add, /rm`.
3. Run `make run_servers` which sends a curl request to the load balancer to start 3 server instances with names 's1', 's2' and 's3'.
4. To stop all running containers, run `make stop`.
5. To remove all images and networks, run `make rm`.