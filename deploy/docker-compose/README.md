# Deploying Dragonfly with Container Directly

> Currently, docker compose deploying is tested just in single host, no HA support.

## Deploy with Docker Compose

The `run.sh` script will generate config and deploy all components with `docker-compose`.

Just run:

```shell
# Without network=host mode,the HOST IP would be the docker network gateway IP address, use the command below to
# obtain the ip address, "docker network inspect bridge -f '{{range .IPAM.Config}}{{.Gateway}}{{end}}'"
export IP=<host ip>
./run.sh
```

## Delete containers with docker compose

```shell
docker compose down
```
