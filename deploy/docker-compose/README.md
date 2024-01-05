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

## Deploy without Docker Compose

Just run:

```shell
export IP=<host ip>
./run.sh container
```

## Deploy with Other Container Runtime

Just run:

```shell
export IP=<host ip>
export RUNTIME=pouch
./run.sh container
```

## Delete containers without docker compose

```shell
./run.sh delete_container
```
