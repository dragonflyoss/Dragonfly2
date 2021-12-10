# Deploying Dragonfly with Container Directly

> Currently, docker compose deploying is tested just in single host, no HA support.

## Deploy with Docker Compose

The `run.sh` script will generate config and deploy all components with `docker-compose`.

Just run:

```shell
export IP=<host ip>
./run.sh
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
