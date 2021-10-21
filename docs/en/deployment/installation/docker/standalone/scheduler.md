# Installing Dragonfly Scheduler Server

This topic explains how to install the Dragonfly scheduler server.

## Prerequisites

When deploying with Docker, the following conditions must be met.

Required Software | Version Limit
---|---
Git|1.9.1+
Docker|1.12.0+

## Procedure - When Deploying with Docker

### Get scheduler image

You can get it from [DockerHub](https://hub.docker.com/) directly.

1. Obtain the latest Docker image of the scheduler.

```sh
docker pull dragonflyoss/scheduler
```

Or you can build your own scheduler image.

1. Obtain the source code of Dragonfly.

```sh
git clone https://github.com/dragonflyoss/Dragonfly2.git
```

2. Enter the project directory.

```sh
cd Dragonfly2
```

3. Build the Docker image.

```sh
TAG="2.0.0"
make docker-build-scheduler D7Y_VERSION=$TAG
```

4. Obtain the latest Docker image ID of the scheduler.

```sh
docker image ls | grep 'scheduler' | awk '{print $3}' | head -n1
```

### Start scheduler

**NOTE:** Replace ${schedulerDockerImageId} with the ID obtained at the previous step.

```sh
docker run -d --name scheduler --restart=always -p 8002:8002 ${schedulerDockerImageId}
```

After scheduler is installed, run the following commands to verify if **scheduler** is started, and if Port `8002` is available.

```sh
telnet 127.0.0.1 8002
```
