# Installing Dragonfly Scheduler Server

This topic explains how to install the Dragonfly scheduler server.

## Context

Install scheduler in one of the following ways:

- Deploying with Docker: Recommended for production usage.
- Deploying with physical machines.

## Prerequisites

When deploying with Docker, the following conditions must be met.

Required Software | Version Limit
---|---
Git|1.9.1+
Docker|1.12.0+

When deploying with physical machines, the following conditions must be met.

Required Software | Version Limit
---|---
Git|1.9.1+
Golang|1.12.x
Nginx|0.8+

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

## Procedure - When Deploying with Physical Machines

### Get scheduler executable file

1. Download a binary package of the scheduler. You can download one of the latest builds for Dragonfly on the [github releases page](https://github.
   com/dragonflyoss/Dragonfly2/releases).

```sh
version=2.0.0
wget https://github.com/dragonflyoss/Dragonfly2/releases/download/v$version/Dragonfly2_$version_linux_amd64.tar.gz
```

2. Unzip the package.

```bash
# Replace `xxx` with the installation directory.
tar -zxf Dragonfly2_2.0.0_linux_amd64.tar.gz -C xxx
```

3. Move the `scheduler` to your `PATH` environment variable to make sure you can directly use `scheduler` command.

Or you can build your own scheduler executable file.

1. Obtain the source code of Dragonfly.

```sh
git clone https://github.com/dragonflyoss/Dragonfly2.git
```

2. Enter the project directory.

```sh
cd Dragonfly2
```

3. Compile the source code.

```sh
make build-scheduler && make install-scheduler
```

### Start scheduler

```sh
scheduler --options
```

After scheduler is installed, run the following commands to verify if **scheduler** is started, and if Port `8002` is available.

```sh
telnet 127.0.0.1 8002
```
