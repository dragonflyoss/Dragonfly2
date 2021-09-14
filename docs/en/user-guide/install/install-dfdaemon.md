# Installing Dragonfly Dfdaemon Server

This topic explains how to install the Dragonfly dfdaemon server.

## Context

Install dfdaemon in one of the following ways:

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

### Get dfdaemon image

You can get it from [DockerHub](https://hub.docker.com/) directly.

1. Obtain the latest Docker image of the dfdaemon.

    ```sh
    docker pull dragonflyoss/dfdaemon
    ```

Or you can build your own dfdaemon image.

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
    TAG="1.0.0"
    make docker-build-dfdaemon D7Y_VERSION=$TAG
    ```

4. Obtain the latest Docker image ID of the dfdaemon.

    ```sh
    docker image ls | grep 'dfdaemon' | awk '{print $3}' | head -n1
    ```

### Start dfdaemon

**NOTE:** Replace ${dfdaemonDockerImageId} with the ID obtained at the previous step.

```sh
docker run -d --name dfdaemon --restart=always -p 8002 ${dfdaemonDockerImageId} daemon
```

## Procedure - When Deploying with Physical Machines

### Get dfget executable file

1. Download a binary package of the dfget. You can download one of the latest builds for Dragonfly on the [github releases page](https://github.
   com/dragonflyoss/Dragonfly2/releases).

    ```sh
    version=1.0.0
    wget https://github.com/dragonflyoss/Dragonfly2/releases/download/v$version/Dragonfly2_$version_linux_amd64.tar.gz
    ```

2. Unzip the package.

    ```bash
    # Replace `xxx` with the installation directory.
    tar -zxf Dragonfly2_1.0.0_linux_amd64.tar.gz -C xxx
    ```

3. Move the `dfget` to your `PATH` environment variable to make sure you can directly use `dfget` command.

Or you can build your own dfget executable file.

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
    make build-dfget && make install-dfget
    ```

### Start dfdaemon

```sh
dfget daemon --options
```
## After this Task

- After dfget is installed, run the following commands to verify if **dfdaemon** is started, and if Port `65001` is available.

    ```sh
    telnet 127.0.0.1 65001
    ```
