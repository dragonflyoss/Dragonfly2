# Installing Dragonfly Dfdaemon Server

This topic explains how to install the Dragonfly dfdaemon server.

## Prerequisites

When deploying with Docker, the following conditions must be met.

Required Software | Version Limit
---|---
Git|1.9.1+
Docker|1.12.0+

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
    TAG="2.0.0"
    make docker-build-dfdaemon D7Y_VERSION=$TAG
    ```

4. Obtain the latest Docker image ID of the dfdaemon.

    ```sh
    docker image ls | grep 'dfdaemon' | awk '{print $3}' | head -n1
    ```

### Start dfdaemon

**NOTE:** Replace ${dfdaemonDockerImageId} with the ID obtained at the previous step.

```sh
docker run -d --name dfdaemon --restart=always \
    -p 65000:65000 -p 65001:65001 -p 65002:65002 ${dfdaemonDockerImageId} daemon
```

After dfget is installed, run the following commands to
verify if **dfdaemon** is started,
and if Port `65000`, `65001` and `65002` is available.

```sh
telnet 127.0.0.1 65000
telnet 127.0.0.1 65001
telnet 127.0.0.1 65002
```
