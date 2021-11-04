# Installing Dragonfly CDN Server

This topic explains how to install the Dragonfly CDN server.

## Prerequisites

When deploying with Docker, the following conditions must be met.

Required Software | Version Limit
---|---
Git|1.9.1+
Docker|1.12.0+

## Procedure - When Deploying with Docker

### Get cdn image

You can get it from [DockerHub](https://hub.docker.com/) directly.

1. Obtain the latest Docker image of the cdn.

    ```sh
    docker pull dragonflyoss/cdn
    ```

Or you can build your own cdn image.

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
    make docker-build-cdn D7Y_VERSION=$TAG
    ```

4. Obtain the latest Docker image ID of the cdn.

    ```sh
    docker image ls | grep 'cdn' | awk '{print $3}' | head -n1
    ```

### Start cdn

**NOTE:** Replace ${cdnDockerImageId} with the ID obtained at the previous step.

```sh
docker run -d --name cdn --restart=always \
    -p 8001:8001 -p 8003:8003 \
    -v /home/admin/ftp:/home/admin/ftp ${cdnDockerImageId} \ 
    --download-port=8001
```

After cdn is installed, run the following commands to
verify if Nginx and **cdn** are started,
and if Port `8001` and `8003` are available.

```sh
telnet 127.0.0.1 8001
telnet 127.0.0.1 8003
```
