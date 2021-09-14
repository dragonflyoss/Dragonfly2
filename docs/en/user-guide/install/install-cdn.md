# Installing Dragonfly CDN Server

This topic explains how to install the Dragonfly CDN server.

## Context

Install CDN in one of the following ways:

- Deploying with Docker.
- Deploying with physical machines: Recommended for production usage.

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
docker run -d --name cdn --restart=always -p 8001:8001 -p 8003:8003 -v /home/admin/ftp:/home/admin/ftp ${cdnDockerImageId} 
--download-port=8001
```

After cdn is installed, run the following commands to verify if Nginx and **cdn** are started, and if Port `8001` and `8003` are available.

```sh
telnet 127.0.0.1 8001
telnet 127.0.0.1 8003
```

## Procedure - When Deploying with Physical Machines

### Get cdn executable file

1. Download a binary package of the cdn. You can download one of the latest builds for Dragonfly on the [github releases page](https://github.
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

3. Move the `cdn` to your `PATH` environment variable to make sure you can directly use `cdn` command.

Or you can build your own cdn executable file.

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
make build-cdn && make install-cdn
```

### Start cdn

```sh
cdnHomeDir=/home/admin
cdnDownloadPort=8001
cdn --port=8003 --download-port=$cdnDownloadPort
```

### Start file server

You can start a file server in any way. However, the following conditions must be met:

- It must be rooted at `cdnHomeDir/ftp` which is defined in the previous step.
- It must listen on the port `cdnDownloadPort` which is defined in the previous step.

Let's take nginx as an example.

1. Add the following configuration items to the Nginx configuration file.

```conf
server {
# Must be ${cdnDownloadPort}
listen 8001;
location / {
 # Must be ${cdnHomeDir}/ftp
 root /home/admin/ftp;
}
}
```

2. Start Nginx.

```sh
sudo nginx
```

After cdn is installed, run the following commands to verify if Nginx and **cdn** are started, and if Port `8001` and `8003` are available.

```sh
telnet 127.0.0.1 8001
telnet 127.0.0.1 8003
```
