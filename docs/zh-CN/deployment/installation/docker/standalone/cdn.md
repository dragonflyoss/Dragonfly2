# 安装 Dragonfly CDN

本文档阐述如何安装并启动 Dragonfly CDN。

## 环境要求

使用 Docker 部署时，以下条件必须满足：

所需软件 | 版本要求
---|---
Git|1.9.1+
Docker|1.12.0+

## 使用 Docker 部署

### 获取 CDN 镜像

您可以直接从 [DockerHub](https://hub.docker.com/) 获取 CDN 镜像。

1. 获取最新的 CDN 镜像

    ```sh
    docker pull dragonflyoss/cdn
    ```

或者您可以构建自己的 CDN 镜像

1. 获取 Dragonfly 的源码

    ```sh
    git clone https://github.com/dragonflyoss/Dragonfly2.git
    ```

2. 打开项目文件夹

    ```sh
    cd Dragonfly2
    ```

3. 构建 CDN 的 Docker 镜像

    ```sh
    TAG="2.0.0"
    make docker-build-cdn D7Y_VERSION=$TAG
    ```

4. 获取最新的 CDN 镜像 ID

    ```sh
    docker image ls | grep 'cdn' | awk '{print $3}' | head -n1
    ```

### 启动 cdn

**注意：** 需要使用上述步骤获得的 ID 替换 ${cdnDockerImageId}。

```sh
docker run -d --name cdn --restart=always -p 8001:8001 \
    -p 8003:8003 -v /home/admin/ftp:/home/admin/ftp ${cdnDockerImageId} --download-port=8001
```

CDN 部署完成之后，运行以下命令以检查 Nginx 和 **cdn** 是否正在运行，以及 `8001` 和 `8003` 端口是否可用。

```sh
telnet 127.0.0.1 8001
telnet 127.0.0.1 8003
```
