# 安装 Dragonfly Dfdaemon

本文档阐述如何安装并启动 Dragonfly Dfdaemon。

## 环境要求

使用 Docker 部署时，以下条件必须满足：

| 所需软件 | 版本要求 |
| -------- | -------- |
| Git      | 1.9.1+   |
| Docker   | 1.12.0+  |

## 使用 Docker 部署

### 获取 dfdaemon 镜像

您可以直接从 [DockerHub](https://hub.docker.com/) 获取 dfdaemon 镜像。

1. 获取最新的 dfdaemon 镜像

   ```sh
   docker pull dragonflyoss/dfdaemon
   ```

或者您可以构建自己的 dfdaemon 镜像

1. 获取 Dragonfly 的源码

   ```sh
   git clone https://github.com/dragonflyoss/Dragonfly2.git
   ```

2. 打开项目文件夹

   ```sh
   cd Dragonfly2
   ```

3. 构建 dfdaemon 的 Docker 镜像

   ```sh
   TAG="2.0.0"
   make docker-build-dfdaemon D7Y_VERSION=$TAG
   ```

4. 获取最新的 dfdaemon 镜像 ID

   ```sh
   docker image ls | grep 'dfdaemon' | awk '{print $3}' | head -n1
   ```

### 启动 dfdaemon

**注意：** 需要使用上述步骤获得的 ID 替换 ${dfdaemonDockerImageId}。

```sh
docker run -d --name dfdaemon --restart=always -p 65000:65000 \
    -p 65001:65001 -p 65002:65002 ${dfdaemonDockerImageId} daemon
```

dfget 部署完成之后，运行以下命令以检查 **dfdaemon** 是否正在运行，以及 `65000`, `65001` 和 `65002` 端口是否可用。

```sh
telnet 127.0.0.1 65000
telnet 127.0.0.1 65001
telnet 127.0.0.1 65002
```

### 检查

- dfget 部署完成之后，运行以下命令以检查 **dfdaemon** 是否正在运行，以及 `65000`, `65001` 和 `65002` 端口是否可用。

```sh
telnet 127.0.0.1 65000
telnet 127.0.0.1 65001
telnet 127.0.0.1 65002
```
