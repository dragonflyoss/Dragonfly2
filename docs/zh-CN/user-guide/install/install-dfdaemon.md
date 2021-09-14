# 安装 Dragonfly Dfdaemon

本文档阐述如何安装并启动 Dragonfly Dfdaemon。

## 部署方式

用下列方式之一部署 dfdaemon：

- 通过 Docker 部署：推荐用于生产用途
- 直接在物理机上部署

## 环境要求

使用 Docker 部署时，以下条件必须满足：

所需软件 | 版本要求
---|---
Git|1.9.1+
Docker|1.12.0+

直接在物理机上部署时，以下条件必须满足：

所需软件 | 版本要求
---|---
Git|1.9.1+
Golang|1.12.x
Nginx|0.8+

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
docker run -d --name dfdaemon --restart=always -p 65000:65000 -p 65001:65001 -p 65002:65002 ${dfdaemonDockerImageId} daemon
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

## 在物理机上部署

### 获取 dfget 可执行文件

1. 下载 Dragonfly 项目的压缩包。您可以从 [github releases page](https://github.
   com/dragonflyoss/Dragonfly2/releases) 下载一个已发布的最近版本

```sh
version=2.0.0
wget https://github.com/dragonflyoss/Dragonfly2/releases/download/v$version/Dragonfly2_$version_linux_amd64.tar.gz
```

2. 解压压缩包

```bash
# Replace `xxx` with the installation directory.
tar -zxf Dragonfly2_2.0.0_linux_amd64.tar.gz -C xxx
```

3. 把 `dfget` 移动到环境变量 `PATH` 下以确保您可以直接使用 `dfget` 命令

或者您可以编译生成自己的 dfget 可执行文件。

1. 获取 Dragonfly 的源码

```sh
git clone https://github.com/dragonflyoss/Dragonfly2.git
```

2. 打开项目文件夹

```sh
cd Dragonfly2
```

3. 编译源码

```sh
make build-dfget && make install-dfget
```

### 启动 dfdaemon

```sh
dfget dfdaemon --options
```

dfget 部署完成之后，运行以下命令以检查 **dfdaemon** 是否正在运行，以及 `65000`, `65001` 和 `65002` 端口是否可用。

```sh
telnet 127.0.0.1 65000
telnet 127.0.0.1 65001
telnet 127.0.0.1 65002
```
