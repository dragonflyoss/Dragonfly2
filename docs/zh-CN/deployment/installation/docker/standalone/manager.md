# 安装 Dragonfly Manager

本文档阐述如何安装并启动 Dragonfly Manger。

## 环境要求

使用 Docker 部署时，以下条件必须满足：

| 所需软件 | 版本要求 |
| -------- | -------- |
| Git      | 1.9.1+   |
| Docker   | 1.12.0+  |

## 使用 Docker 部署

### 获取 Manager 镜像

您可以直接从 [DockerHub](https://hub.docker.com/) 获取 Manager 镜像。

1. 获取最新的 Manager 镜像

   ```sh
   docker pull dragonflyoss/manager
   ```

或者您可以构建自己的 manager 镜像

1. 获取 Dragonfly 的源码

   ```sh
   git clone https://github.com/dragonflyoss/Dragonfly2.git
   ```

2. 打开项目文件夹

   ```sh
   cd Dragonfly2
   ```

3. 构建 manager 的 Docker 镜像

   ```sh
   TAG="2.0.0"
   make docker-build-manager D7Y_VERSION=$TAG
   ```

4. 获取最新的 manager 镜像 ID

   ```sh
   docker image ls | grep 'manager' | awk '{print $3}' | head -n1
   ```

### 启动 manager

**注意：** 需要使用上述步骤获得的 ID 替换 ${managerDockerImageId}。

```sh
docker run -d --name manager --restart=always -p 8080:8080 -p 65003:65003  ${managerDockerImageId}
```

manager 部署完成之后，运行以下命令以检查 **manager** 是否正在运行，以及 `8080` 和 `65003` 端口是否可用。

```sh
telnet 127.0.0.1 8080
telnet 127.0.0.1 65003
```

### 控制台

现在可以打开浏览器，访问控制台 [console](http://localhost:8080)。

控制台功能预览参考文档 [console preview](../../user-guide/console/preview.md)。
