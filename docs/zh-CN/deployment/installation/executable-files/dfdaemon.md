# 安装 Dragonfly Dfdaemon

本文档阐述如何安装并启动 Dragonfly Dfdaemon。

## 环境要求

直接在物理机上部署时，以下条件必须满足：

| 所需软件 | 版本要求 |
| -------- | -------- |
| Git      | 1.9.1+   |
| Golang   | 1.16.x   |

## 在物理机上部署

### 获取 dfget 可执行文件

1. 下载 Dragonfly 项目的压缩包。您可以从
   [github releases page](https://github.com/dragonflyoss/Dragonfly2/releases)
   下载一个已发布的最近版本

   ```sh
   # latest version
   # version=$(https://raw.githubusercontent.com/dragonflyoss/Dragonfly2/main/version/version.latest)

   # stable version
   version=$(curl -s https://raw.githubusercontent.com/dragonflyoss/Dragonfly2/main/version/version.stable)

   wget -o Dragonfly2_linux_amd64.tar.gz \
      https://github.com/dragonflyoss/Dragonfly2/releases/download/v${version}/Dragonfly2_${version}_linux_amd64.tar.gz
   ```

2. 解压压缩包

   ```bash
   # Replace `xxx` with the installation directory.
   tar -zxf Dragonfly2_linux_amd64.tar.gz -C xxx
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
