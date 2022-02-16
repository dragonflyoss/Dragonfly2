# 安装 Dragonfly Scheduler

本文档阐述如何安装并启动 Dragonfly Scheduler。

## 环境要求

直接在物理机上部署时，以下条件必须满足：

| 所需软件 | 版本要求 |
| -------- | -------- |
| Git      | 1.9.1+   |
| Golang   | 1.12.x   |
| Nginx    | 0.8+     |

## 在物理机上部署

### 获取 scheduler 可执行文件

1.  下载 Dragonfly 项目的压缩包。您可以从
    [github releases page](https://github.com/dragonflyoss/Dragonfly2/releases)
    下载一个已发布的最近版本

        ```sh
        version=2.0.0
        wget https://github.com/dragonflyoss/Dragonfly2/releases/download/v$version/Dragonfly2_$version_linux_amd64.tar.gz
        ```

2.  解压压缩包

    ```bash
    # Replace `xxx` with the installation directory.
    tar -zxf Dragonfly2_2.0.0_linux_amd64.tar.gz -C xxx
    ```

3.  把 `scheduler` 移动到环境变量 `PATH` 下以确保您可以直接使用 `scheduler` 命令

或者您可以编译生成自己的 scheduler 可执行文件。

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
   make build-scheduler && make install-scheduler
   ```

### 启动 scheduler

```sh
scheduler --options
```

scheduler 部署完成之后，运行以下命令以检查 **scheduler** 是否正在运行，以及 `8002` 端口是否可用。

```sh
telnet 127.0.0.1 8002
```
