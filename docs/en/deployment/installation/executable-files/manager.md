# Installing Dragonfly Manager Server

This topic explains how to install the Dragonfly manager server.

## Prerequisites

When deploying with physical machines, the following conditions must be met.

| Required Software | Version Limit |
| ----------------- | ------------- |
| Git               | 1.9.1+        |
| Golang            | 1.12.x        |
| Nginx             | 0.8+          |

## Procedure - When Deploying with Physical Machines

### Get manager executable file

1.  Download a binary package of the manager. You can download
    one of the latest builds for Dragonfly on
    the [github releases page](https://github.com/dragonflyoss/Dragonfly2/releases).

        ```sh
        version=2.0.0
        wget https://github.com/dragonflyoss/Dragonfly2/releases/download/v$version/Dragonfly2_$version_linux_amd64.tar.gz
        ```

2.  Unzip the package.

    ```sh
    # Replace `xxx` with the installation directory.
    tar -zxf Dragonfly2_2.0.0_linux_amd64.tar.gz -C xxx
    ```

3.  Move the `manager` to your `PATH` environment variable
    to make sure you can directly use `manager` command.

Or you can build your own manager executable file.

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
   make build-manager && make install-manager
   ```

### Start manager

```sh
manager --options
```

After manager is installed, run the following commands to
verify if **manager** is started, and if Port `8080` and `65003` is available.

```sh
telnet 127.0.0.1 8080
telnet 127.0.0.1 65003
```

### Manager console

Now you can open brower and visit [console](http://localhost:8080)

Console features preview reference document
[console preview](../../../design/manager.md).
