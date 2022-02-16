# 使用 dfdaemon 支持 Containerd

从 v1.1.0 开始，Containerd 支持镜像仓库。

## 快速开始

### 步骤 1: 配置 dfdaemon

下面为镜像仓库的 dfdaemon 配置，在路径 `/etc/dragonfly/dfget.yaml`:

```yaml
proxy:
  security:
    insecure: true
  tcpListen:
    listen: 0.0.0.0
    port: 65001
  registryMirror:
    # multiple registries support, if only mirror single registry, disable this
    dynamic: true
    url: https://index.docker.io
  proxies:
    - regx: blobs/sha256.*
```

运行 dfdaemon

```shell
dfget daemon
```

## 步骤 2: 配置 Containerd

### 选项 1: 单镜像仓库

启动 Containerd 镜像仓库配置 `/etc/containerd/config.toml`:

```toml
# explicitly use v2 config format, if already v2, skip the "version = 2"
version = 2

[plugins."io.containerd.grpc.v1.cri".registry.mirrors."docker.io"]
  endpoint = ["http://127.0.0.1:65001","https://registry-1.docker.io"]
```

配置支持两个镜像仓库地址 `http://127.0.0.1:65001` 以及 `https://registry-1.docker.io`.

在 Containerd 配置文件中启用私有镜像注册中心，配置文件位于 `/etc/containerd/config.toml`:

```toml
# explicitly use v2 config format, if already v2, skip the "version = 2"
version = 2

[plugins."io.containerd.grpc.v1.cri".registry.mirrors."docker.io"]
  endpoint = ["http://127.0.0.1:65001","https://registry-1.docker.io"]

[plugins."io.containerd.grpc.v1.cri".registry.configs."127.0.0.1:65001".auth]
  username = "registry_username"
  password = "registry_password"
```

在本配置文件中，注册中心的 auth 必须基于 `127.0.0.1:65001`。

> 详细 Containerd 配置文档参照: <https://github.com/containerd/containerd/blob/v1.5.2/docs/cri/registry.md#configure-registry-endpoint>
> 镜像仓库配置参照文档: <https://github.com/containerd/containerd/blob/v1.5.2/docs/cri/config.md#registry-configuration>

### 选项 2: 多镜像仓库

1.5.0 以上版本 Containerd 支持多镜像仓库。

#### 1. 开启 Contianerd 配置

启动 Containerd 镜像仓库配置 `/etc/containerd/config.toml`:

```toml
# explicitly use v2 config format, if already v2, skip the "version = 2"
version = 2

[plugins."io.containerd.grpc.v1.cri".registry]
  config_path = "/etc/containerd/certs.d"
```

#### 2. 生成每个仓库的 hosts.toml

##### 选项 1: 手动生成 hosts.toml

路径: `/etc/containerd/certs.d/example.com/hosts.toml`

根据不同的仓库替换 `example.com`。

```toml
server = "https://example.com"

[host."http://127.0.0.1:65001"]
  capabilities = ["pull", "resolve"]
  [host."http://127.0.0.1:65001".header]
    X-Dragonfly-Registry = ["https://example.com"]
```

##### 选项 2: 自动生成 hosts.toml

自动生成 hosts.toml 脚本为 <https://github.com/dragonflyoss/Dragonfly2/blob/main/hack/gen-containerd-hosts.sh>

```shell
bash gen-containerd-hosts.sh example.com
```

> 镜像仓库配置详细文档参照: <https://github.com/containerd/containerd/blob/main/docs/hosts.md#registry-configuration---examples>

## Step 3: 重启 Containerd

```shell
systemctl restart containerd
```

## Step 4: 拉取镜像

使用以下命令拉取镜像:

```shell
crictl pull docker.io/library/busybox
```

## Step 5: 验证 Dragonfly 拉取成功

可以查看日志，判断 busybox 镜像正常拉取。

```shell
grep 'register peer task result' /var/log/dragonfly/daemon/*.log
```

如果正常日志输出如下:

```shell
{
    "level": "info",
    "ts": "2021-02-23 20:03:20.306",
    "caller": "client/client.go:83",
    "msg": "register peer task result:true[200]",
    "peerId": "10.15.233.91-65000-43096-1614081800301788000",
    "errMsg": null
}
```
