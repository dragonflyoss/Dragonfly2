# 使用 dfdaemon 支持 CRI-O

## Step 1: 配置 Dragonfly

下面为镜像仓库的 dfdaemon 配置，在路径 `/etc/dragonfly/dfget.yaml`:

```yaml
proxy:
  security:
    insecure: true
  tcpListen:
    listen: 0.0.0.0
    port: 65001
  registryMirror:
    url: https://index.docker.io
  proxies:
    - regx: blobs/sha256.*
```

上面配置会拦截所有 `https://index.docker.io` 的镜像。

## Step 2: 配置 CRI-O

启动 CRI-O 镜像仓库配置 `/etc/containers/registries.conf`:

```toml
[[registry]]
location = "docker.io"
  [[registry.mirror]]
  location = "127.0.0.1:65001"
  insecure = true
```

## Step 3: 重启 CRI-O

```shell
systemctl restart crio
```

如果遇到如下错误 `mixing sysregistry v1/v2 is not supported` 或
`registry must be in v2 format but is in v1`, 请将您的镜像仓库配置为 v2。

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
{"level":"info","ts":"2021-02-23 20:03:20.306","caller":"client/client.go:83",
"msg":"register peer task result:true[200] for taskId:adf62a86f001e17037eedeaaba3393f3519b80ce,
peerIp:10.15.233.91,securityDomain:,idc:,scheduler:127.0.0.1:8002",
"peerId":"10.15.233.91-65000-43096-1614081800301788000","errMsg":null}
```
