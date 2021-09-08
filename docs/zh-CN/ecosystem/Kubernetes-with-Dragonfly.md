# Dragonfly 部署 kubernetes 集群

下面会解释如何在 Kubernetes 集群内部署 Dragonfly。Dragonfly 部署具体模块包括 4 部分: scheduler 和 cdn 会作为 `StatefulSets` 部署, dfdaemon 会作为 `DaemonSets` 部署, manager 会作为 `Deployments` 部署。

部署方式:

* [Helm](#helm-support)
* [Kustomize](#kustomize-support)
* [TODO Upgrade Guide](#upgrade-guide)

## Helm 部署

### Helm Chart 运行时配置

当使用 Helm Chart 运行时配置时，可以忽略 [运行时配置](#configure-runtime-manually) 章节。因为 Helm Chart 安装时会自动帮助改变 Docker、Containerd 等配置, 无需再手动配置。

#### 1. Docker

Dragonfly Helm 支持自动更改 docker 配置。

**情况 1: 支持指定仓库**

定制 values.yaml 文件:
```yaml
containerRuntime:
  docker:
    enable: true
    # -- Inject domains into /etc/hosts to force redirect traffic to dfdaemon.
    # Caution: This feature need dfdaemon to implement SNI Proxy, confirm image tag is greater than v0.4.0.
    # When use certs and inject hosts in docker, no necessary to restart docker daemon.
    injectHosts: true
    registryDomains:
    - "harbor.example.com"
    - "harbor.example.net"
```

此配置允许 docker 通过 Dragonfly 拉取 `harbor.example.com` 和 `harbor.example.net` 域名镜像。
使用上述配置部署 Dragonfly 时，无需重新启动 docker。

限制:
* 只支持指定域名。

**情况 2: 支持任意仓库**

定制 values.yaml 文件:
```yaml
containerRuntime:
  docker:
    enable: true
    # -- Restart docker daemon to redirect traffic to dfdaemon
    # When containerRuntime.docker.restart=true, containerRuntime.docker.injectHosts and containerRuntime.registry.domains is ignored.
    # If did not want restart docker daemon, keep containerRuntime.docker.restart=false and containerRuntime.docker.injectHosts=true.
    restart: true
```

此配置允许 Dragonfly 拦截所有 docker 流量。
使用上述配置部署 Dragonfly 时，dfdaemon 将重新启动 docker。

限制:
* 必须开启 docker 的 `live-restore` 功能
* 需要重启 docker daemon

#### 2. Containerd

Containerd 的配置有两个版本，字段复杂。有很多情况需要考虑：

**情况 1: V2 版本使用配置文件**

配置文件路径是 `/etc/containerd/config.toml`:

```toml
[plugins."io.containerd.grpc.v1.cri".registry]
  config_path = "/etc/containerd/certs.d"
```

这种情况很简单，并可以启用多个镜像仓库支持。

定制 values.yaml 文件:
```yaml
containerRuntime:
  containerd:
    enable: true
```

**情况 2: V2 版本不使用配置文件**

* 选项 1 - 允许 Charts 注入 `config_path` 并重新启动 containerd。

此选项启用多个镜像仓库支持。

> 警告: 如果 config.toml 中已经有许多其他镜像仓库配置，则不应使用此选项，或使用 `config_path` 迁移您的配置。

定制 values.yaml 文件:

```yaml
containerRuntime:
  containerd:
    enable: true
    injectConfigPath: true
```

* 选项 2 - 只使用一个镜像仓库 `dfdaemon.config.proxy.registryMirror.url`

定制 values.yaml 文件:

```yaml
containerRuntime:
  containerd:
    enable: true
```

**情况 3: V1 版本**

对于 V1 版本 config.toml，仅支持 `dfdaemon.config.proxy.registryMirror.url` 镜像仓库。

定制 values.yaml 文件:

```yaml
containerRuntime:
  containerd:
    enable: true
```

#### 3. [WIP] CRI-O

> 请勿使用，开发中。

Dragonfly helm 自动支持配置 CRI-O。

定制 values.yaml 文件:

```yaml
containerRuntime:
  crio:
    # -- Enable CRI-O support
    # Inject drop-in mirror config into /etc/containers/registries.conf.d.
    enable: true
    # Registries full urls
    registries:
    - "https://ghcr.io"
    - "https://quay.io"
    - "https://harbor.example.com:8443"
```

### 准备 Kubernetes 集群 

如果没有可用的 Kubernetes 集群进行测试，推荐使用 [minikube](https://minikube.sigs.k8s.io/docs/start/)。只需运行`minikube start`。

### 安装 Dragonfly

#### 默认配置安装

```shell
helm repo add dragonfly https://dragonflyoss.github.io/helm-charts/
helm install --create-namespace --namespace dragonfly-system dragonfly dragonfly/dragonfly
```

#### 自定义配置安装

创建 `values.yaml` 配置文件。建议使用外部 redis 和 mysql 代替容器启动。

该示例使用外部 mysql 和 redis。参考[配置文档](https://artifacthub.io/packages/helm/dragonfly/dragonfly#values)。

```yaml
mysql:
  enable: false

externalMysql:
  migrate: true
  host: mysql-host
  username: dragonfly
  password: dragonfly
  database: manager
  port: 3306

redis:
  enable: false

externalRedis:
  host: redis-host
  password: dragonfly
  port: 6379
```

使用配置文件 `values.yaml` 安装 Dragonfly。

```shell
helm repo add dragonfly https://dragonflyoss.github.io/helm-charts/
helm install --create-namespace --namespace dragonfly-system dragonfly dragonfly/dragonfly -f values.yaml
```

#### 安装 Drgonfly 使用已经部署的 manager

创建 `values.yaml` 配置文件。需要配置 scheduler 和 cdn 关联的对应集群的 id。

示例是使用外部的 manager 和 redis。参考[配置文档](https://artifacthub.io/packages/helm/dragonfly/dragonfly#values)。

```yaml
scheduler:
  config:
    manager:
      schedulerClusterID: 1

cdn:
  config:
    base:
      manager:
        cdnClusterID: 1

manager:
  enable: false

externalManager:
  enable: true
  host: "dragonfly-manager.dragonfly-system.svc.cluster.local"
  restPort: 8080
  grpcPort: 65003

redis:
  enable: false

externalRedis:
  host: redis-host
  password: dragonfly
  port: 6379

mysql:
  enable: false
```

### 等待部署成功

等待所有的服务运行成功。

```shell
kubectl -n dragonfly-system wait --for=condition=ready --all --timeout=10m pod
```

### 运行时配置

以 Containerd 和 CRI 为例，更多运行时[文档](../user-guide/quick-start.md)

> 例子为单镜像仓库配置，多镜像仓库配置参考[文档](../user-guide/registry-mirror/cri-containerd.md)

私有仓库:

```toml
# explicitly use v2 config format, if already v2, skip the "version = 2"
version = 2
[plugins."io.containerd.grpc.v1.cri".registry.mirrors."harbor.example.com"]
endpoint = ["http://127.0.0.1:65001", "https://harbor.example.com"]
```

dockerhub 官方仓库:

```toml
# explicitly use v2 config format, if already v2, skip the "version = 2"
version = 2
[plugins."io.containerd.grpc.v1.cri".registry.mirrors."docker.io"]
endpoint = ["http://127.0.0.1:65001", "https://registry-1.docker.io"]
```

增加配置到 `/etc/containerd/config.toml` 文件并重启 Containerd。

```shell
systemctl restart containerd
```

### 使用 Dragonfly

以上步骤执行完毕，可以使用 `crictl` 命令拉取镜像:

```shell
crictl harbor.example.com/library/alpine:latest
```

```shell
crictl pull docker.io/library/alpine:latest
```

拉取镜像后可以在 dfdaemon 查询日志:

```shell
# find pods
kubectl -n dragonfly-system get pod -l component=dfdaemon
# find logs
pod_name=dfdaemon-xxxxx
kubectl -n dragonfly-system exec -it ${pod_name} -- grep "peer task done" /var/log/dragonfly/daemon/core.log
```

日志输出例子:

```
{"level":"info","ts":"2021-06-28 06:02:30.924","caller":"peer/peertask_stream_callback.go:77","msg":"stream peer task done, cost: 2838ms","peer":"172.17.0.9-1-ed7a32ae-3f18-4095-9f54-6ccfc248b16e","task":"3c658c488fd0868847fab30976c2a079d8fd63df148fb3b53fd1a418015723d7","component":"streamPeerTask"}
```

## Kustomize 支持

### 准备 Kubernetes 集群 

如果没有可用的 Kubernetes 集群进行测试，推荐使用 [minikube](https://minikube.sigs.k8s.io/docs/start/)。只需运行`minikube start`。

### 构建 Kustomize 模版并部署

```shell
git clone https://github.com/dragonflyoss/Dragonfly2.git
kustomize build Dragonfly2/deploy/kustomize/single-cluster-native/overlays/sample | kubectl apply -f -
```

### 等待部署成功

等待所有的服务运行成功。

```shell
kubectl -n dragonfly-system wait --for=condition=ready --all --timeout=10m pod
```

### 下一步

按照[文档](#configure-runtime-manually)来配置运行时。

按照[文档](#configure-runtime-manually)来使用 Dragonfly。

## 升级指南
