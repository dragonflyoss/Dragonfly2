# Dragonfly 快速开始

文档的目标是帮助您快速开始使用 Dragonfly。

您可以根据 [Kubernetes-with-Dragonfly](./deployment/installation/kubernetes/README.md)
文档中的内容快速搭建 Dragonfly 的 Kubernetes 集群。
我们推荐使用 `Containerd with CRI` 和 `CRI-O` 客户端。

下表列出了一些容器的运行时、版本和文档。

| Runtime | Version | Document | CRI Support | Pull Command |
| --- | --- | --- | --- | --- |
<!-- markdownlint-disable -->
| Containerd<sup>*</sup> | v1.1.0+ | [Link](runtime-integration/containerd/mirror.md) | Yes | crictl pull docker.io/library/alpine:latest |
| Containerd without CRI | < v1.1.0 | [Link](runtime-integration/containerd/proxy.md) | No | ctr image pull docker.io/library/alpine |
| CRI-O | All | [Link](runtime-integration/cri-o.md) | Yes | crictl pull docker.io/library/alpine:latest |
<!-- markdownlint-restore -->

**:推荐使用`containerd`*

## Helm Chart 运行时配置

Dragonfly Helm 支持自动更改 docker 配置。

**支持指定仓库** 定制 values.yaml 文件:

```yaml
containerRuntime:
  docker:
    enable: true
    # -- Inject domains into /etc/hosts to force redirect traffic to dfdaemon.
    # Caution: This feature need dfdaemon to implement SNI Proxy,
    # confirm image tag is greater than v2.0.0.
    # When use certs and inject hosts in docker, no necessary to restart docker daemon.
    injectHosts: true
    registryDomains:
    - "harbor.example.com"
    - "harbor.example.net"
```

此配置允许 docker 通过 Dragonfly 拉取 `harbor.example.com` 和 `harbor.example.net` 域名镜像。
使用上述配置部署 Dragonfly 时，无需重新启动 docker。

优点：

* 支持 dfdaemon 自身平滑升级

> 这种模式下，当删除 dfdaemon pod 的时候，`preStop` 钩子将会清理已经注入到 `/etc/hosts` 下的所有主机信息，所有流量将会走原来的镜像中心。

限制:

* 只支持指定域名。

## 准备 Kubernetes 集群

如果没有可用的 Kubernetes 集群进行测试，推荐使用 [minikube](https://minikube.sigs.k8s.io/docs/start/)。
只需运行`minikube start`。

## 安装 Dragonfly

### 默认配置安装

```shell
helm repo add dragonfly https://dragonflyoss.github.io/helm-charts/
helm install --create-namespace --namespace dragonfly-system dragonfly dragonfly/dragonfly
```

### 等待部署成功

等待所有的服务运行成功。

```shell
kubectl -n dragonfly-system wait --for=condition=ready --all --timeout=10m pod
```

## Manager 控制台

控制台页面会在 `dragonfly-manager.dragonfly-system.svc.cluster.local:8080` 展示。

需要绑定 Ingress 可以参考 [Helm Charts 配置选项](https://artifacthub.io/packages/helm/dragonfly/dragonfly#values),
或者手动自行创建 Ingress。

控制台功能预览参考文档 [console preview](design/manager.md)。

## 使用 Dragonfly

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

```text
{
    "level": "info",
    "ts": "2021-06-28 06:02:30.924",
    "caller": "peer/peertask_stream_callback.go:77",
    "msg": "stream peer task done, cost: 2838ms",
    "peer": "172.17.0.9-1-ed7a32ae-3f18-4095-9f54-6ccfc248b16e",
    "task": "3c658c488fd0868847fab30976c2a079d8fd63df148fb3b53fd1a418015723d7",
    "component": "streamPeerTask"
}
```

## Preheat

为了使用 Dragonfly 的最佳体验, 你可以通过 [预热](preheat/README.md) 提前下拉镜像。
