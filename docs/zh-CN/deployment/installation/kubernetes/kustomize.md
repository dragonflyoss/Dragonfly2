# Kustomize 支持

## 准备 Kubernetes 集群 

如果没有可用的 Kubernetes 集群进行测试，推荐使用 [minikube](https://minikube.sigs.k8s.io/docs/start/)。只需运行`minikube start`。

## 构建 Kustomize 模版并部署

```shell
git clone https://github.com/dragonflyoss/Dragonfly2.git
kustomize build Dragonfly2/deploy/kustomize/single-cluster-native/overlays/sample | kubectl apply -f -
```

## 等待部署成功

等待所有的服务运行成功。

```shell
kubectl -n dragonfly-system wait --for=condition=ready --all --timeout=10m pod
```

## Manager 控制台

控制台页面会在 `dragonfly-manager.dragonfly-system.svc.cluster.local:8080` 展示。

需要绑定 Ingress 可以参考 [Helm Charts 配置选项](https://artifacthub.io/packages/helm/dragonfly/dragonfly#values), 或者手动自行创建 Ingress。

控制台功能预览参考文档 [console preview](../../../design/manager.md)。

## 运行时配置

以 Containerd 和 CRI 为例，更多运行时[文档](../../../quick-start.md)

> 例子为单镜像仓库配置，多镜像仓库配置参考[文档](../../../runtime-integration)

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

```
{"level":"info","ts":"2021-06-28 06:02:30.924","caller":"peer/peertask_stream_callback.go:77","msg":"stream peer task done, cost: 2838ms","peer":"172.17.0.9-1-ed7a32ae-3f18-4095-9f54-6ccfc248b16e","task":"3c658c488fd0868847fab30976c2a079d8fd63df148fb3b53fd1a418015723d7","component":"streamPeerTask"}
```
