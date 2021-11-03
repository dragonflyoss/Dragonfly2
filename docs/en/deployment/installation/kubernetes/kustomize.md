# Kustomize Support

## Prepare Kubernetes Cluster

If there is no available Kubernetes cluster for testing, [minikube](https://minikube.sigs.k8s.io/docs/start/) is
recommended. Just run `minikube start`.

## Build and Apply Kustomize Configuration

```shell
git clone https://github.com/dragonflyoss/Dragonfly2.git
kustomize build Dragonfly2/deploy/kustomize/single-cluster-native/overlays/sample | kubectl apply -f -
```

## Wait Dragonfly Ready

Wait all pods running

```shell
kubectl -n dragonfly-system wait --for=condition=ready --all --timeout=10m pod
```

## Manager Console

The console page will be displayed on `dragonfly-manager.dragonfly-system.svc.cluster.local:8080`.

If you need to bind Ingress, you can refer to [configuration options](https://artifacthub.io/packages/helm/dragonfly/dragonfly#values) of Helm Charts, or create it manually.

Console features preview reference document [console preview](../../../design/manager.md).

## Configure Runtime Manually

Use Containerd with CRI as example, more runtimes can be found [here](../../../quick-start.md)

> This example is for single registry, multiple registries configuration is [here](../../../runtime-integration)

For private registry:

```toml
# explicitly use v2 config format, if already v2, skip the "version = 2"
version = 2
[plugins."io.containerd.grpc.v1.cri".registry.mirrors."harbor.example.com"]
endpoint = ["http://127.0.0.1:65001", "https://harbor.example.com"]
```

For docker public registry:

```toml
# explicitly use v2 config format, if already v2, skip the "version = 2"
version = 2
[plugins."io.containerd.grpc.v1.cri".registry.mirrors."docker.io"]
endpoint = ["http://127.0.0.1:65001", "https://registry-1.docker.io"]
```

Add above config to `/etc/containerd/config.toml` and restart Containerd

```shell
systemctl restart containerd
```

## Using Dragonfly

After all above steps, create a new pod with target registry. Or just pull an image with `crictl`:

```shell
crictl harbor.example.com/library/alpine:latest
```

```shell
crictl pull docker.io/library/alpine:latest
```

After pulled images, find logs in dfdaemon pod:
```shell
# find pods
kubectl -n dragonfly-system get pod -l component=dfdaemon
# find logs
pod_name=dfdaemon-xxxxx
kubectl -n dragonfly-system exec -it ${pod_name} -- grep "peer task done" /var/log/dragonfly/daemon/core.log
```

Example output:
```
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
