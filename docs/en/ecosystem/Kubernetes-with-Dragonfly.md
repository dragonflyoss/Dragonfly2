# Kubernetes with Dragonfly

Now we can deploy all components of Dragonfly in Kubernetes cluster. We deploy scheduler and cdn as `StatefulSets`,
daemon as `DaemonSets`.

Table of contents:

* [Kustomize](#kustomize-support)
* [TODO Helm](#helm-support)
* [TODO Upgrade Guide](#upgrade-guide)

## Kustomize Support

### Prepare Kubernetes Cluster

If there is no available Kubernetes cluster for testing, [minikube](https://minikube.sigs.k8s.io/docs/start/) is
recommended. Just run `minikube start`.

Requirements:

1. Host network can connect with other pods
2. Host network can resolve service in Kubernetes cluster

If your Kubernetes cluster is not ready for those, please open an issue.

### Build and Apply Kustomize Configuration

```shell
git clone https://github.com/dragonflyoss/Dragonfly2.git
kustomize build Dragonfly2/deploy/kustomize/single-cluster-native | kubectl apply -f -
```

### Wait Dragonfly Ready

Wait all pods running

```
kubectl -n dragonfly get pod -w
```

### Configure Runtime

Use Containerd with CRI as example, more runtimes can be found [here](../user-guide/quick-start.md)

For private registry:

```toml
[plugins."io.containerd.grpc.v1.cri".registry.mirrors."harbor.example.com"]
endpoint = ["http://127.0.0.1:65001", "https://harbor.example.com"]
```

For docker public registry:

```toml
[plugins."io.containerd.grpc.v1.cri".registry.mirrors."docker.io"]
endpoint = ["http://127.0.0.1:65001", "https://registry-1.docker.io"]
```

Add above config to `/etc/containerd/config.toml` and restart Containerd

```shell
systemctl restart containerd
```

### Using Dragonfly

After all above steps, create a new pod with target registry. Or just pull an image with `crictl`:

```shell
crictl harbor.example.com/library/alpine:latest
```

```shell
crictl pull docker.io/library/alpine:latest
```