# Kubernetes with Dragonfly

Now we can deploy all components of Dragonfly in Kubernetes cluster. We deploy scheduler and cdn as `StatefulSets`,
daemon as `DaemonSets`.

Table of contents:

* [Kustomize](#kustomize-support)
* [Helm](#helm-support)
* [TODO Upgrade Guide](#upgrade-guide)

## Kustomize Support

### Prepare Kubernetes Cluster

If there is no available Kubernetes cluster for testing, [minikube](https://minikube.sigs.k8s.io/docs/start/) is
recommended. Just run `minikube start`.

### Build and Apply Kustomize Configuration

```shell
git clone https://github.com/dragonflyoss/Dragonfly2.git
kustomize build Dragonfly2/deploy/kustomize/single-cluster-native/overlays/sample | kubectl apply -f -
```

### Wait Dragonfly Ready

Wait all pods running

```
kubectl -n dragonfly wait --for=condition=ready --all --timeout=10m pod
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

After pulled images, find logs in dfdaemon pod:
```shell
# find pods
kubectl -n dragonfly get pod -l component=dfdaemon
# find logs
pod_name=dfdaemon-xxxxx
kubectl -n dragonfly exec -it ${pod_name} -- grep "peer task done" /var/log/dragonfly/daemon/core.log
```

Example output:
```
{"level":"info","ts":"2021-06-28 06:02:30.924","caller":"peer/peertask_stream_callback.go:77","msg":"stream peer task done, cost: 2838ms","peer":"172.17.0.9-1-ed7a32ae-3f18-4095-9f54-6ccfc248b16e","task":"3c658c488fd0868847fab30976c2a079d8fd63df148fb3b53fd1a418015723d7","component":"streamPeerTask"}
```

## Helm Support

### Clone Chart

```shell
git clone https://github.com/dragonflyoss/Dragonfly2.git
```

### Install

```shell
helm install --namespace dragonfly-system dragonfly Dragonfly2/deploy/charts/dragonfly
```

### Wait Dragonfly Ready

Wait all pods running

```
kubectl -n dragonfly-system wait --for=condition=ready --all --timeout=10m pod
```

### Next Steps

Following [Configure Runtime](#configure-runtime) to configure runtime.
Following [Using Dragonfly](#using-dragonfly) to use Dragonfly.

## Upgrade Guide
