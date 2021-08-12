# Kubernetes with Dragonfly

Now we can deploy all components of Dragonfly in Kubernetes cluster. We deploy scheduler and cdn as `StatefulSets`,
daemon as `DaemonSets`, manager as `Deployments`.

Table of contents:

* [Helm](#helm-support)
* [Kustomize](#kustomize-support)
* [TODO Upgrade Guide](#upgrade-guide)

## Helm Support

### Prepare Kubernetes Cluster

If there is no available Kubernetes cluster for testing, [minikube](https://minikube.sigs.k8s.io/docs/start/) is
recommended. Just run `minikube start`.

### Install Dragonfly

#### Install with default configuration

```bash
$ helm repo add dragonfly https://dragonflyoss.github.io/helm-charts/
$ helm install --create-namespace --namespace dragonfly-system dragonfly dragonfly/dragonfly
```

#### Install with custom configuration

Create the `values.yaml` configuration file. It is recommended to use external redis and mysql instead of containers.

The example uses external mysql and redis. Refer to the document for [configuration](https://artifacthub.io/packages/helm/dragonfly/dragonfly#todo-configuration).

```yaml
mysql:
  enable: false
  auth:
    host: mysql-host
    username: dragonfly
    password: dragonfly 
    database: manager
  primary:
    service:
      port: 3306

redis:
  enable: false
  host: redis-host
  password: dragonfly
  service:
    port: 6379
```

Install dragonfly with `values.yaml`.

```bash
$ helm repo add dragonfly https://dragonflyoss.github.io/helm-charts/
$ helm install --create-namespace --namespace dragonfly-system dragonfly dragonfly/dragonfly -f values.yaml
```

#### Install with an existing manager

Create the `values.yaml` configuration file. Need to configure the cluster id associated with scheduler and cdn.

The example is to deploy a cluster using the existing manager and redis. Refer to the document for [configuration](https://artifacthub.io/packages/helm/dragonfly/dragonfly#todo-configuration).

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

externalManager:
  enable: true
  host: "dragonfly-manager.dragonfly-system.svc.cluster.local"
  restPort: 8080
  grpcPort: 65003

redis:
  enable: false
  host: redis-host
  password: dragonfly
  service:
    port: 6379

mysql:
  enable: false
```

### Wait Dragonfly Ready

Wait all pods running

```bash
$ kubectl -n dragonfly-system wait --for=condition=ready --all --timeout=10m pod
```

### Configure Runtime

Use Containerd with CRI as example, more runtimes can be found [here](../user-guide/quick-start.md)

> This example is for single registry, multiple registries configuration is [here](../user-guide/registry-mirror/cri-containerd.md)

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

## Kustomize Support

### Prepare Kubernetes Cluster

If there is no available Kubernetes cluster for testing, [minikube](https://minikube.sigs.k8s.io/docs/start/) is
recommended. Just run `minikube start`.

### Build and Apply Kustomize Configuration

```shell
$ git clone https://github.com/dragonflyoss/Dragonfly2.git
$ kustomize build Dragonfly2/deploy/kustomize/single-cluster-native/overlays/sample | kubectl apply -f -
```

### Wait Dragonfly Ready

Wait all pods running

```shell
$ kubectl -n dragonfly wait --for=condition=ready --all --timeout=10m pod
```

### Next Steps

Following [Configure Runtime](#configure-runtime) to configure runtime.

Following [Using Dragonfly](#using-dragonfly) to use Dragonfly.

## Upgrade Guide
