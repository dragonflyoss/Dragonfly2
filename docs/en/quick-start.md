# Dragonfly Quick Start

Dragonfly Quick Start document aims to help you to
quick start Dragonfly journey. This experiment is quite easy and
simplified.

You can have a quick start following
[Kubernetes-with-Dragonfly](./deployment/installation/kubernetes/README.md).
We recommend to use `Containerd with CRI` and `CRI-O` client.

This table describes some container runtimes version and documents.

<!-- markdownlint-disable -->
| Runtime | Version | Document | CRI Support | Pull Command |
| --- | --- | --- | --- | --- | 
| Containerd<sup>*</sup> | v1.1.0+ | [Link](runtime-integration/containerd/mirror.md) | Yes | crictl pull docker.io/library/alpine:latest |
| Containerd without CRI | < v1.1.0 | [Link](runtime-integration/containerd/proxy.md) | No | ctr image pull docker.io/library/alpine |
| CRI-O | All | [Link](runtime-integration/cri-o.md) | Yes | crictl pull docker.io/library/alpine:latest |
<!-- markdownlint-restore -->

**:`containerd` is recommended*

## Runtime Configuration Guide for Dragonfly Helm Chart

Dragonfly helm supports config docker automatically.

Config cases:

### Implicit registries support

Chart customize values.yaml:

```yaml
containerRuntime:
  docker:
    enable: true
    # -- Inject domains into /etc/hosts to force redirect traffic to dfdaemon.
    # Caution: This feature need dfdaemon to implement SNI Proxy,
    # confirm image tag is greater than v0.4.0.
    # When use certs and inject hosts in docker, no necessary to restart docker daemon.
    injectHosts: true
    registryDomains:
    - "harbor.example.com"
    - "harbor.example.net"
```

This config enables docker pulling images from registries
`harbor.example.com` and `harbor.example.net` via Dragonfly.
When deploying Dragonfly with above config, it's unnecessary to restart docker daemon.

Limitations:

* Only support implicit registries

## Prepare Kubernetes Cluster

If there is no available Kubernetes cluster for testing,
[minikube](https://minikube.sigs.k8s.io/docs/start/) is
recommended. Just run `minikube start`.

## Install Dragonfly

### Install with default configuration

```shell
helm repo add dragonfly https://dragonflyoss.github.io/helm-charts/
helm install --create-namespace --namespace dragonfly-system dragonfly dragonfly/dragonfly
```

## Wait Dragonfly Ready

Wait all pods running

```shell
kubectl -n dragonfly-system wait --for=condition=ready --all --timeout=10m pod
```

## Manager Console

The console page will be displayed on
`dragonfly-manager.dragonfly-system.svc.cluster.local:8080`.

If you need to bind Ingress, you can refer to
[configuration options](https://artifacthub.io/packages/helm/dragonfly/dragonfly#values)
of Helm Charts, or create it manually.

Console features preview reference document [console preview](design/manager.md).

## Using Dragonfly

After all above steps, create a new pod with target registry.
Or just pull an image with `crictl`:

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

```shell
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

To get the best out of Dragonfly, you can pull the image in advance by [preheat](preheat/README.md)
