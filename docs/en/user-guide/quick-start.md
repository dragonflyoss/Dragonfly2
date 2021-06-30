# [WIP] Dragonfly Quick Start

Dragonfly Quick Start document aims to help you to quick start Dragonfly journey. This experiment is quite easy and
simplified.

You can have a quick start following [Kubernetes-with-Dragonfly](../ecosystem/Kubernetes-with-Dragonfly.md)

This table describes some container runtimes version and documents.

| Runtime | Version | Document | CRI Support | Pull Command |
| --- | --- | --- | --- | --- | 
| Containerd without CRI | All | [Link](./proxy/containerd.md) | No | ctr image pull docker.io/library/alpine |
| Containerd with CRI | v1.1.0+ | [Link](./registry-mirror/cri-containerd.md) | Yes | crictl pull docker.io/library/alpine:latest |
| CRI-O | All | [Link](./registry-mirror/cri-o.md) | Yes | crictl pull docker.io/library/alpine:latest |

When using Dragonfly in Kubernetes, we recommend to use `Containerd with CRI` and `CRI-O`, deploying document can be
found in [Kubernetes-with-Dragonfly](../ecosystem/Kubernetes-with-Dragonfly.md).

## SEE ALSO

- [multi machines deployment](../user-guide/multi-machines-deployment.md) - experience Dragonfly on multiple machines
- [install manager](../user-guide/install/install-manager.md) - how to install the Dragonfly manager
- [install cdn](../user-guide/install/install-cdn.md) - how to install the Dragonfly cdn
- [install scheduler](../user-guide/install/install-scheduler.md) - how to install the Dragonfly scheduler
- [install client](../user-guide/install/install-client.md) - how to install the Dragonfly client
- [proxy](../user-guide/proxy/containerd.md) - make Dragonfly as an HTTP proxy for docker daemon
- [download files](../user-guide/download-files.md) - download files with Dragonfly
- Container Runtimes
    - [cri-o mirror](../user-guide/registry-mirror/cri-o.md) - make Dragonfly as Registry Mirror for CRIO daemon
    - [cri-containerd_mirror](../user-guide/registry-mirror/cri-containerd.md) - make Dragonfly as Registry Mirror for containerd daemon