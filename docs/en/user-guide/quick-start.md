# [WIP] Dragonfly Quick Start

Dragonfly Quick Start document aims to help you to quick start Dragonfly journey. This experiment is quite easy and
simplified.

You can have a quick start following [Kubernetes-with-Dragonfly](../ecosystem/Kubernetes-with-Dragonfly.md)

This table describes some container runtimes version and documents.

| Runtime | Version | Document | CRI Support | Pull Command |
| --- | --- | --- | --- | --- | 
| Docker | All | [Link](./proxy/docker.md) | No | docker pull docker.io/library/alpine |
| Containerd without CRI | All | [Link](./proxy/containerd.md) | No | ctr image pull docker.io/library/alpine |
| Containerd with CRI | v1.1.0+ | [Link](registry-mirror/cri-containerd.md) | Yes | crictl pull docker.io/library/alpine:latest |
| CRI-O | All | [Link](./registry-mirror/cri-o.md) | Yes | crictl pull docker.io/library/alpine:latest |

When using Dragonfly in Kubernetes, we recommend to use `Containerd with CRI` and `CRI-O`, deploying document can be
found in [Kubernetes-with-Dragonfly](../ecosystem/Kubernetes-with-Dragonfly.md).