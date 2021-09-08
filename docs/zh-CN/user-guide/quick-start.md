# Dragonfly 快速开始

Dragonfly 快速开始文档的目标是帮助您快速开始使用 Dragonfly。

您可以根据 [Kubernetes-with-Dragonfly](../ecosystem/Kubernetes-with-Dragonfly.md) 文档中的内容快速搭建 Dragonfly 的 Kubernetes 集群。

下表列出了一些容器的运行时、版本和文档。

| Runtime | Version | Document | CRI Support | Pull Command |
| --- | --- | --- | --- | --- | 
| Containerd without CRI | All | [Link](./proxy/containerd.md) | No | ctr image pull docker.io/library/alpine |
| Containerd with CRI | v1.1.0+ | [Link](./registry-mirror/cri-containerd.md) | Yes | crictl pull docker.io/library/alpine:latest |
| CRI-O | All | [Link](./registry-mirror/cri-o.md) | Yes | crictl pull docker.io/library/alpine:latest |

当在 kubernetes 集群中使用 Dragonfly 时，我们推荐使用 `Containerd with CRI` 和 `CRI-O`，参考文档是
[Kubernetes-with-Dragonfly](../ecosystem/Kubernetes-with-Dragonfly.md)。

## 相关文档

- [install manager](../user-guide/install/install-manager.md) - 安装 Dragonfly manager
- [install cdn](../user-guide/install/install-cdn.md) - 安装 Dragonfly cdn
- [install scheduler](../user-guide/install/install-scheduler.md) - 安装 Dragonfly scheduler
- [proxy](../user-guide/proxy/containerd.md) - 使用 Dragonfly 作为 docker daemon 的 HTTP 代理
- Container Runtimes
    - [cri-o mirror](../user-guide/registry-mirror/cri-o.md) - 使用 Dragonfly 作为 CRIO daemon 的 Registry Mirror
    - [cri-containerd_mirror](../user-guide/registry-mirror/cri-containerd.md) - 使用 Dragonfly 作为 containerd daemon 的 Registry Mirror