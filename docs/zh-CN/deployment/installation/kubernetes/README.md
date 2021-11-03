# Dragonfly 部署 kubernetes 集群

下面会解释如何在 Kubernetes 集群内部署 Dragonfly。Dragonfly 部署具体模块包括 4 部分: scheduler 和 cdn 会作为 `StatefulSets` 部署, dfdaemon 会作为 `DaemonSets` 部署, manager 会作为 `Deployments` 部署。

部署方式:

* [Helm](helm.md)
* [Kustomize](kustomize.md)
* [TODO Upgrade Guide](upgrade-guide.md)
