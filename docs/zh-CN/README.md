# 蜻蜓文档

Dragonfly Document 是关于 Dragonfly 的介绍。对于一般对 Dragonfly 感兴趣的人，
这个 repo 的 README.md 就足够了。对于最终用户来说
`/docs` 中包含的所有详细信息是最好的指南。
而对于开发者来说，[开发者指南](developer-guide/developer-guide.md)部分的内容才是最需要的。

文档文件组织如下：

* [快速开始](quick-start.md)
* [部署](deployment/README.md)
  * [安装](deployment/installation)
  * [配置](deployment/configuration)
* [故障排查](troubleshooting/README.md)
* [CLI参考](cli-reference/README.md)
  * [dfget](cli-reference/dfget.md)
  * [cdn](cli-reference/cdn.md)
  * [scheduler](cli-reference/scheduler.md)
  * [manager](cli-reference/manager.md)
* [预热](preheat/README.md)
  * [Console](preheat/console.md)
  * [Api](preheat/api.md)
* [运行时集成](runtime-integration/README.md)
  * [containerd](runtime-integration/containerd/README.md)
  * [cri-o](runtime-integration/cri-o.md)
  * [docker](runtime-integration/docker.md)
* [架构设计](design/README.md)
  * [整体架构](design/architecture.md)
  * [manager](design/manager.md)
  * [TODO scheduler](design/scheduler.md)
  * [TODO cdn](design/cdn.md)
  * [TODO dfdaemon](design/dfdaemon.md)
* [开发者指南](developer-guide/developer-guide.md)
* [测试指南](test-guide/test-guide.md)
* [API 参考](api-reference/api-reference.md)
