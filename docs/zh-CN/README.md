# 蜻蜓文档

Dragonfly Document 是关于 Dragonfly 的介绍。对于一般对 Dragonfly 感兴趣的人，这个 repo 的 README.md 就足够了。对于最终用户来说，`/docs` 中包含的所有详细信息是最好的指南。而对于开发者来说，[开发者指南](#开发者指南)部分的内容才是最需要的。

文档文件组织如下：

* [快速开始](#快速开始)
* [用户指南](#用户指南)
* [CLI参考](#CLI参考)
    * [WIP dfget](cli-reference/dfget.md)
    * [WIP cdnsystem](cli-reference/cdn.md)
    * [WIP 调度器](cli-reference/scheduler.md)
    * [TODO manager](cli-reference/manager.md)
* [TODO API 参考](#API参考)
* [生态](#生态)
    * [Kubernetes 集成](ecosystem/Kubernetes-with-Dragonfly.md)
    * [WIP Harbor 集成](ecosystem/Harbor-with-Dragonfly.md)
* [开发者指南](#开发者指南)
    * [设计文档](#设计文档)
    * [测试指南](#测试指南)

## 如何贡献文档

在此页面中找到 `WIP` 或 `TODO` 并参考 [CONTRIBUTING](../../CONTRIBUTING.md)。

## 快速开始

[快速开始](user-guide/quick-start.md) 如果您想尝试 Dragonfly，这正是您所需要的。本文件包括您需要做什么准备工作，
如何安装 Dragonfly 以及如何体验 Dragonfly 的使用。

## 用户指南

[用户指南](user-guide/README.md) 帮助用户体验 Dragonfly 所需的各种指南。不仅仅是非常简短的 [快速开始]
(user-guide/quick-start.md)，而且包括详细的安装和配置说明。此外，任何有助于用户更好地理解 Dragonfly 的概念也将包括在内。

## CLI参考

对于几乎所有用户，命令行是您可能需要的第一个参考。 [CLI Reference](cli-reference)目录下的文档是关于Dragonfly CLI的命令详细使用，包括`dfget`、`cdnsystem`、`scheduler`和`manager`。您可以获得有关命令的介绍、概要、示例和选项。最后但同样重要的是，Dragonfly 可以保证命令行文档与 Dragonfly CLI 的源代码高度一致。更重要的是，所有命令行文档都是通过源代码自动生成的。

## [TODO] API参考

命令行是体验 Dragonfly 能力的最简单方法。 API参考将为 Dragonfly 带来更多更进一步的体验。 Commandline只是API的一种组合用法，如果你想破解或利用Dragonfly的更多优势，请参见[API参考](./api-reference)。与命令行文档一样，所有 API 文档都是通过源代码自动生成的。

## 生态

生态文档显示了 Dragonfly 与云原生生态系统中流行的工具或系统之间的联系。他们指导最终用户如何体验云
使用 Dragonfly 的原生系统，例如其他 CNCF 项目 [Kubernetes](ecosystem/Kubernetes-with-Dragonfly.md) 和 [Harbor](ecosystem/Harbor-with-Dragonfly.md)。

## 开发者指南

[开发者指南](development/local.md) 帮助（潜在的）开发者/贡献者理解 Dragonfly 内部的理论而不是它公开的接口。和
更好地理解 Dragonfly 的设计方式，开发人员可以更轻松地学习 Dragonfly 的源代码，并知道如何调试、测试和破解。

### [TODO] 设计文档

[Design Doc](./design/design.md) 是关于 Dragonfly 设计的内容。它包括一开始就考虑到的所有事情，为 Dragonfly 中所有组件设计的架构，组件之间的交互工作流，Dragonfly 中的所有 API 以及其他一些技术内容。

### [TODO] 测试指南

[测试指南](./test-guide/test-guide.md) 是帮助贡献者了解如何设置测试环境并进行操作的最佳参考。目前我们可以将 Dragonfly 的测试分为四个维度：

* 单元测试;
* API集成测试；
* CLI集成测试；
* 节点 e2e 测试。

更多详情请参考[测试](./test-guide)。

## 写在后面

文件夹`/docs` 没有完全包含有关蜻蜓的所有文档，在此 repo 的其他路径中还有其他非常有用的文档，例如：

* [TODO FAQ.md](./FAQ.md)
* [CHANGELOG.md](../../CHANGELOG.md)
* [TODO ROADMAP.md](./ROADMAP.md)
* 其他。

如果您正在搜索某个文档但没有找到，请不要犹豫 [提交问题](https://github.com/dragonflyoss/Dragonfly2/issues/new/choose) 寻求帮助。另外，如果您发现文档中有一些不正确的地方或错别字，请发起 Pull Request 以更正。