# Dragonfly

![alt][logo-linear]

[![GitHub release](https://img.shields.io/github/release/dragonflyoss/Dragonfly2.svg)](https://github.com/dragonflyoss/Dragonfly2/releases)
[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/dragonfly)](https://artifacthub.io/packages/search?repo=dragonfly)
[![CI](https://github.com/dragonflyoss/Dragonfly2/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/dragonflyoss/Dragonfly2/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/dragonflyoss/Dragonfly2/branch/main/graph/badge.svg)](https://codecov.io/gh/dragonflyoss/Dragonfly2)
[![Go Report Card](https://goreportcard.com/badge/github.com/dragonflyoss/Dragonfly2?style=flat-square)](https://goreportcard.com/report/github.com/dragonflyoss/Dragonfly2)
[![Open Source Helpers](https://www.codetriage.com/dragonflyoss/dragonfly2/badges/users.svg)](https://www.codetriage.com/dragonflyoss/dragonfly2)
[![TODOs](https://badgen.net/https/api.tickgit.com/badgen/github.com/dragonflyoss/Dragonfly2/main)](https://www.tickgit.com/browse?repo=github.com/dragonflyoss/Dragonfly2&branch=main)
[![Discussions](https://img.shields.io/badge/discussions-on%20github-blue?style=flat-square)](https://github.com/dragonflyoss/Dragonfly2/discussions)
[![Twitter](https://img.shields.io/twitter/url?style=social&url=https%3A%2F%2Ftwitter.com%2Fdragonfly_oss)](https://twitter.com/dragonfly_oss)
[![GoDoc](https://godoc.org/github.com/dragonflyoss/Dragonfly2?status.svg)](https://godoc.org/github.com/dragonflyoss/Dragonfly2)
[![LICENSE](https://img.shields.io/github/license/dragonflyoss/Dragonfly2.svg?style=flat-square)](https://github.com/dragonflyoss/Dragonfly2/blob/main/LICENSE)

Provide efficient, stable, secure, low-cost file and
image distribution services to be the best practice and
standard solution in cloud native architectures.

## Introduction

Dragonfly is an open source P2P-based file and
image distribution system. It is hosted by the
Cloud Native Computing Foundation ([CNCF](https://cncf.io/)) as
an Incubating Level Project.
Its goal is to tackle all distribution problems in cloud native architectures.
Currently Dragonfly focuses on being:

- **Simple**: Well-defined user-facing API (HTTP), non-invasive to all container engines;
- **Efficient**: Seed peer support, P2P based file distribution to save enterprise bandwidth;
- **Intelligent**: Host-level speed limit, intelligent flow control due to host detection;
- **Secure**: Block transmission encryption, HTTPS connection support.

## Architecture

![alt][arch]

**Manager:** Used to manage the dynamic configuration that
each module depends on, and provide keepalive and metrics features.
It can be deployed using manager module.

**Scheduler:** The tracker and scheduler in the P2P
network that chooses appropriate downloading net-path for each peer.
It can be deployed using scheduler module.

**Seed Peer:** A seed peer server that caches downloaded data
from source. It can be deployed using dfdaemon or cdn module and
is recommended to provide quality networking and storage.

**Peer:** A peer server can download resources from other peers,
and can also share resources with other peers. It can be deployed using dfdaemon module.
Dfdaemon provides a proxy to allow client traffic such as containerd/CRI-O to use the P2P network,
and also provides dfget download command tool, which is similar to wget.

## Documentation

You can find the full documentation on the [d7y.io][d7y.io].

## Community

Welcome developers to actively participate in community discussions
and contribute code to Dragonfly. We will remain
concerned about the issues discussed in the community and respond quickly.

- **Slack Channel**: [#dragonfly](https://cloud-native.slack.com/messages/dragonfly/) on [CNCF Slack](https://slack.cncf.io/)
- **Discussion Group**: <dragonfly-discuss@googlegroups.com>
- **Developer Group**: <dragonfly-developers@googlegroups.com>
- **Github Discussions**: [Dragonfly Discussion Forum][discussion]
- **Twitter**: [@dragonfly_oss](https://twitter.com/dragonfly_oss)
- **DingTalk**: [23304666](https://h5.dingtalk.com/circle/healthCheckin.html?dtaction=os&corpId=ding0ba5f94d8290b9f7f235fbadcd45de0c&f4462ef5-a7d=9bec3e94-b34&cbdbhh=qwertyuiop)

<!-- markdownlint-disable -->
<div align="center">
  <img src="docs/images/community/dingtalk-group.jpeg" width="300" title="dingtalk">
</div>
<!-- markdownlint-restore -->

## Contributing

You should check out our
[CONTRIBUTING][contributing] and develop the project together.

## Code of Conduct

Please refer to our [Code of Conduct][codeconduct].

[arch]: docs/images/arch.png
[logo-linear]: docs/images/logo/dragonfly-linear.svg
[website]: https://d7y.io
[discussion]: https://github.com/dragonflyoss/Dragonfly2/discussions
[contributing]: CONTRIBUTING.md
[codeconduct]: CODE_OF_CONDUCT.md
[d7y.io]: https://d7y.io/
[dingtalk]: docs/images/community/dingtalk-group.jpeg
