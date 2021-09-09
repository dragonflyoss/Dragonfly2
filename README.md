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

Provide efficient, stable, secure, low-cost file and image distribution services to be the best practice and standard solution in the related Cloud-Native area.

## Introduction

Dragonfly is an open source intelligent P2P based image and file distribution system. Its goal is to tackle all distribution problems in cloud native scenarios. Currently Dragonfly focuses on being:

- Simple: well-defined user-facing API (HTTP), non-invasive to all container engines;
- Efficient: CDN support, P2P based file distribution to save enterprise bandwidth;
- Intelligent: host level speed limit, intelligent flow control due to host detection;
- Secure: block transmission encryption, HTTPS connection support.

Dragonfly is now hosted by the Cloud Native Computing Foundation (CNCF) as an Incubating Level Project. Originally it was born to solve all kinds of distribution at very large scales, such as application distribution, cache distribution, log distribution, image distribution, and so on.

## Features

- Implement P2P files distribution with various storage types (HDFS, storage services from various cloud vendors, Maven, Yum, etc.) through a unified back-to-source adapter layer.
- Support more distribution modes: active pull, active push, real-time synchronization, remote replication, automatic warm-up, cross-cloud transmission, etc.
-  Provide separation and decoupling between systems, scheduling and plug-in CDN. Support on-demand deployment with flexible types: light or heavy, inside or outside, to meet the actual needs of different scenarios.
- Newly designed P2P protocol framework based on GRPC with improved efficiency and stability.
- Perform encrypted transmission, account-based transmission authentication and rate limit, and multi-tenant isolation mechanism.
- Bear more efficient IO methods: multithreaded IO, memory mapping, DMA, etc.
- Advocate dynamic compression, in-memory file systems, and more efficient scheduling algorithms to improve distribution efficiency.
- Client allows third-party software to natively integrate Dragonfly’s P2P capabilities through C/S mode.
- Productivity: Support file uploading, task management of various distribution modes, data visualization, global control, etc.
- Consistent internal and external versions, shared core features, and individual extensions of non-generic features.
- Enhanced integration with ecology: Harbor, Nydus (on-demand image download), warehouse services for various cloud vendors, etc.

## Architecture

![alt][arch]

**Manager:** Used to manage the dynamic configuration that each module depends on, and provide keepalive and metrics functions.

**Scheduler:** The tracker and scheduler in the P2P network that choose appropriate downloading net-path for each peer.

**CDN:** A CDN server that caches downloaded data from source to avoid downloading same files repeatedly.

**Daemon:** It's a daemon of dfget client. It establishes a proxy between containerd/CRI-O and registry.

**Dfget:** The client of Dragonfly used for downloading files. It's similar to wget.

## Getting Started

- [Quick Start][quickstart]
- [Installation][installation]

## Documentation

You can find the full documentation [on the repo][document].

## Community

Welcome developers to actively participate in community discussions and contribute code to Dragonfly. We will remain concerned about the issues discussed in the community and respond quickly.

- Discussions: [Github Discussion Forum][discussion]
- Twitter: [@dragonfly_oss](https://twitter.com/dragonfly_oss)
- DingTalk: 23304666

<div align="center">
  <img src="docs/en/images/community/dingtalk-group.jpeg" width="250" title="dingtalk">
</div>

## Contributing
You should check out our [CONTRIBUTING][contributing] and develop the project together.

## Code of Conduct
Please refer to our [Code of Conduct][codeconduct].

[arch]: docs/en/images/arch.png
[logo-linear]: docs/en/images/logo/dragonfly-linear.svg
[quickstart]: docs/en/user-guide/quick-start.md
[installation]: docs/en/user-guide/install/README.md
[website]: https://d7y.io
[discussion]: https://github.com/dragonflyoss/Dragonfly2/discussions
[contributing]: CONTRIBUTING.md
[codeconduct]: CODE_OF_CONDUCT.md
[document]: docs/README.md
