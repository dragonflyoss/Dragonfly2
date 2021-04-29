# Dragonfly

<img src="docs/en/images/logo/dragonfly-linear.svg" alt="logo" width="400"/>

[![GitHub release](https://img.shields.io/github/release/dragonflyoss/Dragonfly2.svg)](https://github.com/dragonflyoss/Dragonfly2/releases)
[![CI](https://github.com/dragonflyoss/Dragonfly2/actions/workflows/ci.yaml/badge.svg)](https://github.com/dragonflyoss/Dragonfly2/actions/workflows/ci.yaml)
[![Coverage](https://codecov.io/gh/dragonflyoss/Dragonfly2/branch/main/graph/badge.svg)](https://codecov.io/gh/dragonflyoss/Dragonfly2)
[![Go Report Card](https://goreportcard.com/badge/github.com/dragonflyoss/Dragonfly2?style=flat-square)](https://goreportcard.com/report/github.com/dragonflyoss/Dragonfly2)
[![Open Source Helpers](https://www.codetriage.com/dragonflyoss/dragonfly2/badges/users.svg)](https://www.codetriage.com/dragonflyoss/dragonfly2)
[![TODOs](https://badgen.net/https/api.tickgit.com/badgen/github.com/dragonflyoss/Dragonfly2/main)](https://www.tickgit.com/browse?repo=github.com/dragonflyoss/Dragonfly2&branch=main)
[![Discussions](https://img.shields.io/badge/discussions-on%20github-blue?style=flat-square)](https://github.com/dragonflyoss/Dragonfly2/discussions)
[![Twitter](https://img.shields.io/twitter/url?style=social&url=https%3A%2F%2Ftwitter.com%2Fdragonfly_oss)](https://twitter.com/dragonfly_oss)
[![GoDoc](https://godoc.org/github.com/dragonflyoss/Dragonfly2?status.svg)](https://godoc.org/github.com/dragonflyoss/Dragonfly2)
[![LICENSE](https://img.shields.io/github/license/dragonflyoss/Dragonfly2.svg?style=flat-square)](https://github.com/dragonflyoss/Dragonfly2/blob/main/LICENSE)

Provide efficient, stable, secure, low-cost file and image distribution services to be the best practice and standard solution in the related Cloud-Native area.

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

<img src="docs/en/images/architecture.png" alt="logo" width="800"/>

**Manager:** Used to manage the dynamic configuration that each module depends on, and provide keepalive and metrics functions.
**Scheduler:** The tracker and scheduler in the P2P network that choose appropriate downloading net-path for each peer.
**CDN:** A CDN server that caches downloaded data from source to avoid downloading same files repeatedly.
**Daemon:** Used for pulling images only. It establishes a proxy between containerd/CRI-O and registry.
**Dfget:** The client of Dragonfly used for downloading files. It's similar to wget.

## Getting Started

- [Introduction][introduction]
- [Installation][installation]
- [Quick start][quickstart]

## Documentation
You can find the Dragonfly documentation [on the website][website].

## Community
Welcome developers to actively participate in community discussions and contribute code to Dragonfly. We will remain concerned about the issues discussed in the community and respond quickly.

- Discussions: [Github Discussion Forum][discussion]
- DingTalk: 23304666

## Contributing
You should check out our [CONTRIBUTING][contributing] and develop the project together.

## Code of Conduct
Please refer to our [Code of Conduct][codeconduct].

[contributing]: CONTRIBUTING.md
[codeconduct]: CODE_OF_CONDUCT.md
[introduction]: https://github.com/dragonflyoss/Dragonfly2
[installation]: https://github.com/dragonflyoss/Dragonfly2
[quickstart]: https://github.com/dragonflyoss/Dragonfly2
[website]: https://d7y.io
[discussion]: https://github.com/dragonflyoss/Dragonfly2/discussions
