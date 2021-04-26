# Dragonfly2
## Goal
Provide efficient, stable, secure, low-cost file and image distribution services to be the best practice and standard solution in the related Cloud-Native area.

## Problems Solved
1. Architecture design defects: The existing architecture fails to meet the growing needs of the file and image distribution business, which gradually exposes its deficiencies in stability, efficiency, and security. It also faces a growing number of challenges.
2. Insufficient values penetration: Currently it can only support HTTP protocol, and lacks adaptation for other types of storage (such as HDFS, storage services from various cloud vendors, Maven, Yum, etc.). Thus, it greatly restricts coverage scenarios and further hinders promotion and values dissemination in companies and communities.
3. Single distribution mode: Currently it only supports the active pull mode, and lacks the active push as well as the synchronization skills, which are the basic conditions to satisfy for our product positioning.
4. Lack of productivity: No complete console functions are provided, such as distribution task management and control, data dashboard, multi-tenancy, permission control, and so on.
5. Inconsistent internal and external versions, high maintenance costs, and difficulties to synchronize new features as well as fix problems within a short time.

## Core Features

+ Implement P2P files distribution with various storage types (HDFS, storage services from various cloud vendors, Maven, Yum, etc.) through a unified back-to-source adapter layer.
+ Support more distribution modes: active pull, active push, real-time synchronization, remote replication, automatic warm-up, cross-cloud transmission, etc.
+ Provide separation and decoupling between systems, scheduling and plug-in CDN. Support on-demand deployment with flexible types: light or heavy, inside or outside, to meet the actual needs of different scenarios.
+ Newly designed P2P protocol framework based on GRPC with improved efficiency and stability.
+ Perform encrypted transmission, account-based transmission authentication and rate limit, and multi-tenant isolation mechanism.
+ Bear more efficient IO methods: multi-threaded IO, memory mapping, DMA, etc.
+ Advocate dynamic compression, in-memory file systems, and more efficient scheduling algorithms to improve distribution efficiency.
+ Client allows third-party software to natively integrate Dragonfly’s P2P capabilities through C/S mode.
+ Productivity: Support file uploading, task management of various distribution modes, data visualization, global control, etc.
+ Consistent internal and external versions, shared core features, and individual extensions of non-generic features.
+ Enhanced integration with ecology: Harbor, Nydus (on-demand image download), warehouse services for various cloud vendors, etc.
## Architecture
![](../tech-arch.png)


