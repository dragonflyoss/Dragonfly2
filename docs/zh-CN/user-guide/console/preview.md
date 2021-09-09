# Manager 控制台

Manager 控制台方便用户控制集群各模块服务。

## 关系模型

<div align="center">
  <img src="../../../en/images/manager-console/relationship.jpg" width="500" title="manager-relationship">
</div>

- CDN 集群与 Scheduler 集群为一对多关系
- CDN 集群与 CDN 实例是一对多关系
- Scheduler 集群与 Scheduler 实例是一对多关系

Scheduler 实例信息通过，配置文件启动实例上报指定 Scheduler 集群 ID。参考[文档配置](../../config/scheduler.yaml) `schedulerClusterID`。

CDN 实例信息通过，配置文件启动实例上报指定 CDN 集群 ID。参考[文档配置](../../config/cdn.yaml) `cdnClusterID`。

## 用户账号

服务启动后会默认生成 Root 用户, 账号为 `root`, 密码为 `dragonfly`。

## 功能页面

### 登陆页面

![signin][signin]

### 注册页面

![signup][signup]

### Scheduler 集群

![scheduler-cluster][scheduler-cluster]

### 添加 Scheduler 集群

![add-scheduler-cluster][add-scheduler-cluster]

### CDN 集群

![cdn-cluster][cdn-cluster]

### 添加 CDN 集群

![add-cdn-cluster][add-cdn-cluster]

[signin]: ../../../en/images/manager-console/signin.jpg
[signup]: ../../../en/images/manager-console/signup.jpg
[scheduler-cluster]: ../../../en/images/manager-console/scheduler-cluster.jpg
[add-scheduler-cluster]: ../../../en/images/manager-console/add-scheduler-cluster.jpg
[cdn-cluster]: ../../../en/images/manager-console/cdn-cluster.jpg
[add-cdn-cluster]: ../../../en/images/manager-console/add-cdn-cluster.jpg
