# Manager Console

The Manager console controls other module services. 

## Relationship

- CDN cluster and Scheduler cluster have a `1:N` relationship
- CDN cluster and CDN instance have a `1:N` relationship
- Scheduler cluster and Scheduler instance have a `1:N` relationship

<div align="center">
  <img src="../../images/manager-console/relationship.jpg" width="500" title="manager-relationship">
</div>

When the Scheduler instance starts, it reports to the manager the Scheduler Cluster ID. Refer to [document](../../config/scheduler.yaml) to configure `schedulerClusterID`.

When the CDN instance starts, it reports to the manager the CDN Cluster ID. Refer to [document](../../config/cdn.yaml) to configure `cdnClusterID`.

## User account

Default root username: `root` password: `dragonfly`.

## Pages

### Sign in

![signin][signin]

### Sign up

![signup][signup]

### Scheduler Cluster

![scheduler-cluster][scheduler-cluster]

### Add Scheduler Cluster

![add-scheduler-cluster][add-scheduler-cluster]

### CDN Cluster

![cdn-cluster][cdn-cluster]

### Add CDN Cluster

![add-cdn-cluster][add-cdn-cluster]

[signin]: ../../images/manager-console/signin.jpg
[signup]: ../../images/manager-console/signup.jpg
[scheduler-cluster]: ../../images/manager-console/scheduler-cluster.jpg
[add-scheduler-cluster]: ../../images/manager-console/add-scheduler-cluster.jpg
[cdn-cluster]: ../../images/manager-console/cdn-cluster.jpg
[add-cdn-cluster]: ../../images/manager-console/add-cdn-cluster.jpg
