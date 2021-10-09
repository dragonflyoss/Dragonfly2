
<a name="paths"></a>
## 路径

<a name="api-v1-cdn-clusters-post"></a>
### Create CDNCluster
```
POST /api/v1/cdn-clusters
```


#### 说明
create by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Body**|**CDNCluster**  <br>*必填*|DNCluster|[types.CreateCDNClusterRequest](#types-createcdnclusterrequest)|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.CDNCluster](#model-cdncluster)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* CDNCluster


<a name="api-v1-cdn-clusters-get"></a>
### Get CDNClusters
```
GET /api/v1/cdn-clusters
```


#### 说明
Get CDNClusters


#### 参数

|类型|名称|说明|类型|默认值|
|---|---|---|---|---|
|**Query**|**page**  <br>*必填*|current page|integer|`0`|
|**Query**|**per_page**  <br>*必填*|return max item count, default 10, max 50|integer|`10`|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|< [model.CDNCluster](#model-cdncluster) > array|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* CDNCluster


<a name="api-v1-cdn-clusters-id-get"></a>
### Get CDNCluster
```
GET /api/v1/cdn-clusters/{id}
```


#### 说明
Get CDNCluster by id


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.CDNCluster](#model-cdncluster)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* CDNCluster


<a name="api-v1-cdn-clusters-id-delete"></a>
### Destroy CDNCluster
```
DELETE /api/v1/cdn-clusters/{id}
```


#### 说明
Destroy by id


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|


#### 响应

|HTTP代码|类型|
|---|---|
|**200**|无内容|
|**400**|无内容|
|**404**|无内容|
|**500**|无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* CDNCluster


<a name="api-v1-cdn-clusters-id-patch"></a>
### Update CDNCluster
```
PATCH /api/v1/cdn-clusters/{id}
```


#### 说明
Update by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|
|**Body**|**CDNCluster**  <br>*必填*|CDNCluster|[types.UpdateCDNClusterRequest](#types-updatecdnclusterrequest)|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.CDNCluster](#model-cdncluster)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* CDNCluster


<a name="api-v1-cdn-clusters-id-cdns-cdn_id-put"></a>
### Add Instance to CDNCluster
```
PUT /api/v1/cdn-clusters/{id}/cdns/{cdn_id}
```


#### 说明
Add CDN to CDNCluster


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**cdn_id**  <br>*必填*|cdn id|string|
|**Path**|**id**  <br>*必填*|id|string|


#### 响应

|HTTP代码|类型|
|---|---|
|**200**|无内容|
|**400**|无内容|
|**404**|无内容|
|**500**|无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* CDNCluster


<a name="api-v1-cdn-clusters-id-scheduler-clusters-scheduler_cluster_id-put"></a>
### Add SchedulerCluster to CDNCluster
```
PUT /api/v1/cdn-clusters/{id}/scheduler-clusters/{scheduler_cluster_id}
```


#### 说明
Add SchedulerCluster to CDNCluster


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|
|**Path**|**scheduler_cluster_id**  <br>*必填*|scheduler cluster id|string|


#### 响应

|HTTP代码|类型|
|---|---|
|**200**|无内容|
|**400**|无内容|
|**404**|无内容|
|**500**|无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* CDNCluster


<a name="api-v1-cdns-post"></a>
### Create CDN
```
POST /api/v1/cdns
```


#### 说明
create by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Body**|**CDN**  <br>*必填*|CDN|[types.CreateCDNRequest](#types-createcdnrequest)|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.CDN](#model-cdn)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* CDN


<a name="api-v1-cdns-get"></a>
### Get CDNs
```
GET /api/v1/cdns
```


#### 说明
Get CDNs


#### 参数

|类型|名称|说明|类型|默认值|
|---|---|---|---|---|
|**Query**|**page**  <br>*必填*|current page|integer|`0`|
|**Query**|**per_page**  <br>*必填*|return max item count, default 10, max 50|integer|`10`|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|< [model.CDN](#model-cdn) > array|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* CDN


<a name="api-v1-cdns-id-get"></a>
### Get CDN
```
GET /api/v1/cdns/{id}
```


#### 说明
Get CDN by id


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.CDN](#model-cdn)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* CDN


<a name="api-v1-cdns-id-delete"></a>
### Destroy CDN
```
DELETE /api/v1/cdns/{id}
```


#### 说明
Destroy by id


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|


#### 响应

|HTTP代码|类型|
|---|---|
|**200**|无内容|
|**400**|无内容|
|**404**|无内容|
|**500**|无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* CDN


<a name="api-v1-cdns-id-patch"></a>
### Update CDN
```
PATCH /api/v1/cdns/{id}
```


#### 说明
Update by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|
|**Body**|**CDN**  <br>*必填*|CDN|[types.UpdateCDNRequest](#types-updatecdnrequest)|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.CDN](#model-cdn)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* CDN


<a name="api-v1-healthy-get"></a>
### Get Health
```
GET /api/v1/healthy
```


#### 说明
Get app health


#### 响应

|HTTP代码|类型|
|---|---|
|**200**|无内容|
|**400**|无内容|
|**404**|无内容|
|**500**|无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Health


<a name="api-v1-oauth-post"></a>
### Create Oauth
```
POST /api/v1/oauth
```


#### 说明
create by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Body**|**Oauth**  <br>*必填*|Oauth|[types.CreateOauthRequest](#types-createoauthrequest)|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.Oauth](#model-oauth)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Oauth


<a name="api-v1-oauth-get"></a>
### Get Oauths
```
GET /api/v1/oauth
```


#### 说明
Get Oauths


#### 参数

|类型|名称|说明|类型|默认值|
|---|---|---|---|---|
|**Query**|**page**  <br>*必填*|current page|integer|`0`|
|**Query**|**per_page**  <br>*必填*|return max item count, default 10, max 50|integer|`10`|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|< [model.Oauth](#model-oauth) > array|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Oauth


<a name="api-v1-oauth-id-get"></a>
### Get Oauth
```
GET /api/v1/oauth/{id}
```


#### 说明
Get Oauth by id


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.Oauth](#model-oauth)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Oauth


<a name="api-v1-oauth-id-delete"></a>
### Destroy Oauth
```
DELETE /api/v1/oauth/{id}
```


#### 说明
Destroy by id


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|


#### 响应

|HTTP代码|类型|
|---|---|
|**200**|无内容|
|**400**|无内容|
|**404**|无内容|
|**500**|无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Oauth


<a name="api-v1-oauth-id-patch"></a>
### Update Oauth
```
PATCH /api/v1/oauth/{id}
```


#### 说明
Update by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|
|**Body**|**Oauth**  <br>*必填*|Oauth|[types.UpdateOauthRequest](#types-updateoauthrequest)|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.Oauth](#model-oauth)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Oauth


<a name="api-v1-permissions-get"></a>
### Get Permissions
```
GET /api/v1/permissions
```


#### 说明
Get Permissions


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|< [rbac.Permission](#rbac-permission) > array|
|**400**||无内容|
|**500**||无内容|


#### 生成

* `application/json`


#### 标签

* Permission


<a name="api-v1-preheats-post"></a>
### Create Preheat
```
POST /api/v1/preheats
```


#### 说明
create by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Body**|**CDN**  <br>*必填*|Preheat|[types.CreatePreheatRequest](#types-createpreheatrequest)|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[types.Preheat](#types-preheat)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Preheat


<a name="api-v1-preheats-id-get"></a>
### Get Preheat
```
GET /api/v1/preheats/{id}
```


#### 说明
Get Preheat by id


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[types.Preheat](#types-preheat)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Preheat


<a name="api-v1-roles-post"></a>
### Create Role
```
POST /api/v1/roles
```


#### 说明
Create Role by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Body**|**Role**  <br>*必填*|Role|[types.CreateRoleRequest](#types-createrolerequest)|


#### 响应

|HTTP代码|类型|
|---|---|
|**200**|无内容|
|**400**|无内容|
|**500**|无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Role


<a name="api-v1-roles-get"></a>
### Get Roles
```
GET /api/v1/roles
```


#### 说明
Get roles


#### 响应

|HTTP代码|类型|
|---|---|
|**200**|无内容|
|**400**|无内容|
|**500**|无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Role


<a name="api-v1-roles-role-get"></a>
### Get Role
```
GET /api/v1/roles/:role
```


#### 说明
Get Role


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**role**  <br>*必填*|role|string|


#### 响应

|HTTP代码|类型|
|---|---|
|**200**|无内容|
|**400**|无内容|
|**500**|无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Role


<a name="api-v1-roles-role-delete"></a>
### Destroy Role
```
DELETE /api/v1/roles/:role
```


#### 说明
Destroy role by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**role**  <br>*必填*|role|string|


#### 响应

|HTTP代码|类型|
|---|---|
|**200**|无内容|
|**400**|无内容|
|**500**|无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Role


<a name="api-v1-roles-role-permissions-post"></a>
### Add Permission For Role
```
POST /api/v1/roles/:role/permissions
```


#### 说明
Add Permission by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**role**  <br>*必填*|role|string|
|**Body**|**Permission**  <br>*必填*|Permission|[types.AddPermissionForRoleRequest](#types-addpermissionforrolerequest)|


#### 响应

|HTTP代码|类型|
|---|---|
|**200**|无内容|
|**400**|无内容|
|**500**|无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Role


<a name="api-v1-roles-role-permissions-delete"></a>
### Update Role
```
DELETE /api/v1/roles/:role/permissions
```


#### 说明
Remove Role Permission by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**role**  <br>*必填*|role|string|
|**Body**|**Permission**  <br>*必填*|Permission|[types.DeletePermissionForRoleRequest](#types-deletepermissionforrolerequest)|


#### 响应

|HTTP代码|类型|
|---|---|
|**200**|无内容|
|**400**|无内容|
|**500**|无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Role


<a name="api-v1-scheduler-clusters-post"></a>
### Create SchedulerCluster
```
POST /api/v1/scheduler-clusters
```


#### 说明
create by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Body**|**SchedulerCluster**  <br>*必填*|SchedulerCluster|[types.CreateSchedulerClusterRequest](#types-createschedulerclusterrequest)|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.SchedulerCluster](#model-schedulercluster)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* SchedulerCluster


<a name="api-v1-scheduler-clusters-get"></a>
### Get SchedulerClusters
```
GET /api/v1/scheduler-clusters
```


#### 说明
Get SchedulerClusters


#### 参数

|类型|名称|说明|类型|默认值|
|---|---|---|---|---|
|**Query**|**page**  <br>*必填*|current page|integer|`0`|
|**Query**|**per_page**  <br>*必填*|return max item count, default 10, max 50|integer|`10`|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|< [model.SchedulerCluster](#model-schedulercluster) > array|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* SchedulerCluster


<a name="api-v1-scheduler-clusters-id-get"></a>
### Get SchedulerCluster
```
GET /api/v1/scheduler-clusters/{id}
```


#### 说明
Get SchedulerCluster by id


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.SchedulerCluster](#model-schedulercluster)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* SchedulerCluster


<a name="api-v1-scheduler-clusters-id-delete"></a>
### Destroy SchedulerCluster
```
DELETE /api/v1/scheduler-clusters/{id}
```


#### 说明
Destroy by id


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|


#### 响应

|HTTP代码|类型|
|---|---|
|**200**|无内容|
|**400**|无内容|
|**404**|无内容|
|**500**|无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* SchedulerCluster


<a name="api-v1-scheduler-clusters-id-patch"></a>
### Update SchedulerCluster
```
PATCH /api/v1/scheduler-clusters/{id}
```


#### 说明
Update by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|
|**Body**|**SchedulerCluster**  <br>*必填*|SchedulerCluster|[types.UpdateSchedulerClusterRequest](#types-updateschedulerclusterrequest)|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.SchedulerCluster](#model-schedulercluster)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* SchedulerCluster


<a name="api-v1-scheduler-clusters-id-schedulers-scheduler_id-put"></a>
### Add Scheduler to schedulerCluster
```
PUT /api/v1/scheduler-clusters/{id}/schedulers/{scheduler_id}
```


#### 说明
Add Scheduler to schedulerCluster


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|
|**Path**|**scheduler_id**  <br>*必填*|scheduler id|string|


#### 响应

|HTTP代码|类型|
|---|---|
|**200**|无内容|
|**400**|无内容|
|**404**|无内容|
|**500**|无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* SchedulerCluster


<a name="api-v1-schedulers-post"></a>
### Create Scheduler
```
POST /api/v1/schedulers
```


#### 说明
create by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Body**|**Scheduler**  <br>*必填*|Scheduler|[types.CreateSchedulerRequest](#types-createschedulerrequest)|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.Scheduler](#model-scheduler)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Scheduler


<a name="api-v1-schedulers-get"></a>
### Get Schedulers
```
GET /api/v1/schedulers
```


#### 说明
Get Schedulers


#### 参数

|类型|名称|说明|类型|默认值|
|---|---|---|---|---|
|**Query**|**page**  <br>*必填*|current page|integer|`0`|
|**Query**|**per_page**  <br>*必填*|return max item count, default 10, max 50|integer|`10`|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|< [model.Scheduler](#model-scheduler) > array|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Scheduler


<a name="api-v1-schedulers-id-get"></a>
### Get Scheduler
```
GET /api/v1/schedulers/{id}
```


#### 说明
Get Scheduler by id


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.Scheduler](#model-scheduler)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Scheduler


<a name="api-v1-schedulers-id-delete"></a>
### Destroy Scheduler
```
DELETE /api/v1/schedulers/{id}
```


#### 说明
Destroy by id


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|


#### 响应

|HTTP代码|类型|
|---|---|
|**200**|无内容|
|**400**|无内容|
|**404**|无内容|
|**500**|无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Scheduler


<a name="api-v1-schedulers-id-patch"></a>
### Update Scheduler
```
PATCH /api/v1/schedulers/{id}
```


#### 说明
Update by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|
|**Body**|**Scheduler**  <br>*必填*|Scheduler|[types.UpdateSchedulerRequest](#types-updateschedulerrequest)|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.Scheduler](#model-scheduler)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Scheduler


<a name="api-v1-security-groups-post"></a>
### Create SecurityGroup
```
POST /api/v1/security-groups
```


#### 说明
create by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Body**|**SecurityGroup**  <br>*必填*|SecurityGroup|[types.CreateSecurityGroupRequest](#types-createsecuritygrouprequest)|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.SecurityGroup](#model-securitygroup)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* SecurityGroup


<a name="api-v1-security-groups-get"></a>
### Get SecurityGroups
```
GET /api/v1/security-groups
```


#### 说明
Get SecurityGroups


#### 参数

|类型|名称|说明|类型|默认值|
|---|---|---|---|---|
|**Query**|**page**  <br>*必填*|current page|integer|`0`|
|**Query**|**per_page**  <br>*必填*|return max item count, default 10, max 50|integer|`10`|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|< [model.SecurityGroup](#model-securitygroup) > array|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* SecurityGroup


<a name="api-v1-security-groups-id-get"></a>
### Get SecurityGroup
```
GET /api/v1/security-groups/{id}
```


#### 说明
Get SecurityGroup by id


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.SecurityGroup](#model-securitygroup)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* SecurityGroup


<a name="api-v1-security-groups-id-patch"></a>
### Update SecurityGroup
```
PATCH /api/v1/security-groups/{id}
```


#### 说明
Update by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|
|**Body**|**SecurityGroup**  <br>*必填*|SecurityGroup|[types.UpdateSecurityGroupRequest](#types-updatesecuritygrouprequest)|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.SecurityGroup](#model-securitygroup)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* SecurityGroup


<a name="api-v1-security-groups-id-cdn-clusters-cdn_cluster_id-put"></a>
### Add CDN to SecurityGroup
```
PUT /api/v1/security-groups/{id}/cdn-clusters/{cdn_cluster_id}
```


#### 说明
Add CDN to SecurityGroup


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**cdn_cluster_id**  <br>*必填*|cdn cluster id|string|
|**Path**|**id**  <br>*必填*|id|string|


#### 响应

|HTTP代码|类型|
|---|---|
|**200**|无内容|
|**400**|无内容|
|**404**|无内容|
|**500**|无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* SecurityGroup


<a name="api-v1-security-groups-id-scheduler-clusters-scheduler_cluster_id-put"></a>
### Add Scheduler to SecurityGroup
```
PUT /api/v1/security-groups/{id}/scheduler-clusters/{scheduler_cluster_id}
```


#### 说明
Add Scheduler to SecurityGroup


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|
|**Path**|**scheduler_cluster_id**  <br>*必填*|scheduler cluster id|string|


#### 响应

|HTTP代码|类型|
|---|---|
|**200**|无内容|
|**400**|无内容|
|**404**|无内容|
|**500**|无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* SecurityGroup


<a name="api-v1-securitygroups-id-delete"></a>
### Destroy SecurityGroup
```
DELETE /api/v1/securityGroups/{id}
```


#### 说明
Destroy by id


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|


#### 响应

|HTTP代码|类型|
|---|---|
|**200**|无内容|
|**400**|无内容|
|**404**|无内容|
|**500**|无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* SecurityGroup


<a name="api-v1-user-signin-name-get"></a>
### Oauth Signin
```
GET /api/v1/user/signin/{name}
```


#### 说明
oauth signin by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**name**  <br>*必填*|name|string|


#### 响应

|HTTP代码|类型|
|---|---|
|**200**|无内容|
|**400**|无内容|
|**404**|无内容|
|**500**|无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* User


<a name="api-v1-user-signin-name-callback-get"></a>
### Oauth Signin Callback
```
GET /api/v1/user/signin/{name}/callback
```


#### 说明
oauth signin callback by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**name**  <br>*必填*|name|string|
|**Query**|**code**  <br>*必填*|code|string|


#### 响应

|HTTP代码|类型|
|---|---|
|**200**|无内容|
|**400**|无内容|
|**404**|无内容|
|**500**|无内容|


#### 标签

* Oauth


<a name="api-v1-user-signup-post"></a>
### SignUp user
```
POST /api/v1/user/signup
```


#### 说明
signup by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Body**|**User**  <br>*必填*|User|[types.SignUpRequest](#types-signuprequest)|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.User](#model-user)|
|**400**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* User


<a name="api-v1-users-id-reset_password-post"></a>
### Reset Password For User
```
POST /api/v1/users/:id/reset_password
```


#### 说明
reset password by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Body**|**User**  <br>*必填*|User|[types.ResetPasswordRequest](#types-resetpasswordrequest)|


#### 响应

|HTTP代码|类型|
|---|---|
|**200**|无内容|
|**400**|无内容|
|**500**|无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* User


<a name="api-v1-users-id-roles-get"></a>
### Get User Roles
```
GET /api/v1/users/:id/roles
```


#### 说明
get roles by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|< string > array|
|**400**||无内容|
|**500**||无内容|


#### 生成

* `application/json`


#### 标签

* User


<a name="api-v1-users-id-roles-role-put"></a>
### Add Role For User
```
PUT /api/v1/users/:id/roles/:role
```


#### 说明
add role to user by uri config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|
|**Path**|**role**  <br>*必填*|role|string|


#### 响应

|HTTP代码|类型|
|---|---|
|**200**|无内容|
|**400**|无内容|
|**500**|无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Users


<a name="api-v1-users-id-roles-role-delete"></a>
### Delete Role For User
```
DELETE /api/v1/users/:id/roles/:role
```


#### 说明
delete role by uri config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|
|**Path**|**role**  <br>*必填*|role|string|


#### 响应

|HTTP代码|类型|
|---|---|
|**200**|无内容|
|**400**|无内容|
|**500**|无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Users



