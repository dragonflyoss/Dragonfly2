# Dragonfly Manager


<a name="overview"></a>
## 概览
Dragonfly Manager Server


### 版本信息
*版本* : 1.0.0


### 许可信息
*许可证* : Apache 2.0  
*服务条款* : null


### URI scheme
*域名* : localhost:8080  
*基础路径* : /api/v1




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


<a name="api-v1-configs-post"></a>
### Create Config
```
POST /api/v1/configs
```


#### 说明
create by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Body**|**Config**  <br>*必填*|Config|[types.CreateConfigRequest](#types-createconfigrequest)|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.Config](#model-config)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Config


<a name="api-v1-configs-get"></a>
### Get Configs
```
GET /api/v1/configs
```


#### 说明
Get Configs


#### 参数

|类型|名称|说明|类型|默认值|
|---|---|---|---|---|
|**Query**|**page**  <br>*必填*|current page|integer|`0`|
|**Query**|**per_page**  <br>*必填*|return max item count, default 10, max 50|integer|`10`|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|< [model.Config](#model-config) > array|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Config


<a name="api-v1-configs-id-get"></a>
### Get Config
```
GET /api/v1/configs/{id}
```


#### 说明
Get Config by id


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.Config](#model-config)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Config


<a name="api-v1-configs-id-delete"></a>
### Destroy Config
```
DELETE /api/v1/configs/{id}
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

* Config


<a name="api-v1-configs-id-patch"></a>
### Update Config
```
PATCH /api/v1/configs/{id}
```


#### 说明
Update by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|
|**Body**|**Config**  <br>*必填*|Config|[types.UpdateConfigRequest](#types-updateconfigrequest)|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.Config](#model-config)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Config


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


<a name="api-v1-jobs-post"></a>
### Create Job
```
POST /api/v1/jobs
```


#### 说明
create by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Body**|**Job**  <br>*必填*|Job|[types.CreateJobRequest](#types-createjobrequest)|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.Job](#model-job)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Job


<a name="api-v1-jobs-get"></a>
### Get Jobs
```
GET /api/v1/jobs
```


#### 说明
Get Jobs


#### 参数

|类型|名称|说明|类型|默认值|
|---|---|---|---|---|
|**Query**|**page**  <br>*必填*|current page|integer|`0`|
|**Query**|**per_page**  <br>*必填*|return max item count, default 10, max 50|integer|`10`|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|< [model.Job](#model-job) > array|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Job


<a name="api-v1-jobs-id-get"></a>
### Get Job
```
GET /api/v1/jobs/{id}
```


#### 说明
Get Job by id


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.Job](#model-job)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Job


<a name="api-v1-jobs-id-delete"></a>
### Destroy Job
```
DELETE /api/v1/jobs/{id}
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

* Job


<a name="api-v1-jobs-id-patch"></a>
### Update Job
```
PATCH /api/v1/jobs/{id}
```


#### 说明
Update by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|
|**Body**|**Job**  <br>*必填*|Job|[types.UpdateJobRequest](#types-updatejobrequest)|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.Job](#model-job)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* Job


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
### Create V1 Preheat
```
POST /api/v1/preheats
```


#### 说明
create by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Body**|**CDN**  <br>*必填*|Preheat|[types.CreateV1PreheatRequest](#types-createv1preheatrequest)|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[types.CreateV1PreheatResponse](#types-createv1preheatresponse)|
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
### Get V1 Preheat
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
|**200**|OK|[types.GetV1PreheatResponse](#types-getv1preheatresponse)|
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


<a name="api-v1-security-groups-id-security-rules-security_rule_id-put"></a>
### Add SecurityRule to SecurityGroup
```
PUT /api/v1/security-groups/{id}/security-rules/{security_rule_id}
```


#### 说明
Add SecurityRule to SecurityGroup


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|
|**Path**|**security_rule_id**  <br>*必填*|security rule id|string|


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


<a name="api-v1-security-groups-id-security-rules-security_rule_id-delete"></a>
### Destroy SecurityRule to SecurityGroup
```
DELETE /api/v1/security-groups/{id}/security-rules/{security_rule_id}
```


#### 说明
Destroy SecurityRule to SecurityGroup


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|
|**Path**|**security_rule_id**  <br>*必填*|security rule id|string|


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


<a name="api-v1-security-rules-post"></a>
### Create SecurityRule
```
POST /api/v1/security-rules
```


#### 说明
create by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Body**|**SecurityRule**  <br>*必填*|SecurityRule|[types.CreateSecurityRuleRequest](#types-createsecurityrulerequest)|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.SecurityRule](#model-securityrule)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* SecurityRule


<a name="api-v1-security-rules-get"></a>
### Get SecurityRules
```
GET /api/v1/security-rules
```


#### 说明
Get SecurityRules


#### 参数

|类型|名称|说明|类型|默认值|
|---|---|---|---|---|
|**Query**|**page**  <br>*必填*|current page|integer|`0`|
|**Query**|**per_page**  <br>*必填*|return max item count, default 10, max 50|integer|`10`|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|< [model.SecurityRule](#model-securityrule) > array|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* SecurityRule


<a name="api-v1-security-rules-id-get"></a>
### Get SecurityRule
```
GET /api/v1/security-rules/{id}
```


#### 说明
Get SecurityRule by id


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.SecurityRule](#model-securityrule)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* SecurityRule


<a name="api-v1-security-rules-id-patch"></a>
### Update SecurityRule
```
PATCH /api/v1/security-rules/{id}
```


#### 说明
Update by json config


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|
|**Body**|**SecurityRule**  <br>*必填*|SecurityRule|[types.UpdateSecurityRuleRequest](#types-updatesecurityrulerequest)|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.SecurityRule](#model-securityrule)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* SecurityRule


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


<a name="api-v1-securityrules-id-delete"></a>
### Destroy SecurityRule
```
DELETE /api/v1/securityRules/{id}
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

* SecurityRule


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


<a name="api-v1-users-get"></a>
### Get Users
```
GET /api/v1/users
```


#### 说明
Get Users


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


<a name="api-v1-users-id-get"></a>
### Get User
```
GET /api/v1/users/{id}
```


#### 说明
Get User by id


#### 参数

|类型|名称|说明|类型|
|---|---|---|---|
|**Path**|**id**  <br>*必填*|id|string|


#### 响应

|HTTP代码|说明|类型|
|---|---|---|
|**200**|OK|[model.User](#model-user)|
|**400**||无内容|
|**404**||无内容|
|**500**||无内容|


#### 消耗

* `application/json`


#### 生成

* `application/json`


#### 标签

* User




<a name="definitions"></a>
## 定义

<a name="model-cdn"></a>
### model.CDN

|名称|类型|
|---|---|
|**cdnclusterID**  <br>*可选*|integer|
|**created_at**  <br>*可选*|string|
|**download_port**  <br>*可选*|integer|
|**host_name**  <br>*可选*|string|
|**id**  <br>*可选*|integer|
|**idc**  <br>*可选*|string|
|**ip**  <br>*可选*|string|
|**location**  <br>*可选*|string|
|**port**  <br>*可选*|integer|
|**status**  <br>*可选*|string|
|**updated_at**  <br>*可选*|string|


<a name="model-cdncluster"></a>
### model.CDNCluster

|名称|类型|
|---|---|
|**bio**  <br>*可选*|string|
|**config**  <br>*可选*|[model.JSONMap](#model-jsonmap)|
|**created_at**  <br>*可选*|string|
|**id**  <br>*可选*|integer|
|**is_default**  <br>*可选*|boolean|
|**jobs**  <br>*可选*|< [model.Job](#model-job) > array|
|**name**  <br>*可选*|string|
|**scheduler_clusters**  <br>*可选*|< [model.SchedulerCluster](#model-schedulercluster) > array|
|**security_group_id**  <br>*可选*|integer|
|**updated_at**  <br>*可选*|string|


<a name="model-config"></a>
### model.Config

|名称|类型|
|---|---|
|**bio**  <br>*可选*|string|
|**created_at**  <br>*可选*|string|
|**id**  <br>*可选*|integer|
|**name**  <br>*可选*|string|
|**updated_at**  <br>*可选*|string|
|**user_id**  <br>*可选*|integer|
|**value**  <br>*可选*|string|


<a name="model-jsonmap"></a>
### model.JSONMap
*类型* : object


<a name="model-job"></a>
### model.Job

|名称|类型|
|---|---|
|**args**  <br>*可选*|[model.JSONMap](#model-jsonmap)|
|**bio**  <br>*可选*|string|
|**cdn_clusters**  <br>*可选*|< [model.CDNCluster](#model-cdncluster) > array|
|**created_at**  <br>*可选*|string|
|**id**  <br>*可选*|integer|
|**result**  <br>*可选*|[model.JSONMap](#model-jsonmap)|
|**scheduler_clusters**  <br>*可选*|< [model.SchedulerCluster](#model-schedulercluster) > array|
|**status**  <br>*可选*|string|
|**task_id**  <br>*可选*|string|
|**type**  <br>*可选*|string|
|**updated_at**  <br>*可选*|string|
|**user_id**  <br>*可选*|integer|


<a name="model-oauth"></a>
### model.Oauth

|名称|类型|
|---|---|
|**bio**  <br>*可选*|string|
|**client_id**  <br>*可选*|string|
|**client_secret**  <br>*可选*|string|
|**created_at**  <br>*可选*|string|
|**id**  <br>*可选*|integer|
|**name**  <br>*可选*|string|
|**redirect_url**  <br>*可选*|string|
|**updated_at**  <br>*可选*|string|


<a name="model-scheduler"></a>
### model.Scheduler

|名称|类型|
|---|---|
|**created_at**  <br>*可选*|string|
|**host_name**  <br>*可选*|string|
|**id**  <br>*可选*|integer|
|**idc**  <br>*可选*|string|
|**ip**  <br>*可选*|string|
|**location**  <br>*可选*|string|
|**net_config**  <br>*可选*|[model.JSONMap](#model-jsonmap)|
|**port**  <br>*可选*|integer|
|**schedulerClusterID**  <br>*可选*|integer|
|**status**  <br>*可选*|string|
|**updated_at**  <br>*可选*|string|
|**vips**  <br>*可选*|string|


<a name="model-schedulercluster"></a>
### model.SchedulerCluster

|名称|类型|
|---|---|
|**bio**  <br>*可选*|string|
|**cdn_clusters**  <br>*可选*|< [model.CDNCluster](#model-cdncluster) > array|
|**client_config**  <br>*可选*|[model.JSONMap](#model-jsonmap)|
|**config**  <br>*可选*|[model.JSONMap](#model-jsonmap)|
|**created_at**  <br>*可选*|string|
|**id**  <br>*可选*|integer|
|**is_default**  <br>*可选*|boolean|
|**jobs**  <br>*可选*|< [model.Job](#model-job) > array|
|**name**  <br>*可选*|string|
|**scopes**  <br>*可选*|[model.JSONMap](#model-jsonmap)|
|**security_group_id**  <br>*可选*|integer|
|**updated_at**  <br>*可选*|string|


<a name="model-securitygroup"></a>
### model.SecurityGroup

|名称|类型|
|---|---|
|**bio**  <br>*可选*|string|
|**created_at**  <br>*可选*|string|
|**id**  <br>*可选*|integer|
|**name**  <br>*可选*|string|
|**security_rules**  <br>*可选*|< [model.SecurityRule](#model-securityrule) > array|
|**updated_at**  <br>*可选*|string|


<a name="model-securityrule"></a>
### model.SecurityRule

|名称|类型|
|---|---|
|**bio**  <br>*可选*|string|
|**created_at**  <br>*可选*|string|
|**domain**  <br>*可选*|string|
|**id**  <br>*可选*|integer|
|**name**  <br>*可选*|string|
|**proxy_domain**  <br>*可选*|string|
|**security_groups**  <br>*可选*|< [model.SecurityGroup](#model-securitygroup) > array|
|**updated_at**  <br>*可选*|string|


<a name="model-user"></a>
### model.User

|名称|类型|
|---|---|
|**avatar**  <br>*可选*|string|
|**bio**  <br>*可选*|string|
|**created_at**  <br>*可选*|string|
|**email**  <br>*可选*|string|
|**id**  <br>*可选*|integer|
|**location**  <br>*可选*|string|
|**name**  <br>*可选*|string|
|**phone**  <br>*可选*|string|
|**state**  <br>*可选*|string|
|**updated_at**  <br>*可选*|string|


<a name="rbac-permission"></a>
### rbac.Permission

|名称|类型|
|---|---|
|**action**  <br>*必填*|string|
|**object**  <br>*必填*|string|


<a name="types-addpermissionforrolerequest"></a>
### types.AddPermissionForRoleRequest

|名称|类型|
|---|---|
|**action**  <br>*必填*|string|
|**object**  <br>*必填*|string|


<a name="types-cdnclusterconfig"></a>
### types.CDNClusterConfig

|名称|类型|
|---|---|
|**load_limit**  <br>*可选*|integer|
|**net_topology**  <br>*可选*|string|


<a name="types-createcdnclusterrequest"></a>
### types.CreateCDNClusterRequest

|名称|类型|
|---|---|
|**bio**  <br>*可选*|string|
|**config**  <br>*必填*|[types.CDNClusterConfig](#types-cdnclusterconfig)|
|**name**  <br>*必填*|string|


<a name="types-createcdnrequest"></a>
### types.CreateCDNRequest

|名称|类型|
|---|---|
|**cdn_cluster_id**  <br>*必填*|integer|
|**download_port**  <br>*必填*|integer|
|**host_name**  <br>*必填*|string|
|**idc**  <br>*必填*|string|
|**ip**  <br>*必填*|string|
|**location**  <br>*可选*|string|
|**port**  <br>*必填*|integer|


<a name="types-createconfigrequest"></a>
### types.CreateConfigRequest

|名称|类型|
|---|---|
|**bio**  <br>*可选*|string|
|**name**  <br>*必填*|string|
|**user_id**  <br>*必填*|integer|
|**value**  <br>*必填*|string|


<a name="types-createjobrequest"></a>
### types.CreateJobRequest

|名称|类型|
|---|---|
|**args**  <br>*可选*|object|
|**bio**  <br>*可选*|string|
|**cdn_cluster_ids**  <br>*可选*|< integer > array|
|**result**  <br>*可选*|object|
|**scheduler_cluster_ids**  <br>*可选*|< integer > array|
|**type**  <br>*必填*|string|
|**user_id**  <br>*可选*|integer|


<a name="types-createoauthrequest"></a>
### types.CreateOauthRequest

|名称|类型|
|---|---|
|**bio**  <br>*可选*|string|
|**client_id**  <br>*必填*|string|
|**client_secret**  <br>*必填*|string|
|**name**  <br>*必填*|string|
|**redirect_url**  <br>*可选*|string|


<a name="types-createrolerequest"></a>
### types.CreateRoleRequest

|名称|类型|
|---|---|
|**permissions**  <br>*必填*|< [rbac.Permission](#rbac-permission) > array|
|**role**  <br>*必填*|string|


<a name="types-createschedulerclusterrequest"></a>
### types.CreateSchedulerClusterRequest

|名称|类型|
|---|---|
|**bio**  <br>*可选*|string|
|**cdn_cluster_id**  <br>*可选*|integer|
|**client_config**  <br>*必填*|[types.SchedulerClusterClientConfig](#types-schedulerclusterclientconfig)|
|**config**  <br>*必填*|[types.SchedulerClusterConfig](#types-schedulerclusterconfig)|
|**is_default**  <br>*可选*|boolean|
|**name**  <br>*必填*|string|
|**scopes**  <br>*可选*|[types.SchedulerClusterScopes](#types-schedulerclusterscopes)|


<a name="types-createschedulerrequest"></a>
### types.CreateSchedulerRequest

|名称|类型|
|---|---|
|**host_name**  <br>*必填*|string|
|**idc**  <br>*必填*|string|
|**ip**  <br>*必填*|string|
|**location**  <br>*可选*|string|
|**net_config**  <br>*可选*|object|
|**port**  <br>*必填*|integer|
|**scheduler_cluster_id**  <br>*必填*|integer|
|**vips**  <br>*可选*|string|


<a name="types-createsecuritygrouprequest"></a>
### types.CreateSecurityGroupRequest

|名称|类型|
|---|---|
|**bio**  <br>*可选*|string|
|**name**  <br>*必填*|string|


<a name="types-createsecurityrulerequest"></a>
### types.CreateSecurityRuleRequest

|名称|类型|
|---|---|
|**bio**  <br>*可选*|string|
|**domain**  <br>*必填*|string|
|**name**  <br>*必填*|string|
|**proxy_domain**  <br>*可选*|string|


<a name="types-createv1preheatrequest"></a>
### types.CreateV1PreheatRequest

|名称|类型|
|---|---|
|**filter**  <br>*可选*|string|
|**headers**  <br>*可选*|< string, string > map|
|**type**  <br>*必填*|string|
|**url**  <br>*必填*|string|


<a name="types-createv1preheatresponse"></a>
### types.CreateV1PreheatResponse

|名称|类型|
|---|---|
|**id**  <br>*可选*|string|


<a name="types-deletepermissionforrolerequest"></a>
### types.DeletePermissionForRoleRequest

|名称|类型|
|---|---|
|**action**  <br>*必填*|string|
|**object**  <br>*必填*|string|


<a name="types-getv1preheatresponse"></a>
### types.GetV1PreheatResponse

|名称|类型|
|---|---|
|**finishTime**  <br>*可选*|string|
|**id**  <br>*可选*|string|
|**startTime**  <br>*可选*|string|
|**status**  <br>*可选*|string|


<a name="types-resetpasswordrequest"></a>
### types.ResetPasswordRequest

|名称|类型|
|---|---|
|**new_password**  <br>*必填*|string|
|**old_password**  <br>*必填*|string|


<a name="types-schedulerclusterclientconfig"></a>
### types.SchedulerClusterClientConfig

|名称|类型|
|---|---|
|**load_limit**  <br>*可选*|integer|


<a name="types-schedulerclusterconfig"></a>
### types.SchedulerClusterConfig
*类型* : object


<a name="types-schedulerclusterscopes"></a>
### types.SchedulerClusterScopes
*类型* : object


<a name="types-signuprequest"></a>
### types.SignUpRequest

|名称|类型|
|---|---|
|**avatar**  <br>*可选*|string|
|**bio**  <br>*可选*|string|
|**email**  <br>*必填*|string|
|**location**  <br>*可选*|string|
|**name**  <br>*必填*|string|
|**password**  <br>*必填*|string|
|**phone**  <br>*可选*|string|


<a name="types-updatecdnclusterrequest"></a>
### types.UpdateCDNClusterRequest

|名称|类型|
|---|---|
|**bio**  <br>*可选*|string|
|**config**  <br>*可选*|[types.CDNClusterConfig](#types-cdnclusterconfig)|
|**name**  <br>*可选*|string|


<a name="types-updatecdnrequest"></a>
### types.UpdateCDNRequest

|名称|类型|
|---|---|
|**cdn_cluster_id**  <br>*可选*|integer|
|**download_port**  <br>*可选*|integer|
|**idc**  <br>*可选*|string|
|**ip**  <br>*可选*|string|
|**location**  <br>*可选*|string|
|**port**  <br>*可选*|integer|


<a name="types-updateconfigrequest"></a>
### types.UpdateConfigRequest

|名称|类型|
|---|---|
|**bio**  <br>*可选*|string|
|**name**  <br>*可选*|string|
|**user_id**  <br>*可选*|integer|
|**value**  <br>*可选*|string|


<a name="types-updatejobrequest"></a>
### types.UpdateJobRequest

|名称|类型|
|---|---|
|**bio**  <br>*可选*|string|
|**user_id**  <br>*可选*|integer|


<a name="types-updateoauthrequest"></a>
### types.UpdateOauthRequest

|名称|类型|
|---|---|
|**bio**  <br>*可选*|string|
|**client_id**  <br>*可选*|string|
|**client_secret**  <br>*可选*|string|
|**name**  <br>*可选*|string|
|**redirect_url**  <br>*可选*|string|


<a name="types-updateschedulerclusterrequest"></a>
### types.UpdateSchedulerClusterRequest

|名称|类型|
|---|---|
|**bio**  <br>*可选*|string|
|**cdn_cluster_id**  <br>*可选*|integer|
|**client_config**  <br>*可选*|[types.SchedulerClusterClientConfig](#types-schedulerclusterclientconfig)|
|**config**  <br>*可选*|[types.SchedulerClusterConfig](#types-schedulerclusterconfig)|
|**is_default**  <br>*可选*|boolean|
|**name**  <br>*可选*|string|
|**scopes**  <br>*可选*|[types.SchedulerClusterScopes](#types-schedulerclusterscopes)|


<a name="types-updateschedulerrequest"></a>
### types.UpdateSchedulerRequest

|名称|类型|
|---|---|
|**idc**  <br>*可选*|string|
|**ip**  <br>*可选*|string|
|**location**  <br>*可选*|string|
|**net_config**  <br>*可选*|object|
|**port**  <br>*可选*|integer|
|**scheduler_cluster_id**  <br>*可选*|integer|
|**scheduler_id**  <br>*可选*|integer|
|**vips**  <br>*可选*|string|


<a name="types-updatesecuritygrouprequest"></a>
### types.UpdateSecurityGroupRequest

|名称|类型|
|---|---|
|**bio**  <br>*可选*|string|
|**name**  <br>*可选*|string|


<a name="types-updatesecurityrulerequest"></a>
### types.UpdateSecurityRuleRequest

|名称|类型|
|---|---|
|**bio**  <br>*可选*|string|
|**domain**  <br>*可选*|string|
|**name**  <br>*可选*|string|
|**proxy_domain**  <br>*可选*|string|





