# Dragonfly Manager


<a name="overview"></a>
## Overview
Dragonfly Manager Server


### Version information
*Version* : 1.0.0


### License information
*License* : Apache 2.0  
*Terms of service* : null


### URI scheme
*Host* : localhost:8080  
*BasePath* : /api/v1




<a name="paths"></a>
## Paths

<a name="api-v1-applications-post"></a>
### Create Application
```
POST /api/v1/applications
```


#### Description
create by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Body**|**Application**  <br>*required*|Application|[types.CreateApplicationRequest](#types-createapplicationrequest)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.Application](#model-application)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Application


<a name="api-v1-applications-get"></a>
### Get Applications
```
GET /api/v1/applications
```


#### Description
Get Applications


#### Parameters

|Type|Name|Description|Schema|Default|
|---|---|---|---|---|
|**Query**|**page**  <br>*required*|current page|integer|`0`|
|**Query**|**per_page**  <br>*required*|return max item count, default 10, max 50|integer|`10`|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|< [model.Application](#model-application) > array|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Application


<a name="api-v1-applications-id-get"></a>
### Get Application
```
GET /api/v1/applications/{id}
```


#### Description
Get Application by id


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.Application](#model-application)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Application


<a name="api-v1-applications-id-delete"></a>
### Destroy Application
```
DELETE /api/v1/applications/{id}
```


#### Description
Destroy by id


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**404**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Application


<a name="api-v1-applications-id-patch"></a>
### Update Application
```
PATCH /api/v1/applications/{id}
```


#### Description
Update by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|
|**Body**|**Application**  <br>*required*|Application|[types.UpdateApplicationRequest](#types-updateapplicationrequest)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.Application](#model-application)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Application


<a name="api-v1-applications-id-cdn-clusters-cdn_cluster_id-put"></a>
### Add CDN to Application
```
PUT /api/v1/applications/{id}/cdn-clusters/{cdn_cluster_id}
```


#### Description
Add CDN to Application


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**cdn_cluster_id**  <br>*required*|cdn cluster id|string|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**404**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Application


<a name="api-v1-applications-id-cdn-clusters-cdn_cluster_id-delete"></a>
### Delete CDN to Application
```
DELETE /api/v1/applications/{id}/cdn-clusters/{cdn_cluster_id}
```


#### Description
Delete CDN to Application


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**cdn_cluster_id**  <br>*required*|cdn cluster id|string|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**404**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Application


<a name="api-v1-applications-id-scheduler-clusters-scheduler_cluster_id-put"></a>
### Add Scheduler to Application
```
PUT /api/v1/applications/{id}/scheduler-clusters/{scheduler_cluster_id}
```


#### Description
Add Scheduler to Application


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|
|**Path**|**scheduler_cluster_id**  <br>*required*|scheduler cluster id|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**404**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Application


<a name="api-v1-applications-id-scheduler-clusters-scheduler_cluster_id-delete"></a>
### Delete Scheduler to Application
```
DELETE /api/v1/applications/{id}/scheduler-clusters/{scheduler_cluster_id}
```


#### Description
Delete Scheduler to Application


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|
|**Path**|**scheduler_cluster_id**  <br>*required*|scheduler cluster id|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**404**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Application


<a name="api-v1-cdn-clusters-post"></a>
### Create CDNCluster
```
POST /api/v1/cdn-clusters
```


#### Description
create by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Body**|**CDNCluster**  <br>*required*|DNCluster|[types.CreateCDNClusterRequest](#types-createcdnclusterrequest)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.CDNCluster](#model-cdncluster)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* CDNCluster


<a name="api-v1-cdn-clusters-get"></a>
### Get CDNClusters
```
GET /api/v1/cdn-clusters
```


#### Description
Get CDNClusters


#### Parameters

|Type|Name|Description|Schema|Default|
|---|---|---|---|---|
|**Query**|**page**  <br>*required*|current page|integer|`0`|
|**Query**|**per_page**  <br>*required*|return max item count, default 10, max 50|integer|`10`|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|< [model.CDNCluster](#model-cdncluster) > array|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* CDNCluster


<a name="api-v1-cdn-clusters-id-get"></a>
### Get CDNCluster
```
GET /api/v1/cdn-clusters/{id}
```


#### Description
Get CDNCluster by id


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.CDNCluster](#model-cdncluster)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* CDNCluster


<a name="api-v1-cdn-clusters-id-delete"></a>
### Destroy CDNCluster
```
DELETE /api/v1/cdn-clusters/{id}
```


#### Description
Destroy by id


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**404**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* CDNCluster


<a name="api-v1-cdn-clusters-id-patch"></a>
### Update CDNCluster
```
PATCH /api/v1/cdn-clusters/{id}
```


#### Description
Update by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|
|**Body**|**CDNCluster**  <br>*required*|CDNCluster|[types.UpdateCDNClusterRequest](#types-updatecdnclusterrequest)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.CDNCluster](#model-cdncluster)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* CDNCluster


<a name="api-v1-cdn-clusters-id-cdns-cdn_id-put"></a>
### Add Instance to CDNCluster
```
PUT /api/v1/cdn-clusters/{id}/cdns/{cdn_id}
```


#### Description
Add CDN to CDNCluster


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**cdn_id**  <br>*required*|cdn id|string|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**404**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* CDNCluster


<a name="api-v1-cdn-clusters-id-scheduler-clusters-scheduler_cluster_id-put"></a>
### Add SchedulerCluster to CDNCluster
```
PUT /api/v1/cdn-clusters/{id}/scheduler-clusters/{scheduler_cluster_id}
```


#### Description
Add SchedulerCluster to CDNCluster


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|
|**Path**|**scheduler_cluster_id**  <br>*required*|scheduler cluster id|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**404**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* CDNCluster


<a name="api-v1-cdns-post"></a>
### Create CDN
```
POST /api/v1/cdns
```


#### Description
create by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Body**|**CDN**  <br>*required*|CDN|[types.CreateCDNRequest](#types-createcdnrequest)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.CDN](#model-cdn)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* CDN


<a name="api-v1-cdns-get"></a>
### Get CDNs
```
GET /api/v1/cdns
```


#### Description
Get CDNs


#### Parameters

|Type|Name|Description|Schema|Default|
|---|---|---|---|---|
|**Query**|**page**  <br>*required*|current page|integer|`0`|
|**Query**|**per_page**  <br>*required*|return max item count, default 10, max 50|integer|`10`|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|< [model.CDN](#model-cdn) > array|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* CDN


<a name="api-v1-cdns-id-get"></a>
### Get CDN
```
GET /api/v1/cdns/{id}
```


#### Description
Get CDN by id


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.CDN](#model-cdn)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* CDN


<a name="api-v1-cdns-id-delete"></a>
### Destroy CDN
```
DELETE /api/v1/cdns/{id}
```


#### Description
Destroy by id


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**404**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* CDN


<a name="api-v1-cdns-id-patch"></a>
### Update CDN
```
PATCH /api/v1/cdns/{id}
```


#### Description
Update by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|
|**Body**|**CDN**  <br>*required*|CDN|[types.UpdateCDNRequest](#types-updatecdnrequest)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.CDN](#model-cdn)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* CDN


<a name="api-v1-configs-post"></a>
### Create Config
```
POST /api/v1/configs
```


#### Description
create by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Body**|**Config**  <br>*required*|Config|[types.CreateConfigRequest](#types-createconfigrequest)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.Config](#model-config)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Config


<a name="api-v1-configs-get"></a>
### Get Configs
```
GET /api/v1/configs
```


#### Description
Get Configs


#### Parameters

|Type|Name|Description|Schema|Default|
|---|---|---|---|---|
|**Query**|**page**  <br>*required*|current page|integer|`0`|
|**Query**|**per_page**  <br>*required*|return max item count, default 10, max 50|integer|`10`|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|< [model.Config](#model-config) > array|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Config


<a name="api-v1-configs-id-get"></a>
### Get Config
```
GET /api/v1/configs/{id}
```


#### Description
Get Config by id


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.Config](#model-config)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Config


<a name="api-v1-configs-id-delete"></a>
### Destroy Config
```
DELETE /api/v1/configs/{id}
```


#### Description
Destroy by id


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**404**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Config


<a name="api-v1-configs-id-patch"></a>
### Update Config
```
PATCH /api/v1/configs/{id}
```


#### Description
Update by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|
|**Body**|**Config**  <br>*required*|Config|[types.UpdateConfigRequest](#types-updateconfigrequest)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.Config](#model-config)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Config


<a name="api-v1-healthy-get"></a>
### Get Health
```
GET /api/v1/healthy
```


#### Description
Get app health


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**404**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Health


<a name="api-v1-jobs-post"></a>
### Create Job
```
POST /api/v1/jobs
```


#### Description
create by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Body**|**Job**  <br>*required*|Job|[types.CreateJobRequest](#types-createjobrequest)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.Job](#model-job)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Job


<a name="api-v1-jobs-get"></a>
### Get Jobs
```
GET /api/v1/jobs
```


#### Description
Get Jobs


#### Parameters

|Type|Name|Description|Schema|Default|
|---|---|---|---|---|
|**Query**|**page**  <br>*required*|current page|integer|`0`|
|**Query**|**per_page**  <br>*required*|return max item count, default 10, max 50|integer|`10`|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|< [model.Job](#model-job) > array|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Job


<a name="api-v1-jobs-id-get"></a>
### Get Job
```
GET /api/v1/jobs/{id}
```


#### Description
Get Job by id


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.Job](#model-job)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Job


<a name="api-v1-jobs-id-delete"></a>
### Destroy Job
```
DELETE /api/v1/jobs/{id}
```


#### Description
Destroy by id


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**404**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Job


<a name="api-v1-jobs-id-patch"></a>
### Update Job
```
PATCH /api/v1/jobs/{id}
```


#### Description
Update by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|
|**Body**|**Job**  <br>*required*|Job|[types.UpdateJobRequest](#types-updatejobrequest)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.Job](#model-job)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Job


<a name="api-v1-oauth-post"></a>
### Create Oauth
```
POST /api/v1/oauth
```


#### Description
create by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Body**|**Oauth**  <br>*required*|Oauth|[types.CreateOauthRequest](#types-createoauthrequest)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.Oauth](#model-oauth)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Oauth


<a name="api-v1-oauth-get"></a>
### Get Oauths
```
GET /api/v1/oauth
```


#### Description
Get Oauths


#### Parameters

|Type|Name|Description|Schema|Default|
|---|---|---|---|---|
|**Query**|**page**  <br>*required*|current page|integer|`0`|
|**Query**|**per_page**  <br>*required*|return max item count, default 10, max 50|integer|`10`|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|< [model.Oauth](#model-oauth) > array|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Oauth


<a name="api-v1-oauth-id-get"></a>
### Get Oauth
```
GET /api/v1/oauth/{id}
```


#### Description
Get Oauth by id


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.Oauth](#model-oauth)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Oauth


<a name="api-v1-oauth-id-delete"></a>
### Destroy Oauth
```
DELETE /api/v1/oauth/{id}
```


#### Description
Destroy by id


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**404**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Oauth


<a name="api-v1-oauth-id-patch"></a>
### Update Oauth
```
PATCH /api/v1/oauth/{id}
```


#### Description
Update by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|
|**Body**|**Oauth**  <br>*required*|Oauth|[types.UpdateOauthRequest](#types-updateoauthrequest)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.Oauth](#model-oauth)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Oauth


<a name="api-v1-permissions-get"></a>
### Get Permissions
```
GET /api/v1/permissions
```


#### Description
Get Permissions


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|< [rbac.Permission](#rbac-permission) > array|
|**400**||No Content|
|**500**||No Content|


#### Produces

* `application/json`


#### Tags

* Permission


<a name="api-v1-preheats-post"></a>
### Create V1 Preheat
```
POST /api/v1/preheats
```


#### Description
create by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Body**|**CDN**  <br>*required*|Preheat|[types.CreateV1PreheatRequest](#types-createv1preheatrequest)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[types.CreateV1PreheatResponse](#types-createv1preheatresponse)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Preheat


<a name="api-v1-preheats-id-get"></a>
### Get V1 Preheat
```
GET /api/v1/preheats/{id}
```


#### Description
Get Preheat by id


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[types.GetV1PreheatResponse](#types-getv1preheatresponse)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Preheat


<a name="api-v1-roles-post"></a>
### Create Role
```
POST /api/v1/roles
```


#### Description
Create Role by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Body**|**Role**  <br>*required*|Role|[types.CreateRoleRequest](#types-createrolerequest)|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Role


<a name="api-v1-roles-get"></a>
### Get Roles
```
GET /api/v1/roles
```


#### Description
Get roles


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Role


<a name="api-v1-roles-role-get"></a>
### Get Role
```
GET /api/v1/roles/:role
```


#### Description
Get Role


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**role**  <br>*required*|role|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Role


<a name="api-v1-roles-role-delete"></a>
### Destroy Role
```
DELETE /api/v1/roles/:role
```


#### Description
Destroy role by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**role**  <br>*required*|role|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Role


<a name="api-v1-roles-role-permissions-post"></a>
### Add Permission For Role
```
POST /api/v1/roles/:role/permissions
```


#### Description
Add Permission by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**role**  <br>*required*|role|string|
|**Body**|**Permission**  <br>*required*|Permission|[types.AddPermissionForRoleRequest](#types-addpermissionforrolerequest)|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Role


<a name="api-v1-roles-role-permissions-delete"></a>
### Update Role
```
DELETE /api/v1/roles/:role/permissions
```


#### Description
Remove Role Permission by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**role**  <br>*required*|role|string|
|**Body**|**Permission**  <br>*required*|Permission|[types.DeletePermissionForRoleRequest](#types-deletepermissionforrolerequest)|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Role


<a name="api-v1-scheduler-clusters-post"></a>
### Create SchedulerCluster
```
POST /api/v1/scheduler-clusters
```


#### Description
create by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Body**|**SchedulerCluster**  <br>*required*|SchedulerCluster|[types.CreateSchedulerClusterRequest](#types-createschedulerclusterrequest)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.SchedulerCluster](#model-schedulercluster)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* SchedulerCluster


<a name="api-v1-scheduler-clusters-get"></a>
### Get SchedulerClusters
```
GET /api/v1/scheduler-clusters
```


#### Description
Get SchedulerClusters


#### Parameters

|Type|Name|Description|Schema|Default|
|---|---|---|---|---|
|**Query**|**page**  <br>*required*|current page|integer|`0`|
|**Query**|**per_page**  <br>*required*|return max item count, default 10, max 50|integer|`10`|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|< [model.SchedulerCluster](#model-schedulercluster) > array|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* SchedulerCluster


<a name="api-v1-scheduler-clusters-id-get"></a>
### Get SchedulerCluster
```
GET /api/v1/scheduler-clusters/{id}
```


#### Description
Get SchedulerCluster by id


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.SchedulerCluster](#model-schedulercluster)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* SchedulerCluster


<a name="api-v1-scheduler-clusters-id-delete"></a>
### Destroy SchedulerCluster
```
DELETE /api/v1/scheduler-clusters/{id}
```


#### Description
Destroy by id


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**404**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* SchedulerCluster


<a name="api-v1-scheduler-clusters-id-patch"></a>
### Update SchedulerCluster
```
PATCH /api/v1/scheduler-clusters/{id}
```


#### Description
Update by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|
|**Body**|**SchedulerCluster**  <br>*required*|SchedulerCluster|[types.UpdateSchedulerClusterRequest](#types-updateschedulerclusterrequest)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.SchedulerCluster](#model-schedulercluster)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* SchedulerCluster


<a name="api-v1-scheduler-clusters-id-schedulers-scheduler_id-put"></a>
### Add Scheduler to schedulerCluster
```
PUT /api/v1/scheduler-clusters/{id}/schedulers/{scheduler_id}
```


#### Description
Add Scheduler to schedulerCluster


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|
|**Path**|**scheduler_id**  <br>*required*|scheduler id|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**404**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* SchedulerCluster


<a name="api-v1-schedulers-post"></a>
### Create Scheduler
```
POST /api/v1/schedulers
```


#### Description
create by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Body**|**Scheduler**  <br>*required*|Scheduler|[types.CreateSchedulerRequest](#types-createschedulerrequest)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.Scheduler](#model-scheduler)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Scheduler


<a name="api-v1-schedulers-get"></a>
### Get Schedulers
```
GET /api/v1/schedulers
```


#### Description
Get Schedulers


#### Parameters

|Type|Name|Description|Schema|Default|
|---|---|---|---|---|
|**Query**|**page**  <br>*required*|current page|integer|`0`|
|**Query**|**per_page**  <br>*required*|return max item count, default 10, max 50|integer|`10`|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|< [model.Scheduler](#model-scheduler) > array|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Scheduler


<a name="api-v1-schedulers-id-get"></a>
### Get Scheduler
```
GET /api/v1/schedulers/{id}
```


#### Description
Get Scheduler by id


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.Scheduler](#model-scheduler)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Scheduler


<a name="api-v1-schedulers-id-delete"></a>
### Destroy Scheduler
```
DELETE /api/v1/schedulers/{id}
```


#### Description
Destroy by id


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**404**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Scheduler


<a name="api-v1-schedulers-id-patch"></a>
### Update Scheduler
```
PATCH /api/v1/schedulers/{id}
```


#### Description
Update by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|
|**Body**|**Scheduler**  <br>*required*|Scheduler|[types.UpdateSchedulerRequest](#types-updateschedulerrequest)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.Scheduler](#model-scheduler)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Scheduler


<a name="api-v1-security-groups-post"></a>
### Create SecurityGroup
```
POST /api/v1/security-groups
```


#### Description
create by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Body**|**SecurityGroup**  <br>*required*|SecurityGroup|[types.CreateSecurityGroupRequest](#types-createsecuritygrouprequest)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.SecurityGroup](#model-securitygroup)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* SecurityGroup


<a name="api-v1-security-groups-get"></a>
### Get SecurityGroups
```
GET /api/v1/security-groups
```


#### Description
Get SecurityGroups


#### Parameters

|Type|Name|Description|Schema|Default|
|---|---|---|---|---|
|**Query**|**page**  <br>*required*|current page|integer|`0`|
|**Query**|**per_page**  <br>*required*|return max item count, default 10, max 50|integer|`10`|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|< [model.SecurityGroup](#model-securitygroup) > array|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* SecurityGroup


<a name="api-v1-security-groups-id-get"></a>
### Get SecurityGroup
```
GET /api/v1/security-groups/{id}
```


#### Description
Get SecurityGroup by id


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.SecurityGroup](#model-securitygroup)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* SecurityGroup


<a name="api-v1-security-groups-id-patch"></a>
### Update SecurityGroup
```
PATCH /api/v1/security-groups/{id}
```


#### Description
Update by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|
|**Body**|**SecurityGroup**  <br>*required*|SecurityGroup|[types.UpdateSecurityGroupRequest](#types-updatesecuritygrouprequest)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.SecurityGroup](#model-securitygroup)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* SecurityGroup


<a name="api-v1-security-groups-id-cdn-clusters-cdn_cluster_id-put"></a>
### Add CDN to SecurityGroup
```
PUT /api/v1/security-groups/{id}/cdn-clusters/{cdn_cluster_id}
```


#### Description
Add CDN to SecurityGroup


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**cdn_cluster_id**  <br>*required*|cdn cluster id|string|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**404**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* SecurityGroup


<a name="api-v1-security-groups-id-scheduler-clusters-scheduler_cluster_id-put"></a>
### Add Scheduler to SecurityGroup
```
PUT /api/v1/security-groups/{id}/scheduler-clusters/{scheduler_cluster_id}
```


#### Description
Add Scheduler to SecurityGroup


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|
|**Path**|**scheduler_cluster_id**  <br>*required*|scheduler cluster id|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**404**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* SecurityGroup


<a name="api-v1-security-groups-id-security-rules-security_rule_id-put"></a>
### Add SecurityRule to SecurityGroup
```
PUT /api/v1/security-groups/{id}/security-rules/{security_rule_id}
```


#### Description
Add SecurityRule to SecurityGroup


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|
|**Path**|**security_rule_id**  <br>*required*|security rule id|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**404**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* SecurityGroup


<a name="api-v1-security-groups-id-security-rules-security_rule_id-delete"></a>
### Destroy SecurityRule to SecurityGroup
```
DELETE /api/v1/security-groups/{id}/security-rules/{security_rule_id}
```


#### Description
Destroy SecurityRule to SecurityGroup


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|
|**Path**|**security_rule_id**  <br>*required*|security rule id|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**404**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* SecurityGroup


<a name="api-v1-security-rules-post"></a>
### Create SecurityRule
```
POST /api/v1/security-rules
```


#### Description
create by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Body**|**SecurityRule**  <br>*required*|SecurityRule|[types.CreateSecurityRuleRequest](#types-createsecurityrulerequest)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.SecurityRule](#model-securityrule)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* SecurityRule


<a name="api-v1-security-rules-get"></a>
### Get SecurityRules
```
GET /api/v1/security-rules
```


#### Description
Get SecurityRules


#### Parameters

|Type|Name|Description|Schema|Default|
|---|---|---|---|---|
|**Query**|**page**  <br>*required*|current page|integer|`0`|
|**Query**|**per_page**  <br>*required*|return max item count, default 10, max 50|integer|`10`|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|< [model.SecurityRule](#model-securityrule) > array|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* SecurityRule


<a name="api-v1-security-rules-id-get"></a>
### Get SecurityRule
```
GET /api/v1/security-rules/{id}
```


#### Description
Get SecurityRule by id


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.SecurityRule](#model-securityrule)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* SecurityRule


<a name="api-v1-security-rules-id-patch"></a>
### Update SecurityRule
```
PATCH /api/v1/security-rules/{id}
```


#### Description
Update by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|
|**Body**|**SecurityRule**  <br>*required*|SecurityRule|[types.UpdateSecurityRuleRequest](#types-updatesecurityrulerequest)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.SecurityRule](#model-securityrule)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* SecurityRule


<a name="api-v1-securitygroups-id-delete"></a>
### Destroy SecurityGroup
```
DELETE /api/v1/securityGroups/{id}
```


#### Description
Destroy by id


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**404**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* SecurityGroup


<a name="api-v1-securityrules-id-delete"></a>
### Destroy SecurityRule
```
DELETE /api/v1/securityRules/{id}
```


#### Description
Destroy by id


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**404**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* SecurityRule


<a name="api-v1-user-signin-name-get"></a>
### Oauth Signin
```
GET /api/v1/user/signin/{name}
```


#### Description
oauth signin by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**name**  <br>*required*|name|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**404**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* User


<a name="api-v1-user-signin-name-callback-get"></a>
### Oauth Signin Callback
```
GET /api/v1/user/signin/{name}/callback
```


#### Description
oauth signin callback by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**name**  <br>*required*|name|string|
|**Query**|**code**  <br>*required*|code|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**404**|No Content|
|**500**|No Content|


#### Tags

* Oauth


<a name="api-v1-user-signup-post"></a>
### SignUp user
```
POST /api/v1/user/signup
```


#### Description
signup by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Body**|**User**  <br>*required*|User|[types.SignUpRequest](#types-signuprequest)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.User](#model-user)|
|**400**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* User


<a name="api-v1-users-get"></a>
### Get Users
```
GET /api/v1/users
```


#### Description
Get Users


#### Parameters

|Type|Name|Description|Schema|Default|
|---|---|---|---|---|
|**Query**|**page**  <br>*required*|current page|integer|`0`|
|**Query**|**per_page**  <br>*required*|return max item count, default 10, max 50|integer|`10`|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|< [model.CDN](#model-cdn) > array|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* CDN


<a name="api-v1-users-id-get"></a>
### Get User
```
GET /api/v1/users/{id}
```


#### Description
Get User by id


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[model.User](#model-user)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* User


<a name="api-v1-users-id-reset_password-post"></a>
### Reset Password For User
```
POST /api/v1/users/{id}/reset_password
```


#### Description
reset password by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Body**|**User**  <br>*required*|User|[types.ResetPasswordRequest](#types-resetpasswordrequest)|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* User


<a name="api-v1-users-id-roles-get"></a>
### Get User Roles
```
GET /api/v1/users/{id}/roles
```


#### Description
get roles by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|< string > array|
|**400**||No Content|
|**500**||No Content|


#### Produces

* `application/json`


#### Tags

* User


<a name="api-v1-users-id-roles-role-put"></a>
### Add Role For User
```
PUT /api/v1/users/{id}/roles/{role}
```


#### Description
add role to user by uri config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|
|**Path**|**role**  <br>*required*|role|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Users


<a name="api-v1-users-id-roles-role-delete"></a>
### Delete Role For User
```
DELETE /api/v1/users/{id}/roles/{role}
```


#### Description
delete role by uri config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Path**|**id**  <br>*required*|id|string|
|**Path**|**role**  <br>*required*|role|string|


#### Responses

|HTTP Code|Schema|
|---|---|
|**200**|No Content|
|**400**|No Content|
|**500**|No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Users




<a name="definitions"></a>
## Definitions

<a name="model-application"></a>
### model.Application

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**created_at**  <br>*optional*|string|
|**download_rate_limit**  <br>*optional*|integer|
|**id**  <br>*optional*|integer|
|**name**  <br>*optional*|string|
|**state**  <br>*optional*|string|
|**updated_at**  <br>*optional*|string|
|**url**  <br>*optional*|string|
|**user_id**  <br>*optional*|integer|


<a name="model-cdn"></a>
### model.CDN

|Name|Schema|
|---|---|
|**cdnclusterID**  <br>*optional*|integer|
|**created_at**  <br>*optional*|string|
|**download_port**  <br>*optional*|integer|
|**host_name**  <br>*optional*|string|
|**id**  <br>*optional*|integer|
|**idc**  <br>*optional*|string|
|**ip**  <br>*optional*|string|
|**location**  <br>*optional*|string|
|**port**  <br>*optional*|integer|
|**state**  <br>*optional*|string|
|**updated_at**  <br>*optional*|string|


<a name="model-cdncluster"></a>
### model.CDNCluster

|Name|Schema|
|---|---|
|**application_id**  <br>*optional*|integer|
|**bio**  <br>*optional*|string|
|**config**  <br>*optional*|[model.JSONMap](#model-jsonmap)|
|**created_at**  <br>*optional*|string|
|**id**  <br>*optional*|integer|
|**is_default**  <br>*optional*|boolean|
|**jobs**  <br>*optional*|< [model.Job](#model-job) > array|
|**name**  <br>*optional*|string|
|**scheduler_clusters**  <br>*optional*|< [model.SchedulerCluster](#model-schedulercluster) > array|
|**security_group_id**  <br>*optional*|integer|
|**updated_at**  <br>*optional*|string|


<a name="model-config"></a>
### model.Config

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**created_at**  <br>*optional*|string|
|**id**  <br>*optional*|integer|
|**name**  <br>*optional*|string|
|**updated_at**  <br>*optional*|string|
|**user_id**  <br>*optional*|integer|
|**value**  <br>*optional*|string|


<a name="model-jsonmap"></a>
### model.JSONMap
*Type* : object


<a name="model-job"></a>
### model.Job

|Name|Schema|
|---|---|
|**args**  <br>*optional*|[model.JSONMap](#model-jsonmap)|
|**bio**  <br>*optional*|string|
|**cdn_clusters**  <br>*optional*|< [model.CDNCluster](#model-cdncluster) > array|
|**created_at**  <br>*optional*|string|
|**id**  <br>*optional*|integer|
|**result**  <br>*optional*|[model.JSONMap](#model-jsonmap)|
|**scheduler_clusters**  <br>*optional*|< [model.SchedulerCluster](#model-schedulercluster) > array|
|**state**  <br>*optional*|string|
|**task_id**  <br>*optional*|string|
|**type**  <br>*optional*|string|
|**updated_at**  <br>*optional*|string|
|**user_id**  <br>*optional*|integer|


<a name="model-oauth"></a>
### model.Oauth

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**client_id**  <br>*optional*|string|
|**client_secret**  <br>*optional*|string|
|**created_at**  <br>*optional*|string|
|**id**  <br>*optional*|integer|
|**name**  <br>*optional*|string|
|**redirect_url**  <br>*optional*|string|
|**updated_at**  <br>*optional*|string|


<a name="model-scheduler"></a>
### model.Scheduler

|Name|Schema|
|---|---|
|**created_at**  <br>*optional*|string|
|**host_name**  <br>*optional*|string|
|**id**  <br>*optional*|integer|
|**idc**  <br>*optional*|string|
|**ip**  <br>*optional*|string|
|**location**  <br>*optional*|string|
|**net_config**  <br>*optional*|[model.JSONMap](#model-jsonmap)|
|**port**  <br>*optional*|integer|
|**schedulerClusterID**  <br>*optional*|integer|
|**state**  <br>*optional*|string|
|**updated_at**  <br>*optional*|string|
|**vips**  <br>*optional*|string|


<a name="model-schedulercluster"></a>
### model.SchedulerCluster

|Name|Schema|
|---|---|
|**application_id**  <br>*optional*|integer|
|**bio**  <br>*optional*|string|
|**cdn_clusters**  <br>*optional*|< [model.CDNCluster](#model-cdncluster) > array|
|**client_config**  <br>*optional*|[model.JSONMap](#model-jsonmap)|
|**config**  <br>*optional*|[model.JSONMap](#model-jsonmap)|
|**created_at**  <br>*optional*|string|
|**id**  <br>*optional*|integer|
|**is_default**  <br>*optional*|boolean|
|**jobs**  <br>*optional*|< [model.Job](#model-job) > array|
|**name**  <br>*optional*|string|
|**scopes**  <br>*optional*|[model.JSONMap](#model-jsonmap)|
|**security_group_id**  <br>*optional*|integer|
|**updated_at**  <br>*optional*|string|


<a name="model-securitygroup"></a>
### model.SecurityGroup

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**created_at**  <br>*optional*|string|
|**id**  <br>*optional*|integer|
|**name**  <br>*optional*|string|
|**security_rules**  <br>*optional*|< [model.SecurityRule](#model-securityrule) > array|
|**updated_at**  <br>*optional*|string|


<a name="model-securityrule"></a>
### model.SecurityRule

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**created_at**  <br>*optional*|string|
|**domain**  <br>*optional*|string|
|**id**  <br>*optional*|integer|
|**name**  <br>*optional*|string|
|**proxy_domain**  <br>*optional*|string|
|**security_groups**  <br>*optional*|< [model.SecurityGroup](#model-securitygroup) > array|
|**updated_at**  <br>*optional*|string|


<a name="model-user"></a>
### model.User

|Name|Schema|
|---|---|
|**avatar**  <br>*optional*|string|
|**bio**  <br>*optional*|string|
|**created_at**  <br>*optional*|string|
|**email**  <br>*optional*|string|
|**id**  <br>*optional*|integer|
|**location**  <br>*optional*|string|
|**name**  <br>*optional*|string|
|**phone**  <br>*optional*|string|
|**state**  <br>*optional*|string|
|**updated_at**  <br>*optional*|string|


<a name="rbac-permission"></a>
### rbac.Permission

|Name|Schema|
|---|---|
|**action**  <br>*required*|string|
|**object**  <br>*required*|string|


<a name="types-addpermissionforrolerequest"></a>
### types.AddPermissionForRoleRequest

|Name|Schema|
|---|---|
|**action**  <br>*required*|string|
|**object**  <br>*required*|string|


<a name="types-cdnclusterconfig"></a>
### types.CDNClusterConfig

|Name|Schema|
|---|---|
|**load_limit**  <br>*optional*|integer|
|**net_topology**  <br>*optional*|string|


<a name="types-createapplicationrequest"></a>
### types.CreateApplicationRequest

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**download_rate_limit**  <br>*optional*|integer|
|**name**  <br>*required*|string|
|**state**  <br>*optional*|string|
|**url**  <br>*optional*|string|
|**user_id**  <br>*required*|integer|


<a name="types-createcdnclusterrequest"></a>
### types.CreateCDNClusterRequest

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**config**  <br>*required*|[types.CDNClusterConfig](#types-cdnclusterconfig)|
|**name**  <br>*required*|string|


<a name="types-createcdnrequest"></a>
### types.CreateCDNRequest

|Name|Schema|
|---|---|
|**cdn_cluster_id**  <br>*required*|integer|
|**download_port**  <br>*required*|integer|
|**host_name**  <br>*required*|string|
|**idc**  <br>*required*|string|
|**ip**  <br>*required*|string|
|**location**  <br>*optional*|string|
|**port**  <br>*required*|integer|


<a name="types-createconfigrequest"></a>
### types.CreateConfigRequest

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**name**  <br>*required*|string|
|**user_id**  <br>*required*|integer|
|**value**  <br>*required*|string|


<a name="types-createjobrequest"></a>
### types.CreateJobRequest

|Name|Schema|
|---|---|
|**args**  <br>*optional*|object|
|**bio**  <br>*optional*|string|
|**cdn_cluster_ids**  <br>*optional*|< integer > array|
|**result**  <br>*optional*|object|
|**scheduler_cluster_ids**  <br>*optional*|< integer > array|
|**type**  <br>*required*|string|
|**user_id**  <br>*optional*|integer|


<a name="types-createoauthrequest"></a>
### types.CreateOauthRequest

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**client_id**  <br>*required*|string|
|**client_secret**  <br>*required*|string|
|**name**  <br>*required*|string|
|**redirect_url**  <br>*optional*|string|


<a name="types-createrolerequest"></a>
### types.CreateRoleRequest

|Name|Schema|
|---|---|
|**permissions**  <br>*required*|< [rbac.Permission](#rbac-permission) > array|
|**role**  <br>*required*|string|


<a name="types-createschedulerclusterrequest"></a>
### types.CreateSchedulerClusterRequest

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**cdn_cluster_id**  <br>*optional*|integer|
|**client_config**  <br>*required*|[types.SchedulerClusterClientConfig](#types-schedulerclusterclientconfig)|
|**config**  <br>*required*|[types.SchedulerClusterConfig](#types-schedulerclusterconfig)|
|**is_default**  <br>*optional*|boolean|
|**name**  <br>*required*|string|
|**scopes**  <br>*optional*|[types.SchedulerClusterScopes](#types-schedulerclusterscopes)|


<a name="types-createschedulerrequest"></a>
### types.CreateSchedulerRequest

|Name|Schema|
|---|---|
|**host_name**  <br>*required*|string|
|**idc**  <br>*required*|string|
|**ip**  <br>*required*|string|
|**location**  <br>*optional*|string|
|**net_config**  <br>*optional*|object|
|**port**  <br>*required*|integer|
|**scheduler_cluster_id**  <br>*required*|integer|
|**vips**  <br>*optional*|string|


<a name="types-createsecuritygrouprequest"></a>
### types.CreateSecurityGroupRequest

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**name**  <br>*required*|string|


<a name="types-createsecurityrulerequest"></a>
### types.CreateSecurityRuleRequest

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**domain**  <br>*required*|string|
|**name**  <br>*required*|string|
|**proxy_domain**  <br>*optional*|string|


<a name="types-createv1preheatrequest"></a>
### types.CreateV1PreheatRequest

|Name|Schema|
|---|---|
|**filter**  <br>*optional*|string|
|**headers**  <br>*optional*|< string, string > map|
|**type**  <br>*required*|string|
|**url**  <br>*required*|string|


<a name="types-createv1preheatresponse"></a>
### types.CreateV1PreheatResponse

|Name|Schema|
|---|---|
|**id**  <br>*optional*|string|


<a name="types-deletepermissionforrolerequest"></a>
### types.DeletePermissionForRoleRequest

|Name|Schema|
|---|---|
|**action**  <br>*required*|string|
|**object**  <br>*required*|string|


<a name="types-getv1preheatresponse"></a>
### types.GetV1PreheatResponse

|Name|Schema|
|---|---|
|**finishTime**  <br>*optional*|string|
|**id**  <br>*optional*|string|
|**startTime**  <br>*optional*|string|
|**state**  <br>*optional*|string|


<a name="types-resetpasswordrequest"></a>
### types.ResetPasswordRequest

|Name|Schema|
|---|---|
|**new_password**  <br>*required*|string|
|**old_password**  <br>*required*|string|


<a name="types-schedulerclusterclientconfig"></a>
### types.SchedulerClusterClientConfig

|Name|Schema|
|---|---|
|**load_limit**  <br>*optional*|integer|


<a name="types-schedulerclusterconfig"></a>
### types.SchedulerClusterConfig
*Type* : object


<a name="types-schedulerclusterscopes"></a>
### types.SchedulerClusterScopes
*Type* : object


<a name="types-signuprequest"></a>
### types.SignUpRequest

|Name|Schema|
|---|---|
|**avatar**  <br>*optional*|string|
|**bio**  <br>*optional*|string|
|**email**  <br>*required*|string|
|**location**  <br>*optional*|string|
|**name**  <br>*required*|string|
|**password**  <br>*required*|string|
|**phone**  <br>*optional*|string|


<a name="types-updateapplicationrequest"></a>
### types.UpdateApplicationRequest

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**download_rate_limit**  <br>*optional*|integer|
|**name**  <br>*optional*|string|
|**state**  <br>*optional*|string|
|**url**  <br>*optional*|string|
|**user_id**  <br>*required*|integer|


<a name="types-updatecdnclusterrequest"></a>
### types.UpdateCDNClusterRequest

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**config**  <br>*optional*|[types.CDNClusterConfig](#types-cdnclusterconfig)|
|**name**  <br>*optional*|string|


<a name="types-updatecdnrequest"></a>
### types.UpdateCDNRequest

|Name|Schema|
|---|---|
|**cdn_cluster_id**  <br>*optional*|integer|
|**download_port**  <br>*optional*|integer|
|**idc**  <br>*optional*|string|
|**ip**  <br>*optional*|string|
|**location**  <br>*optional*|string|
|**port**  <br>*optional*|integer|


<a name="types-updateconfigrequest"></a>
### types.UpdateConfigRequest

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**name**  <br>*optional*|string|
|**user_id**  <br>*optional*|integer|
|**value**  <br>*optional*|string|


<a name="types-updatejobrequest"></a>
### types.UpdateJobRequest

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**user_id**  <br>*optional*|integer|


<a name="types-updateoauthrequest"></a>
### types.UpdateOauthRequest

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**client_id**  <br>*optional*|string|
|**client_secret**  <br>*optional*|string|
|**name**  <br>*optional*|string|
|**redirect_url**  <br>*optional*|string|


<a name="types-updateschedulerclusterrequest"></a>
### types.UpdateSchedulerClusterRequest

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**cdn_cluster_id**  <br>*optional*|integer|
|**client_config**  <br>*optional*|[types.SchedulerClusterClientConfig](#types-schedulerclusterclientconfig)|
|**config**  <br>*optional*|[types.SchedulerClusterConfig](#types-schedulerclusterconfig)|
|**is_default**  <br>*optional*|boolean|
|**name**  <br>*optional*|string|
|**scopes**  <br>*optional*|[types.SchedulerClusterScopes](#types-schedulerclusterscopes)|


<a name="types-updateschedulerrequest"></a>
### types.UpdateSchedulerRequest

|Name|Schema|
|---|---|
|**idc**  <br>*optional*|string|
|**ip**  <br>*optional*|string|
|**location**  <br>*optional*|string|
|**net_config**  <br>*optional*|object|
|**port**  <br>*optional*|integer|
|**scheduler_cluster_id**  <br>*optional*|integer|
|**scheduler_id**  <br>*optional*|integer|
|**vips**  <br>*optional*|string|


<a name="types-updatesecuritygrouprequest"></a>
### types.UpdateSecurityGroupRequest

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**name**  <br>*optional*|string|


<a name="types-updatesecurityrulerequest"></a>
### types.UpdateSecurityRuleRequest

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**domain**  <br>*optional*|string|
|**name**  <br>*optional*|string|
|**proxy_domain**  <br>*optional*|string|





