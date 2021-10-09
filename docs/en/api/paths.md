
<a name="paths"></a>
## Paths

<a name="cdn-clusters-post"></a>
### Create CDNCluster
```
POST /cdn-clusters
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


<a name="cdn-clusters-get"></a>
### Get CDNClusters
```
GET /cdn-clusters
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


<a name="cdn-clusters-id-get"></a>
### Get CDNCluster
```
GET /cdn-clusters/{id}
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


<a name="cdn-clusters-id-delete"></a>
### Destroy CDNCluster
```
DELETE /cdn-clusters/{id}
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


<a name="cdn-clusters-id-patch"></a>
### Update CDNCluster
```
PATCH /cdn-clusters/{id}
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


<a name="cdn-clusters-id-cdns-cdn_id-put"></a>
### Add Instance to CDNCluster
```
PUT /cdn-clusters/{id}/cdns/{cdn_id}
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


<a name="cdn-clusters-id-scheduler-clusters-scheduler_cluster_id-put"></a>
### Add SchedulerCluster to CDNCluster
```
PUT /cdn-clusters/{id}/scheduler-clusters/{scheduler_cluster_id}
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


<a name="cdns-post"></a>
### Create CDN
```
POST /cdns
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


<a name="cdns-get"></a>
### Get CDNs
```
GET /cdns
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


<a name="cdns-id-get"></a>
### Get CDN
```
GET /cdns/{id}
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


<a name="cdns-id-delete"></a>
### Destroy CDN
```
DELETE /cdns/{id}
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


<a name="cdns-id-patch"></a>
### Update CDN
```
PATCH /cdns/{id}
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


<a name="healthy-get"></a>
### Get Health
```
GET /healthy
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


<a name="oauth-post"></a>
### Create Oauth
```
POST /oauth
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


<a name="oauth-get"></a>
### Get Oauths
```
GET /oauth
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


<a name="oauth-id-get"></a>
### Get Oauth
```
GET /oauth/{id}
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


<a name="oauth-id-delete"></a>
### Destroy Oauth
```
DELETE /oauth/{id}
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


<a name="oauth-id-patch"></a>
### Update Oauth
```
PATCH /oauth/{id}
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


<a name="permissions-get"></a>
### Get Permissions
```
GET /permissions
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


<a name="preheats-post"></a>
### Create Preheat
```
POST /preheats
```


#### Description
create by json config


#### Parameters

|Type|Name|Description|Schema|
|---|---|---|---|
|**Body**|**CDN**  <br>*required*|Preheat|[types.CreatePreheatRequest](#types-createpreheatrequest)|


#### Responses

|HTTP Code|Description|Schema|
|---|---|---|
|**200**|OK|[types.Preheat](#types-preheat)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Preheat


<a name="preheats-id-get"></a>
### Get Preheat
```
GET /preheats/{id}
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
|**200**|OK|[types.Preheat](#types-preheat)|
|**400**||No Content|
|**404**||No Content|
|**500**||No Content|


#### Consumes

* `application/json`


#### Produces

* `application/json`


#### Tags

* Preheat


<a name="roles-post"></a>
### Create Role
```
POST /roles
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


<a name="roles-get"></a>
### Get Roles
```
GET /roles
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


<a name="roles-role-get"></a>
### Get Role
```
GET /roles/:role
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


<a name="roles-role-delete"></a>
### Destroy Role
```
DELETE /roles/:role
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


<a name="roles-role-permissions-post"></a>
### Add Permission For Role
```
POST /roles/:role/permissions
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


<a name="roles-role-permissions-delete"></a>
### Update Role
```
DELETE /roles/:role/permissions
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


<a name="scheduler-clusters-post"></a>
### Create SchedulerCluster
```
POST /scheduler-clusters
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


<a name="scheduler-clusters-get"></a>
### Get SchedulerClusters
```
GET /scheduler-clusters
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


<a name="scheduler-clusters-id-get"></a>
### Get SchedulerCluster
```
GET /scheduler-clusters/{id}
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


<a name="scheduler-clusters-id-delete"></a>
### Destroy SchedulerCluster
```
DELETE /scheduler-clusters/{id}
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


<a name="scheduler-clusters-id-patch"></a>
### Update SchedulerCluster
```
PATCH /scheduler-clusters/{id}
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


<a name="scheduler-clusters-id-schedulers-scheduler_id-put"></a>
### Add Scheduler to schedulerCluster
```
PUT /scheduler-clusters/{id}/schedulers/{scheduler_id}
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


<a name="schedulers-post"></a>
### Create Scheduler
```
POST /schedulers
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


<a name="schedulers-get"></a>
### Get Schedulers
```
GET /schedulers
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


<a name="schedulers-id-get"></a>
### Get Scheduler
```
GET /schedulers/{id}
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


<a name="schedulers-id-delete"></a>
### Destroy Scheduler
```
DELETE /schedulers/{id}
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


<a name="schedulers-id-patch"></a>
### Update Scheduler
```
PATCH /schedulers/{id}
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


<a name="security-groups-post"></a>
### Create SecurityGroup
```
POST /security-groups
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


<a name="security-groups-get"></a>
### Get SecurityGroups
```
GET /security-groups
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


<a name="security-groups-id-get"></a>
### Get SecurityGroup
```
GET /security-groups/{id}
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


<a name="security-groups-id-patch"></a>
### Update SecurityGroup
```
PATCH /security-groups/{id}
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


<a name="security-groups-id-cdn-clusters-cdn_cluster_id-put"></a>
### Add CDN to SecurityGroup
```
PUT /security-groups/{id}/cdn-clusters/{cdn_cluster_id}
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


<a name="security-groups-id-scheduler-clusters-scheduler_cluster_id-put"></a>
### Add Scheduler to SecurityGroup
```
PUT /security-groups/{id}/scheduler-clusters/{scheduler_cluster_id}
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


<a name="securitygroups-id-delete"></a>
### Destroy SecurityGroup
```
DELETE /securityGroups/{id}
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


<a name="user-signin-name-get"></a>
### Oauth Signin
```
GET /user/signin/{name}
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


<a name="user-signin-name-callback-get"></a>
### Oauth Signin Callback
```
GET /user/signin/{name}/callback
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


<a name="user-signup-post"></a>
### SignUp user
```
POST /user/signup
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


<a name="users-id-reset_password-post"></a>
### Reset Password For User
```
POST /users/:id/reset_password
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


<a name="users-id-roles-get"></a>
### Get User Roles
```
GET /users/:id/roles
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


<a name="users-id-roles-role-put"></a>
### Add Role For User
```
PUT /users/:id/roles/:role
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


<a name="users-id-roles-role-delete"></a>
### Delete Role For User
```
DELETE /users/:id/roles/:role
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



