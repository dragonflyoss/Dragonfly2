# Dragonfly Manager
Dragonfly Manager Server

## Version: 1.0.0

**Contact information:**  
https://d7y.io  

**License:** Apache 2.0

### /cdn-clusters

#### GET
##### Summary:

Get CDNClusters

##### Description:

Get CDNClusters

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| page | query | current page | Yes | integer |
| per_page | query | return max item count, default 10, max 50 | Yes | integer |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [ [model.CDNCluster](#model.CDNCluster) ] |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

#### POST
##### Summary:

Create CDNCluster

##### Description:

create by json config

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| CDNCluster | body | DNCluster | Yes | [types.CreateCDNClusterRequest](#types.CreateCDNClusterRequest) |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [model.CDNCluster](#model.CDNCluster) |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

### /cdn-clusters/{id}

#### DELETE
##### Summary:

Destroy CDNCluster

##### Description:

Destroy by id

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 |  |
| 400 |  |
| 404 |  |
| 500 |  |

#### GET
##### Summary:

Get CDNCluster

##### Description:

Get CDNCluster by id

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [model.CDNCluster](#model.CDNCluster) |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

#### PATCH
##### Summary:

Update CDNCluster

##### Description:

Update by json config

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |
| CDNCluster | body | CDNCluster | Yes | [types.UpdateCDNClusterRequest](#types.UpdateCDNClusterRequest) |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [model.CDNCluster](#model.CDNCluster) |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

### /cdn-clusters/{id}/cdns/{cdn_id}

#### PUT
##### Summary:

Add Instance to CDNCluster

##### Description:

Add CDN to CDNCluster

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |
| cdn_id | path | cdn id | Yes | string |

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 |  |
| 400 |  |
| 404 |  |
| 500 |  |

### /cdn-clusters/{id}/scheduler-clusters/{scheduler_cluster_id}

#### PUT
##### Summary:

Add SchedulerCluster to CDNCluster

##### Description:

Add SchedulerCluster to CDNCluster

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |
| scheduler_cluster_id | path | scheduler cluster id | Yes | string |

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 |  |
| 400 |  |
| 404 |  |
| 500 |  |

### /cdns

#### GET
##### Summary:

Get CDNs

##### Description:

Get CDNs

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| page | query | current page | Yes | integer |
| per_page | query | return max item count, default 10, max 50 | Yes | integer |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [ [model.CDN](#model.CDN) ] |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

#### POST
##### Summary:

Create CDN

##### Description:

create by json config

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| CDN | body | CDN | Yes | [types.CreateCDNRequest](#types.CreateCDNRequest) |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [model.CDN](#model.CDN) |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

### /cdns/{id}

#### DELETE
##### Summary:

Destroy CDN

##### Description:

Destroy by id

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 |  |
| 400 |  |
| 404 |  |
| 500 |  |

#### GET
##### Summary:

Get CDN

##### Description:

Get CDN by id

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [model.CDN](#model.CDN) |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

#### PATCH
##### Summary:

Update CDN

##### Description:

Update by json config

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |
| CDN | body | CDN | Yes | [types.UpdateCDNRequest](#types.UpdateCDNRequest) |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [model.CDN](#model.CDN) |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

### /healthy

#### GET
##### Summary:

Get Health

##### Description:

Get app health

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 |  |
| 400 |  |
| 404 |  |
| 500 |  |

### /oauth

#### GET
##### Summary:

Get Oauths

##### Description:

Get Oauths

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| page | query | current page | Yes | integer |
| per_page | query | return max item count, default 10, max 50 | Yes | integer |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [ [model.Oauth](#model.Oauth) ] |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

#### POST
##### Summary:

Create Oauth

##### Description:

create by json config

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| Oauth | body | Oauth | Yes | [types.CreateOauthRequest](#types.CreateOauthRequest) |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [model.Oauth](#model.Oauth) |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

### /oauth/{id}

#### DELETE
##### Summary:

Destroy Oauth

##### Description:

Destroy by id

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 |  |
| 400 |  |
| 404 |  |
| 500 |  |

#### GET
##### Summary:

Get Oauth

##### Description:

Get Oauth by id

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [model.Oauth](#model.Oauth) |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

#### PATCH
##### Summary:

Update Oauth

##### Description:

Update by json config

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |
| Oauth | body | Oauth | Yes | [types.UpdateOauthRequest](#types.UpdateOauthRequest) |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [model.Oauth](#model.Oauth) |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

### /permissions

#### GET
##### Summary:

Get Permissions

##### Description:

Get Permissions

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [ [rbac.Permission](#rbac.Permission) ] |
| 400 |  |  |
| 500 |  |  |

### /preheats

#### POST
##### Summary:

Create Preheat

##### Description:

create by json config

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| CDN | body | Preheat | Yes | [types.CreatePreheatRequest](#types.CreatePreheatRequest) |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [types.Preheat](#types.Preheat) |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

### /preheats/{id}

#### GET
##### Summary:

Get Preheat

##### Description:

Get Preheat by id

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [types.Preheat](#types.Preheat) |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

### /roles

#### GET
##### Summary:

Get Roles

##### Description:

Get roles

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 |  |
| 400 |  |
| 500 |  |

#### POST
##### Summary:

Create Role

##### Description:

Create Role by json config

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| Role | body | Role | Yes | [types.CreateRoleRequest](#types.CreateRoleRequest) |

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 |  |
| 400 |  |
| 500 |  |

### /roles/:role

#### DELETE
##### Summary:

Destroy Role

##### Description:

Destroy role by json config

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| role | path | role | Yes | string |

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 |  |
| 400 |  |
| 500 |  |

#### GET
##### Summary:

Get Role

##### Description:

Get Role

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| role | path | role | Yes | string |

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 |  |
| 400 |  |
| 500 |  |

### /roles/:role/permissions

#### DELETE
##### Summary:

Update Role

##### Description:

Remove Role Permission by json config

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| Permission | body | Permission | Yes | [types.DeletePermissionForRoleRequest](#types.DeletePermissionForRoleRequest) |
| role | path | role | Yes | string |

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 |  |
| 400 |  |
| 500 |  |

#### POST
##### Summary:

Add Permission For Role

##### Description:

Add Permission by json config

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| Permission | body | Permission | Yes | [types.AddPermissionForRoleRequest](#types.AddPermissionForRoleRequest) |
| role | path | role | Yes | string |

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 |  |
| 400 |  |
| 500 |  |

### /scheduler-clusters

#### GET
##### Summary:

Get SchedulerClusters

##### Description:

Get SchedulerClusters

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| page | query | current page | Yes | integer |
| per_page | query | return max item count, default 10, max 50 | Yes | integer |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [ [model.SchedulerCluster](#model.SchedulerCluster) ] |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

#### POST
##### Summary:

Create SchedulerCluster

##### Description:

create by json config

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| SchedulerCluster | body | SchedulerCluster | Yes | [types.CreateSchedulerClusterRequest](#types.CreateSchedulerClusterRequest) |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [model.SchedulerCluster](#model.SchedulerCluster) |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

### /scheduler-clusters/{id}

#### DELETE
##### Summary:

Destroy SchedulerCluster

##### Description:

Destroy by id

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 |  |
| 400 |  |
| 404 |  |
| 500 |  |

#### GET
##### Summary:

Get SchedulerCluster

##### Description:

Get SchedulerCluster by id

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [model.SchedulerCluster](#model.SchedulerCluster) |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

#### PATCH
##### Summary:

Update SchedulerCluster

##### Description:

Update by json config

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |
| SchedulerCluster | body | SchedulerCluster | Yes | [types.UpdateSchedulerClusterRequest](#types.UpdateSchedulerClusterRequest) |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [model.SchedulerCluster](#model.SchedulerCluster) |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

### /scheduler-clusters/{id}/schedulers/{scheduler_id}

#### PUT
##### Summary:

Add Scheduler to schedulerCluster

##### Description:

Add Scheduler to schedulerCluster

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |
| scheduler_id | path | scheduler id | Yes | string |

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 |  |
| 400 |  |
| 404 |  |
| 500 |  |

### /schedulers

#### GET
##### Summary:

Get Schedulers

##### Description:

Get Schedulers

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| page | query | current page | Yes | integer |
| per_page | query | return max item count, default 10, max 50 | Yes | integer |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [ [model.Scheduler](#model.Scheduler) ] |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

#### POST
##### Summary:

Create Scheduler

##### Description:

create by json config

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| Scheduler | body | Scheduler | Yes | [types.CreateSchedulerRequest](#types.CreateSchedulerRequest) |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [model.Scheduler](#model.Scheduler) |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

### /schedulers/{id}

#### DELETE
##### Summary:

Destroy Scheduler

##### Description:

Destroy by id

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 |  |
| 400 |  |
| 404 |  |
| 500 |  |

#### GET
##### Summary:

Get Scheduler

##### Description:

Get Scheduler by id

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [model.Scheduler](#model.Scheduler) |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

#### PATCH
##### Summary:

Update Scheduler

##### Description:

Update by json config

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |
| Scheduler | body | Scheduler | Yes | [types.UpdateSchedulerRequest](#types.UpdateSchedulerRequest) |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [model.Scheduler](#model.Scheduler) |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

### /security-groups

#### GET
##### Summary:

Get SecurityGroups

##### Description:

Get SecurityGroups

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| page | query | current page | Yes | integer |
| per_page | query | return max item count, default 10, max 50 | Yes | integer |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [ [model.SecurityGroup](#model.SecurityGroup) ] |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

#### POST
##### Summary:

Create SecurityGroup

##### Description:

create by json config

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| SecurityGroup | body | SecurityGroup | Yes | [types.CreateSecurityGroupRequest](#types.CreateSecurityGroupRequest) |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [model.SecurityGroup](#model.SecurityGroup) |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

### /security-groups/{id}

#### GET
##### Summary:

Get SecurityGroup

##### Description:

Get SecurityGroup by id

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [model.SecurityGroup](#model.SecurityGroup) |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

#### PATCH
##### Summary:

Update SecurityGroup

##### Description:

Update by json config

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |
| SecurityGroup | body | SecurityGroup | Yes | [types.UpdateSecurityGroupRequest](#types.UpdateSecurityGroupRequest) |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [model.SecurityGroup](#model.SecurityGroup) |
| 400 |  |  |
| 404 |  |  |
| 500 |  |  |

### /security-groups/{id}/cdn-clusters/{cdn_cluster_id}

#### PUT
##### Summary:

Add CDN to SecurityGroup

##### Description:

Add CDN to SecurityGroup

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |
| cdn_cluster_id | path | cdn cluster id | Yes | string |

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 |  |
| 400 |  |
| 404 |  |
| 500 |  |

### /security-groups/{id}/scheduler-clusters/{scheduler_cluster_id}

#### PUT
##### Summary:

Add Scheduler to SecurityGroup

##### Description:

Add Scheduler to SecurityGroup

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |
| scheduler_cluster_id | path | scheduler cluster id | Yes | string |

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 |  |
| 400 |  |
| 404 |  |
| 500 |  |

### /securityGroups/{id}

#### DELETE
##### Summary:

Destroy SecurityGroup

##### Description:

Destroy by id

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 |  |
| 400 |  |
| 404 |  |
| 500 |  |

### /user/signin/{name}

#### GET
##### Summary:

Oauth Signin

##### Description:

oauth signin by json config

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| name | path | name | Yes | string |

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 |  |
| 400 |  |
| 404 |  |
| 500 |  |

### /user/signin/{name}/callback

#### GET
##### Summary:

Oauth Signin Callback

##### Description:

oauth signin callback by json config

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| name | path | name | Yes | string |
| code | query | code | Yes | string |

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 |  |
| 400 |  |
| 404 |  |
| 500 |  |

### /user/signup

#### POST
##### Summary:

SignUp user

##### Description:

signup by json config

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| User | body | User | Yes | [types.SignUpRequest](#types.SignUpRequest) |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [model.User](#model.User) |
| 400 |  |  |
| 500 |  |  |

### /users/:id/reset_password

#### POST
##### Summary:

Reset Password For User

##### Description:

reset password by json config

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| User | body | User | Yes | [types.ResetPasswordRequest](#types.ResetPasswordRequest) |

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 |  |
| 400 |  |
| 500 |  |

### /users/:id/roles

#### GET
##### Summary:

Get User Roles

##### Description:

get roles by json config

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |

##### Responses

| Code | Description | Schema |
| ---- | ----------- | ------ |
| 200 | OK | [ string ] |
| 400 |  |  |
| 500 |  |  |

### /users/:id/roles/:role

#### DELETE
##### Summary:

Delete Role For User

##### Description:

delete role by uri config

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |
| role | path | role | Yes | string |

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 |  |
| 400 |  |
| 500 |  |

#### PUT
##### Summary:

Add Role For User

##### Description:

add role to user by uri config

##### Parameters

| Name | Located in | Description | Required | Schema |
| ---- | ---------- | ----------- | -------- | ---- |
| id | path | id | Yes | string |
| role | path | role | Yes | string |

##### Responses

| Code | Description |
| ---- | ----------- |
| 200 |  |
| 400 |  |
| 500 |  |

### Models


#### model.Assertion

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| key | string |  | No |
| policy | [ [ string ] ] |  | No |
| policyMap | object |  | No |
| rm | [rbac.RoleManager](#rbac.RoleManager) |  | No |
| tokens | [ string ] |  | No |
| value | string |  | No |

#### model.AssertionMap

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| model.AssertionMap | object |  |  |

#### model.CDN

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| cdnclusterID | integer |  | No |
| download_port | integer |  | No |
| host_name | string |  | No |
| idc | string |  | No |
| ip | string |  | No |
| location | string |  | No |
| port | integer |  | No |
| status | string |  | No |

#### model.CDNCluster

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| bio | string |  | No |
| config | [model.JSONMap](#model.JSONMap) |  | No |
| is_default | boolean |  | No |
| name | string |  | No |
| securityGroupID | integer |  | No |

#### model.JSONMap

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| model.JSONMap | object |  |  |

#### model.Oauth

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| bio | string |  | No |
| client_id | string |  | No |
| client_secret | string |  | No |
| name | string |  | No |
| redirect_url | string |  | No |

#### model.Scheduler

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| host_name | string |  | No |
| idc | string |  | No |
| ip | string |  | No |
| location | string |  | No |
| net_config | [model.JSONMap](#model.JSONMap) |  | No |
| port | integer |  | No |
| schedulerClusterID | integer |  | No |
| status | string |  | No |
| vips | string |  | No |

#### model.SchedulerCluster

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| bio | string |  | No |
| client_config | [model.JSONMap](#model.JSONMap) |  | No |
| config | [model.JSONMap](#model.JSONMap) |  | No |
| is_default | boolean |  | No |
| name | string |  | No |
| scopes | [model.JSONMap](#model.JSONMap) |  | No |
| securityGroupID | integer |  | No |

#### model.SecurityGroup

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| bio | string |  | No |
| domain | string |  | No |
| name | string |  | No |
| proxy_domain | string |  | No |

#### model.User

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| avatar | string |  | No |
| bio | string |  | No |
| email | string |  | No |
| location | string |  | No |
| name | string |  | No |
| phone | string |  | No |
| private_token | string |  | No |
| state | string |  | No |

#### rbac.Permission

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| action | string |  | Yes |
| object | string |  | Yes |

#### rbac.RoleManager

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| rbac.RoleManager | object |  |  |

#### types.AddPermissionForRoleRequest

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| action | string |  | Yes |
| object | string |  | Yes |

#### types.CreateCDNClusterRequest

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| bio | string |  | No |
| config | object |  | Yes |
| name | string |  | Yes |
| security_group_domain | string |  | No |

#### types.CreateCDNRequest

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| cdn_cluster_id | integer |  | Yes |
| download_port | integer |  | Yes |
| host_name | string |  | Yes |
| idc | string |  | Yes |
| ip | string |  | Yes |
| location | string |  | No |
| port | integer |  | Yes |

#### types.CreateOauthRequest

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| bio | string |  | No |
| client_id | string |  | Yes |
| client_secret | string |  | Yes |
| name | string |  | Yes |
| redirect_url | string |  | No |

#### types.CreatePreheatRequest

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| filter | string |  | No |
| headers | object |  | No |
| scheduler_cluster_id | integer |  | No |
| type | string |  | Yes |
| url | string |  | Yes |

#### types.CreateRoleRequest

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| permissions | [ [rbac.Permission](#rbac.Permission) ] |  | Yes |
| role | string |  | Yes |

#### types.CreateSchedulerClusterRequest

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| bio | string |  | No |
| cdn_cluster_id | integer |  | No |
| client_config | object |  | Yes |
| config | object |  | Yes |
| is_default | boolean |  | No |
| name | string |  | Yes |
| scopes | object |  | No |
| security_group_domain | string |  | No |

#### types.CreateSchedulerRequest

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| host_name | string |  | Yes |
| idc | string |  | Yes |
| ip | string |  | Yes |
| location | string |  | No |
| net_config | object |  | No |
| port | integer |  | Yes |
| scheduler_cluster_id | integer |  | Yes |
| vips | string |  | No |

#### types.CreateSecurityGroupRequest

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| bio | string |  | No |
| domain | string |  | Yes |
| name | string |  | Yes |
| proxy_domain | string |  | No |

#### types.DeletePermissionForRoleRequest

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| action | string |  | Yes |
| object | string |  | Yes |

#### types.Preheat

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| create_at | string |  | No |
| id | string |  | No |
| status | string |  | No |

#### types.ResetPasswordRequest

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| new_password | string |  | Yes |
| old_password | string |  | Yes |

#### types.SignUpRequest

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| avatar | string |  | No |
| bio | string |  | No |
| email | string |  | Yes |
| location | string |  | No |
| name | string |  | Yes |
| password | string |  | Yes |
| phone | string |  | No |

#### types.UpdateCDNClusterRequest

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| bio | string |  | No |
| config | object |  | No |
| name | string |  | No |
| security_group_domain | string |  | No |

#### types.UpdateCDNRequest

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| cdn_cluster_id | integer |  | No |
| download_port | integer |  | No |
| idc | string |  | No |
| ip | string |  | No |
| location | string |  | No |
| port | integer |  | No |

#### types.UpdateOauthRequest

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| bio | string |  | No |
| client_id | string |  | No |
| client_secret | string |  | No |
| name | string |  | No |
| redirect_url | string |  | No |

#### types.UpdateSchedulerClusterRequest

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| bio | string |  | No |
| cdn_cluster_id | integer |  | No |
| client_config | object |  | No |
| config | object |  | No |
| is_default | boolean |  | No |
| name | string |  | No |
| scopes | object |  | No |
| security_group_domain | string |  | No |

#### types.UpdateSchedulerRequest

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| idc | string |  | No |
| ip | string |  | No |
| location | string |  | No |
| net_config | object |  | No |
| port | integer |  | No |
| scheduler_cluster_id | integer |  | No |
| scheduler_id | integer |  | No |
| vips | string |  | No |

#### types.UpdateSecurityGroupRequest

| Name | Type | Description | Required |
| ---- | ---- | ----------- | -------- |
| bio | string |  | No |
| domain | string |  | No |
| name | string |  | No |
| proxy_domain | string |  | No |