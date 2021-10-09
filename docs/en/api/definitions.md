
<a name="definitions"></a>
## Definitions

<a name="model-assertion"></a>
### model.Assertion

|Name|Schema|
|---|---|
|**key**  <br>*optional*|string|
|**policy**  <br>*optional*|< < string > array > array|
|**policyMap**  <br>*optional*|< string, integer > map|
|**rm**  <br>*optional*|[rbac.RoleManager](#rbac-rolemanager)|
|**tokens**  <br>*optional*|< string > array|
|**value**  <br>*optional*|string|


<a name="model-assertionmap"></a>
### model.AssertionMap
*Type* : < string, [model.Assertion](#model-assertion) > map


<a name="model-cdn"></a>
### model.CDN

|Name|Schema|
|---|---|
|**cdnclusterID**  <br>*optional*|integer|
|**download_port**  <br>*optional*|integer|
|**host_name**  <br>*optional*|string|
|**idc**  <br>*optional*|string|
|**ip**  <br>*optional*|string|
|**location**  <br>*optional*|string|
|**port**  <br>*optional*|integer|
|**status**  <br>*optional*|string|


<a name="model-cdncluster"></a>
### model.CDNCluster

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**config**  <br>*optional*|[model.JSONMap](#model-jsonmap)|
|**is_default**  <br>*optional*|boolean|
|**name**  <br>*optional*|string|
|**securityGroupID**  <br>*optional*|integer|


<a name="model-jsonmap"></a>
### model.JSONMap
*Type* : object


<a name="model-oauth"></a>
### model.Oauth

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**client_id**  <br>*optional*|string|
|**client_secret**  <br>*optional*|string|
|**name**  <br>*optional*|string|
|**redirect_url**  <br>*optional*|string|


<a name="model-scheduler"></a>
### model.Scheduler

|Name|Schema|
|---|---|
|**host_name**  <br>*optional*|string|
|**idc**  <br>*optional*|string|
|**ip**  <br>*optional*|string|
|**location**  <br>*optional*|string|
|**net_config**  <br>*optional*|[model.JSONMap](#model-jsonmap)|
|**port**  <br>*optional*|integer|
|**schedulerClusterID**  <br>*optional*|integer|
|**status**  <br>*optional*|string|
|**vips**  <br>*optional*|string|


<a name="model-schedulercluster"></a>
### model.SchedulerCluster

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**client_config**  <br>*optional*|[model.JSONMap](#model-jsonmap)|
|**config**  <br>*optional*|[model.JSONMap](#model-jsonmap)|
|**is_default**  <br>*optional*|boolean|
|**name**  <br>*optional*|string|
|**scopes**  <br>*optional*|[model.JSONMap](#model-jsonmap)|
|**securityGroupID**  <br>*optional*|integer|


<a name="model-securitygroup"></a>
### model.SecurityGroup

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**domain**  <br>*optional*|string|
|**name**  <br>*optional*|string|
|**proxy_domain**  <br>*optional*|string|


<a name="model-user"></a>
### model.User

|Name|Schema|
|---|---|
|**avatar**  <br>*optional*|string|
|**bio**  <br>*optional*|string|
|**email**  <br>*optional*|string|
|**location**  <br>*optional*|string|
|**name**  <br>*optional*|string|
|**phone**  <br>*optional*|string|
|**private_token**  <br>*optional*|string|
|**state**  <br>*optional*|string|


<a name="rbac-permission"></a>
### rbac.Permission

|Name|Schema|
|---|---|
|**action**  <br>*required*|string|
|**object**  <br>*required*|string|


<a name="rbac-rolemanager"></a>
### rbac.RoleManager
*Type* : object


<a name="types-addpermissionforrolerequest"></a>
### types.AddPermissionForRoleRequest

|Name|Schema|
|---|---|
|**action**  <br>*required*|string|
|**object**  <br>*required*|string|


<a name="types-createcdnclusterrequest"></a>
### types.CreateCDNClusterRequest

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**config**  <br>*required*|object|
|**name**  <br>*required*|string|
|**security_group_domain**  <br>*optional*|string|


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


<a name="types-createoauthrequest"></a>
### types.CreateOauthRequest

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**client_id**  <br>*required*|string|
|**client_secret**  <br>*required*|string|
|**name**  <br>*required*|string|
|**redirect_url**  <br>*optional*|string|


<a name="types-createpreheatrequest"></a>
### types.CreatePreheatRequest

|Name|Schema|
|---|---|
|**filter**  <br>*optional*|string|
|**headers**  <br>*optional*|< string, string > map|
|**scheduler_cluster_id**  <br>*optional*|integer|
|**type**  <br>*required*|string|
|**url**  <br>*required*|string|


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
|**client_config**  <br>*required*|object|
|**config**  <br>*required*|object|
|**is_default**  <br>*optional*|boolean|
|**name**  <br>*required*|string|
|**scopes**  <br>*optional*|object|
|**security_group_domain**  <br>*optional*|string|


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
|**domain**  <br>*required*|string|
|**name**  <br>*required*|string|
|**proxy_domain**  <br>*optional*|string|


<a name="types-deletepermissionforrolerequest"></a>
### types.DeletePermissionForRoleRequest

|Name|Schema|
|---|---|
|**action**  <br>*required*|string|
|**object**  <br>*required*|string|


<a name="types-preheat"></a>
### types.Preheat

|Name|Schema|
|---|---|
|**create_at**  <br>*optional*|string|
|**id**  <br>*optional*|string|
|**status**  <br>*optional*|string|


<a name="types-resetpasswordrequest"></a>
### types.ResetPasswordRequest

|Name|Schema|
|---|---|
|**new_password**  <br>*required*|string|
|**old_password**  <br>*required*|string|


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


<a name="types-updatecdnclusterrequest"></a>
### types.UpdateCDNClusterRequest

|Name|Schema|
|---|---|
|**bio**  <br>*optional*|string|
|**config**  <br>*optional*|object|
|**name**  <br>*optional*|string|
|**security_group_domain**  <br>*optional*|string|


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
|**client_config**  <br>*optional*|object|
|**config**  <br>*optional*|object|
|**is_default**  <br>*optional*|boolean|
|**name**  <br>*optional*|string|
|**scopes**  <br>*optional*|object|
|**security_group_domain**  <br>*optional*|string|


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
|**domain**  <br>*optional*|string|
|**name**  <br>*optional*|string|
|**proxy_domain**  <br>*optional*|string|



