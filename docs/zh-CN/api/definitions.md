
<a name="definitions"></a>
## 定义

<a name="model-assertion"></a>
### model.Assertion

|名称|类型|
|---|---|
|**key**  <br>*可选*|string|
|**policy**  <br>*可选*|< < string > array > array|
|**policyMap**  <br>*可选*|< string, integer > map|
|**rm**  <br>*可选*|[rbac.RoleManager](#rbac-rolemanager)|
|**tokens**  <br>*可选*|< string > array|
|**value**  <br>*可选*|string|


<a name="model-assertionmap"></a>
### model.AssertionMap
*类型* : < string, [model.Assertion](#model-assertion) > map


<a name="model-cdn"></a>
### model.CDN

|名称|类型|
|---|---|
|**cdnclusterID**  <br>*可选*|integer|
|**download_port**  <br>*可选*|integer|
|**host_name**  <br>*可选*|string|
|**idc**  <br>*可选*|string|
|**ip**  <br>*可选*|string|
|**location**  <br>*可选*|string|
|**port**  <br>*可选*|integer|
|**status**  <br>*可选*|string|


<a name="model-cdncluster"></a>
### model.CDNCluster

|名称|类型|
|---|---|
|**bio**  <br>*可选*|string|
|**config**  <br>*可选*|[model.JSONMap](#model-jsonmap)|
|**is_default**  <br>*可选*|boolean|
|**name**  <br>*可选*|string|
|**securityGroupID**  <br>*可选*|integer|


<a name="model-jsonmap"></a>
### model.JSONMap
*类型* : object


<a name="model-oauth"></a>
### model.Oauth

|名称|类型|
|---|---|
|**bio**  <br>*可选*|string|
|**client_id**  <br>*可选*|string|
|**client_secret**  <br>*可选*|string|
|**name**  <br>*可选*|string|
|**redirect_url**  <br>*可选*|string|


<a name="model-scheduler"></a>
### model.Scheduler

|名称|类型|
|---|---|
|**host_name**  <br>*可选*|string|
|**idc**  <br>*可选*|string|
|**ip**  <br>*可选*|string|
|**location**  <br>*可选*|string|
|**net_config**  <br>*可选*|[model.JSONMap](#model-jsonmap)|
|**port**  <br>*可选*|integer|
|**schedulerClusterID**  <br>*可选*|integer|
|**status**  <br>*可选*|string|
|**vips**  <br>*可选*|string|


<a name="model-schedulercluster"></a>
### model.SchedulerCluster

|名称|类型|
|---|---|
|**bio**  <br>*可选*|string|
|**client_config**  <br>*可选*|[model.JSONMap](#model-jsonmap)|
|**config**  <br>*可选*|[model.JSONMap](#model-jsonmap)|
|**is_default**  <br>*可选*|boolean|
|**name**  <br>*可选*|string|
|**scopes**  <br>*可选*|[model.JSONMap](#model-jsonmap)|
|**securityGroupID**  <br>*可选*|integer|


<a name="model-securitygroup"></a>
### model.SecurityGroup

|名称|类型|
|---|---|
|**bio**  <br>*可选*|string|
|**domain**  <br>*可选*|string|
|**name**  <br>*可选*|string|
|**proxy_domain**  <br>*可选*|string|


<a name="model-user"></a>
### model.User

|名称|类型|
|---|---|
|**avatar**  <br>*可选*|string|
|**bio**  <br>*可选*|string|
|**email**  <br>*可选*|string|
|**location**  <br>*可选*|string|
|**name**  <br>*可选*|string|
|**phone**  <br>*可选*|string|
|**private_token**  <br>*可选*|string|
|**state**  <br>*可选*|string|


<a name="rbac-permission"></a>
### rbac.Permission

|名称|类型|
|---|---|
|**action**  <br>*必填*|string|
|**object**  <br>*必填*|string|


<a name="rbac-rolemanager"></a>
### rbac.RoleManager
*类型* : object


<a name="types-addpermissionforrolerequest"></a>
### types.AddPermissionForRoleRequest

|名称|类型|
|---|---|
|**action**  <br>*必填*|string|
|**object**  <br>*必填*|string|


<a name="types-createcdnclusterrequest"></a>
### types.CreateCDNClusterRequest

|名称|类型|
|---|---|
|**bio**  <br>*可选*|string|
|**config**  <br>*必填*|object|
|**name**  <br>*必填*|string|
|**security_group_domain**  <br>*可选*|string|


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


<a name="types-createoauthrequest"></a>
### types.CreateOauthRequest

|名称|类型|
|---|---|
|**bio**  <br>*可选*|string|
|**client_id**  <br>*必填*|string|
|**client_secret**  <br>*必填*|string|
|**name**  <br>*必填*|string|
|**redirect_url**  <br>*可选*|string|


<a name="types-createpreheatrequest"></a>
### types.CreatePreheatRequest

|名称|类型|
|---|---|
|**filter**  <br>*可选*|string|
|**headers**  <br>*可选*|< string, string > map|
|**scheduler_cluster_id**  <br>*可选*|integer|
|**type**  <br>*必填*|string|
|**url**  <br>*必填*|string|


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
|**client_config**  <br>*必填*|object|
|**config**  <br>*必填*|object|
|**is_default**  <br>*可选*|boolean|
|**name**  <br>*必填*|string|
|**scopes**  <br>*可选*|object|
|**security_group_domain**  <br>*可选*|string|


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
|**domain**  <br>*必填*|string|
|**name**  <br>*必填*|string|
|**proxy_domain**  <br>*可选*|string|


<a name="types-deletepermissionforrolerequest"></a>
### types.DeletePermissionForRoleRequest

|名称|类型|
|---|---|
|**action**  <br>*必填*|string|
|**object**  <br>*必填*|string|


<a name="types-preheat"></a>
### types.Preheat

|名称|类型|
|---|---|
|**create_at**  <br>*可选*|string|
|**id**  <br>*可选*|string|
|**status**  <br>*可选*|string|


<a name="types-resetpasswordrequest"></a>
### types.ResetPasswordRequest

|名称|类型|
|---|---|
|**new_password**  <br>*必填*|string|
|**old_password**  <br>*必填*|string|


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
|**config**  <br>*可选*|object|
|**name**  <br>*可选*|string|
|**security_group_domain**  <br>*可选*|string|


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
|**client_config**  <br>*可选*|object|
|**config**  <br>*可选*|object|
|**is_default**  <br>*可选*|boolean|
|**name**  <br>*可选*|string|
|**scopes**  <br>*可选*|object|
|**security_group_domain**  <br>*可选*|string|


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
|**domain**  <br>*可选*|string|
|**name**  <br>*可选*|string|
|**proxy_domain**  <br>*可选*|string|



