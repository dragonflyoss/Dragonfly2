package types

type PolicyRequest struct {
	Subject string `form:"subject" binding:"required,min=1"`
	Object  string `form:"object" binding:"required,min=1"`
	Action  string `form:"aciton" binding:"required,oneof=read write"`
}

type PermissionGroups []string

type UserRolesParams struct {
	UserName string `uri:"userName" binding:"required"`
}

type UserHasRoleParams struct {
	UserName string `uri:"userName" binding:"required"`
	Object   string `uri:"object" binding:"required"`
	Action   string `uri:"action" binding:"required,oneof=read write"`
}
