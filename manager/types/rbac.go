package types

type PolicyRequest struct {
	Subject string `form:"subject" binding:"required,min=1"`
	Role    string `form:"role" binding:"required,min=1"`
}

type PermissionGroups []string

type UserRolesParams struct {
	UserName string `uri:"userName" binding:"required"`
}

type UserHasRoleParams struct {
	UserName string `uri:"userName" binding:"required"`
	Role     string `uri:"role" binding:"required"`
}
