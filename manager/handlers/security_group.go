package handlers

// CreateSecurityGroup godoc
// @Summary Add security group
// @Description add by json config
// @Tags SecurityGroup
// @Accept  json
// @Produce  json
// @Param instance body types.CreateSecurityGroupRequest true "security group"
// @Success 200 {object} types.SchedulerInstance
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /security-group [post]
// func (handler *Handlers) CreateSecurityGroup(ctx *gin.Context) {
// var json types.CreateSecurityGroupRequest
// if err := ctx.ShouldBindJSON(&json); err != nil {
// ctx.Error(err)
// return
// }

// securityGroup, err := handler.server.AddSchedulerInstance(context.TODO(), &json)
// if err != nil {
// ctx.Error(err)
// return
// }

// ctx.JSON(http.StatusOK, securityGroup)
// }

// DestroySecurityGroup godoc
// @Summary Delete security group
// @Description Delete by ID
// @Tags SchedulerInstance
// @Accept  json
// @Produce  json
// @Param  id path string true "ID"
// @Success 200 {string} string
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /security-group/{id} [delete]
// func (handler *Handlers) DestroySecurityGroup(ctx *gin.Context) {
// var params types.SecurityGroupParams
// if err := ctx.ShouldBindUri(&params); err != nil {
// ctx.Error(err)
// return
// }

// securityGroup, err := handler.server.DeleteSchedulerInstance(context.TODO(), params.ID)
// if err != nil {
// ctx.Error(err)
// return
// }

// ctx.JSON(http.StatusOK, securityGroup)
// }

// UpdateSecurityGroup godoc
// @Summary Update security group
// @Description Update by json security group
// @Tags SecurityGroup
// @Accept  json
// @Produce  json
// @Param  id path string true "ID"
// @Param  Instance body types.UpdateSecurityGroupRequest true "SecurityGroup"
// @Success 200 {string} string
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /security-group/{id} [patch]
// func (handler *Handlers) UpdateSecurityGroup(ctx *gin.Context) {
// var params types.SecurityGroupParams
// if err := ctx.ShouldBindUri(&params); err != nil {
// ctx.Error(err)
// return
// }

// var json types.UpdateSecurityGroupRequest
// if err := ctx.ShouldBindJSON(&json); err != nil {
// ctx.Error(err)
// return
// }

// securityGroup, err := handler.server.UpdateSchedulerInstance(context.TODO(), params.ID, json)
// if err != nil {
// ctx.Error(err)
// return
// }

// ctx.JSON(http.StatusOK, securityGroup)
// }

// GetSecurityGroup godoc
// @Summary Get security group
// @Description Get security group by ID
// @Tags SecurityGroup
// @Accept  json
// @Produce  json
// @Param id path string true "ID"
// @Success 200 {object} types.SecurityGroup
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /security-group/{id} [get]
// func (handler *Handlers) GetSecurityGroup(ctx *gin.Context) {
// var params types.SecurityGroupParams
// if err := ctx.ShouldBindUri(&params); err != nil {
// ctx.Error(err)
// return
// }

// securityGroup, err := handler.server.GetSchedulerInstance(context.TODO(), params.ID)
// if err != nil {
// ctx.Error(err)
// }

// ctx.JSON(http.StatusOK, securityGroup)
// }

// GetSecurityGroups godoc
// @Summary List scheduler instances
// @Description List by object
// @Tags SchedulerInstance
// @Accept  json
// @Produce  json
// @Param marker query int true "begin marker of current page" default(0)
// @Param maxItemCount query int true "return max item count, default 10, max 50" default(10) minimum(10) maximum(50)
// @Success 200 {object} types.ListSchedulerInstancesResponse
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /scheduler/instances [get]
// func (handler *Handlers) GetSecurityGroups(ctx *gin.Context) {
// var query types.GetSecurityGroupsQuery
// if err := ctx.ShouldBindQuery(&query); err != nil {
// ctx.Error(err)
// return
// }

// securityGroup, err := handler.server.ListSchedulerInstances(context.TODO(), store.WithMarker(query.Marker, query.MaxItemCount))
// if err != nil {
// ctx.Error(err)
// return
// }

// ctx.JSON(http.StatusOK, securityGroup)
// }
