package handlers

// CreateCDNInstance godoc
// @Summary Add cdn instance
// @Description add by json config
// @Tags CDNInstance
// @Accept  json
// @Produce  json
// @Param instance body types.CDNInstance true "Cdn instance"
// @Success 200 {object} types.CDNInstance
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdn/instances [post]
// func (handler *Handlers) CreateCDNInstance(ctx *gin.Context) {
// var json types.CreateCDNInstanceRequest
// if err := ctx.ShouldBindJSON(&json); err != nil {
// ctx.Error(err)
// return
// }

// cdnInstance, err := handler.server.AddCDNInstance(context.TODO(), &json)
// if err != nil {
// ctx.Error(err)
// return
// }

// ctx.JSON(http.StatusOK, cdnInstance)
// }

// DestroyCDNInstance godoc
// @Summary Delete cdn instance
// @Description Delete by instanceId
// @Tags CDNInstance
// @Accept  json
// @Produce  json
// @Param  id path string true "InstanceID"
// @Success 200 {string} string
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdn/instances/{id} [delete]
// func (handler *Handlers) DestroyCDNInstance(ctx *gin.Context) {
// var params types.CDNInstanceParams
// if err := ctx.ShouldBindUri(&params); err != nil {
// ctx.Error(err)
// return
// }

// cdnInstance, err := handler.server.DeleteCDNInstance(context.TODO(), params.ID)
// if err != nil {
// ctx.Error(err)
// return
// }

// ctx.JSON(http.StatusOK, cdnInstance)
// }

// UpdateCDNInstance godoc
// @Summary Update cdn instance
// @Description Update by json cdn instance
// @Tags CDNInstance
// @Accept  json
// @Produce  json
// @Param  id path string true "InstanceID"
// @Param  Instance body types.CDNInstance true "CDNInstance"
// @Success 200 {string} string
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdn/instances/{id} [post]
// func (handler *Handlers) UpdateCDNInstance(ctx *gin.Context) {
// var params types.CDNInstanceParams
// if err := ctx.ShouldBindUri(&params); err != nil {
// ctx.Error(err)
// return
// }

// var json types.UpdateCDNInstanceRequest
// if err := ctx.ShouldBindJSON(&json); err != nil {
// ctx.Error(err)
// return
// }

// cdnInstance, err := handler.server.UpdateCDNInstance(context.TODO(), params.ID, json)
// if err != nil {
// ctx.Error(err)
// return
// }

// ctx.JSON(http.StatusOK, cdnInstance)
// }

// GetCDNInstance godoc
// @Summary Get cdn instance
// @Description Get cdn instance by InstanceID
// @Tags CDNInstance
// @Accept  json
// @Produce  json
// @Param id path string true "InstanceID"
// @Success 200 {object} types.CDNInstance
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdn/instances/{id} [get]
// func (handler *Handlers) GetCDNInstance(ctx *gin.Context) {
// var params types.CDNInstanceParams
// if err := ctx.ShouldBindUri(&params); err != nil {
// ctx.Error(err)
// return
// }

// cdnInstance, err := handler.server.GetCDNInstance(context.TODO(), params.ID)
// if err == nil {
// ctx.Error(err)
// }

// ctx.JSON(http.StatusOK, cdnInstance)
// }

// GetCDNInstances godoc
// @Summary List cdn instances
// @Description List by object
// @Tags CDNInstance
// @Accept  json
// @Produce  json
// @Param marker query int true "begin marker of current page" default(0)
// @Param maxItemCount query int true "return max item count, default 10, max 50" default(10) minimum(10) maximum(50)
// @Success 200 {object} types.ListCDNInstancesResponse
// @Failure 400 {object} HTTPError
// @Failure 404 {object} HTTPError
// @Failure 500 {object} HTTPError
// @Router /cdn/instances [get]
// func (handler *Handlers) GetCDNInstances(ctx *gin.Context) {
// var query types.GetCDNsQuery

// query.Page = 1
// query.PerPage = 10

// if err := ctx.ShouldBindQuery(&query); err != nil {
// ctx.Error(err)
// return
// }

// cdnInstances, err := handler.server.ListCDNInstances(context.TODO(), store.WithMarker(query.Marker, query.MaxItemCount))
// if err != nil {
// ctx.Error(err)
// }

// TODO(Gaius) Add pagination link header
// ctx.JSON(http.StatusOK, cdnInstances)
// }
