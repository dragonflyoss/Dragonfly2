package types

type SettingParams struct {
	Key string `uri:"key" binding:"required"`
}
type CreateSettingRequest struct {
	Key   string `json:"key" binding:"required"`
	Value string `json:"value" binding:"required"`
}

type UpdateSettingRequest struct {
	Key   string `json:"key" binding:"required"`
	Value string `json:"value" binding:"required"`
}

type GetSettingsQuery struct {
	Page    int `form:"page" binding:"omitempty,gte=1"`
	PerPage int `form:"per_page" binding:"omitempty,gte=1,lte=50"`
}
