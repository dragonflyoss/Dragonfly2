package types

type PolicyRequest struct {
	Subject string `form:"subject" binding:"required,min=1"`
	Object  string `form:"object" binding:"required,min=1"`
	Action  string `form:"action" binding:"required,min=1"`
}

type Policy struct {
	Method   string `json:"method"`
	Resource string `json:"resource"`
}

type Policys []Policy
