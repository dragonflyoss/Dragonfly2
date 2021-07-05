package types

type LoginRequest struct {
	Name     string `form:"name" binding:"required"`
	Password string `form:"password" binding:"required"`
}

type RegisterRequest struct {
	Name     string `form:"name" binding:"required"`
	Password string `form:"password" binding:"required"`
	Email    string `form:"email" binding:"required"`
	Phone    string `form:"phone" binding:"omitempty"`
	Avatar   string `form:"avatar" binding:"omitempty"`
	Location string `form:"location" binding:"omitempty"`
	Bio      string `form:"bio" binding:"omitempty"`
}
