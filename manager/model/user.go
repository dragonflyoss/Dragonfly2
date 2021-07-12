package model

const (
	UserStateEnabled  = "enable"
	UserStateDisabled = "disable"
)

type User struct {
	Model
	Email             string `gorm:"column:email;size:256;uniqueIndex;not null" json:"email"`
	Name              string `gorm:"column:name;size:256;uniqueIndex;not null" json:"name"`
	EncryptedPassword string `gorm:"column:encrypted_password;size:1024" json:"-"`
	Avatar            string `gorm:"column:avatar;size:256" json:"avatar"`
	Phone             string `gorm:"column:phone;size:256" json:"phone"`
	PrivateToken      string `gorm:"column:private_token;size:256" json:"private_token"`
	State             string `gorm:"type:enum('enable', 'disable');default:'enable'" json:"state"`
	Location          string `gorm:"column:location;size:1024" json:"location"`
	BIO               string `gorm:"column:bio;size:1024" json:"bio"`
}
