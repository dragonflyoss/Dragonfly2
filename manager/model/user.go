package model

const (
	UserStateEnabled  = "enable"
	UserStateDisabled = "disable"
)

type User struct {
	Model
	Email             string `gorm:"column:email;type:varchar(256);uniqueIndex;not null" json:"email"`
	Name              string `gorm:"column:name;type:varchar(256);uniqueIndex;not null" json:"name"`
	EncryptedPassword string `gorm:"column:encrypted_password;not null;size:1024" json:"-"`
	Avatar            string `gorm:"column:avatar;type:varchar(256)" json:"avatar"`
	Phone             string `gorm:"column:phone;type:varchar(256)" json:"phone"`
	PrivateToken      string `gorm:"column:private_token;type:varchar(256)" json:"private_token"`
	State             string `gorm:"column:state;type:varchar(256);default:'enable'" json:"state"`
	Location          string `gorm:"column:location;type:varchar(256)" json:"location"`
	BIO               string `gorm:"column:bio;type:varchar(256)" json:"bio"`
}
