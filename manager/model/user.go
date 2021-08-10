package model

const (
	UserStateEnabled  = "enable"
	UserStateDisabled = "disable"
)

type User struct {
	Model
	Email             string `gorm:"column:email;type:varchar(256);index:uk_user_email,unique;not null;comment:email address" json:"email"`
	Name              string `gorm:"column:name;type:varchar(256);index:uk_user_name,unique;not null;comment:name" json:"name"`
	EncryptedPassword string `gorm:"column:encrypted_password;not null;size:1024;comment:encrypted password" json:"-"`
	Avatar            string `gorm:"column:avatar;type:varchar(256);comment:avatar address" json:"avatar"`
	Phone             string `gorm:"column:phone;type:varchar(256);comment:phone number" json:"phone"`
	PrivateToken      string `gorm:"column:private_token;type:varchar(256);comment:private token" json:"private_token"`
	State             string `gorm:"column:state;type:varchar(256);default:'enable';comment:state" json:"state"`
	Location          string `gorm:"column:location;type:varchar(256);comment:location" json:"location"`
	BIO               string `gorm:"column:bio;type:varchar(256);comment:biography" json:"bio"`
}
