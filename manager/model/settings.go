package model

type Settings struct {
	ID    uint   `gorm:"primarykey;comment:id" json:"id"`
	Key   string `gorm:"column:key;type:varchar(256);index:uk_settings_key,unique;not null;comment:setting key" json:"key"`
	Value string `gorm:"column:value;type:varchar(256);not null;comment:setting value" json:"value"`
}
