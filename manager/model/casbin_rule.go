package model

type CasbinRule struct {
	ID    uint   `gorm:"primaryKey;autoIncrement;comment:id"`
	Ptype string `gorm:"type:varchar(100);default:NULL;uniqueIndex:uk_casbin_rule;comment:policy type"`
	V0    string `gorm:"type:varchar(100);default:NULL;uniqueIndex:uk_casbin_rule;comment:v0"`
	V1    string `gorm:"type:varchar(100);default:NULL;uniqueIndex:uk_casbin_rule;comment:v1"`
	V2    string `gorm:"type:varchar(100);default:NULL;uniqueIndex:uk_casbin_rule;comment:v2"`
	V3    string `gorm:"type:varchar(100);default:NULL;uniqueIndex:uk_casbin_rule;comment:v3"`
	V4    string `gorm:"type:varchar(100);default:NULL;uniqueIndex:uk_casbin_rule;comment:v4"`
	V5    string `gorm:"type:varchar(100);default:NULL;uniqueIndex:uk_casbin_rule;comment:v5"`
}
