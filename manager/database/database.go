package database

import "gorm.io/gorm"

type Database struct {
	*gorm.DB
}

func New() *Database {
	return nil
}
