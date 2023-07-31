package base

import "gorm.io/gorm"

type DbManagerBase interface {
	Connect() error
	Disconnect() error
}

type DbFactory interface {
	Create() DbManager
}

type DbFactories struct {
	Factories map[string]DbFactory
}

type CRUD interface {
	Create(*gorm.Model) error
	Read(*gorm.Model) error
	Update(*gorm.Model) error
	Delete(*gorm.Model) error
}

type DbManager interface {
	DbManagerBase
	CRUD
}
