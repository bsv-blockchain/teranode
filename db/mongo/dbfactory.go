package mongo

import (
	"github.com/TAAL-GmbH/ubsv/db/base"
	u "github.com/ordishs/go-utils"
)

type MongoFactory struct {
	logger u.Logger
}

func New(logger u.Logger) base.DbFactory {
	return &MongoFactory{logger: logger}
}

func (f *MongoFactory) Create(db_config string) base.DbManager {
	m := &MongoManager{logger: f.logger}
	if err := m.Connect(db_config); err != nil {
		return nil
	}
	return m
}
