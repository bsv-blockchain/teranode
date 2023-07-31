package mongo

import (
	"github.com/TAAL-GmbH/ubsv/db/base"
	u "github.com/ordishs/go-utils"
)

type MongoFactory struct {
	logger u.Logger
}

func (f *MongoFactory) Create() base.DbManager {
	m := &MongoManager{logger: f.logger}
	if err := m.Connect(); err != nil {
		return nil
	}
	return m
}
