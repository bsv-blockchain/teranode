package sqlite

import (
	"github.com/TAAL-GmbH/ubsv/db/base"
	u "github.com/ordishs/go-utils"
)

type SqliteFactory struct {
	logger u.Logger
}

func New(logger u.Logger) base.DbFactory {
	return &SqliteFactory{
		logger: logger,
	}
}

func (f *SqliteFactory) Create() base.DbManager {
	m := &SqliteManager{logger: f.logger}
	if err := m.Connect(); err != nil {
		return nil
	}
	return m
}
