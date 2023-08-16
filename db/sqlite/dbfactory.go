package sqlite

import (
	"github.com/bitcoin-sv/ubsv/db/base"
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

func (f *SqliteFactory) Create(db_config string) base.DbManager {
	m := &SqliteManager{logger: f.logger}
	if err := m.Connect(db_config); err != nil {
		return nil
	}
	return m
}
