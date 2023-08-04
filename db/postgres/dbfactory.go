package postgres

import (
	"github.com/TAAL-GmbH/ubsv/db/base"
	u "github.com/ordishs/go-utils"
)

type PostgresFactory struct {
	logger u.Logger
}

func New(logger u.Logger) base.DbFactory {
	return &PostgresFactory{logger: logger}
}

func (f *PostgresFactory) Create(db_config string) base.DbManager {
	m := &PostgresManager{logger: f.logger}
	if err := m.Connect(db_config); err != nil {
		return nil
	}
	return m
}
