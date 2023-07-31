package postgres

import (
	"github.com/TAAL-GmbH/ubsv/db/base"
	u "github.com/ordishs/go-utils"
)

type PostgresFactory struct {
	logger u.Logger
}

func (f *PostgresFactory) Create() base.DbManager {
	m := &PostgresManager{logger: f.logger}
	if err := m.Connect(); err != nil {
		return nil
	}
	return m
}
