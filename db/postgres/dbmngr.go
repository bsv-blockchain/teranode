package postgres

import (
	"errors"

	u "github.com/ordishs/go-utils"
)

type PostgresManager struct {
	logger u.Logger
}

func (m *PostgresManager) Connect(db_config string) error {
	m.logger.Debugf("Connect")
	return nil
}

func (m *PostgresManager) Disconnect() error {
	m.logger.Debugf("Disconnect")
	return nil
}

func (m *PostgresManager) Create(model any) error {
	panic(errors.New("PostgresManager - Postgres not implemented"))
}

func (m *PostgresManager) Read(model any) error {
	return nil
}

func (m *PostgresManager) Read_All_Cond(model any, cond *[]string) error {
	return nil
}

func (m *PostgresManager) Update(model any) error {
	return nil
}

func (m *PostgresManager) Delete(model any) error {
	return nil
}
