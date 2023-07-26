package postgres

import (
	u "github.com/ordishs/go-utils"
)

type PostgresManager struct {
	logger u.Logger
}

func (m *PostgresManager) Connect() error {
	m.logger.Debugf("Connect")
	return nil
}

func (m *PostgresManager) Disconnect() error {
	m.logger.Debugf("Disconnect")
	return nil
}
