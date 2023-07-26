package sqlite

import (
	u "github.com/ordishs/go-utils"
)

type SqliteManager struct {
	logger u.Logger
}

func (m *SqliteManager) Connect() error {
	m.logger.Debugf("Connect")
	return nil
}

func (m *SqliteManager) Disconnect() error {
	m.logger.Debugf("Disconnect")
	return nil
}
