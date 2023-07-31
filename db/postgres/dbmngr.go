package postgres

import (
	u "github.com/ordishs/go-utils"
	"gorm.io/gorm"
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

func (m *PostgresManager) Create(model *gorm.Model) error {
	return nil
}

func (m *PostgresManager) Read(model *gorm.Model) error {
	return nil
}

func (m *PostgresManager) Update(model *gorm.Model) error {
	return nil
}

func (m *PostgresManager) Delete(model *gorm.Model) error {
	return nil
}
