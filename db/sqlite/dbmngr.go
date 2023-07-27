package sqlite

import (
	u "github.com/ordishs/go-utils"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type SqliteManager struct {
	db     *gorm.DB
	logger u.Logger
}

func (m *SqliteManager) Connect() error {
	m.logger.Debugf("Connecting to sqlite...")
	db, err := gorm.Open(sqlite.Open("con.db"), &gorm.Config{})
	if err != nil {
		m.logger.Errorf("Failed to open sqlite: %s", err.Error())
		return err
	}
	m.logger.Debugf("Successfully connected to sqlite. %+v", db)
	sqldb, _ := db.DB()
	if err := sqldb.Ping(); err != nil {
		m.logger.Errorf("Failed to ping sqlite: %s", err.Error())
		return err
	}
	m.logger.Debugf("sqlite stats: %s", sqldb.Stats())
	m.db = db
	return nil
}

func (m *SqliteManager) Disconnect() error {
	m.logger.Debugf("Disconnecting from sqlite...")
	if m.db != nil {
		d, e := m.db.DB()
		if e == nil {
			d.Close()
			m.logger.Debugf("Successfully disconnected from sqlite")
		} else {
			m.logger.Errorf("Failed to disconnect from sqlite: %s", e.Error())
			return e
		}
	}
	return nil
}
