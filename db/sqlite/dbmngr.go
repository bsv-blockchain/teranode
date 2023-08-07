package sqlite

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/TAAL-GmbH/ubsv/db/model"
	u "github.com/ordishs/go-utils"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type SqliteManager struct {
	db     *gorm.DB
	logger u.Logger
}

func (m *SqliteManager) Connect(db_config string) error {
	m.logger.Debugf("Connecting to sqlite: %s", db_config)
	var dsn string
	if strings.Contains(db_config, ":memory:") {
		dsn = db_config
	} else {
		uhdir, err := os.UserHomeDir()
		if err != nil {
			m.logger.Errorf("cannot find user home directory: %s", err.Error())
			return err
		}
		data_path := filepath.Join(uhdir, "data")
		if _, err := os.Stat(data_path); os.IsNotExist(err) {
			if err := os.Mkdir(data_path, 0755); os.IsExist(err) {
				dsn = filepath.Join(data_path, db_config)
			} else {
				m.logger.Errorf("cannot create data directory: %s", err.Error())
				dsn = db_config
			}
		} else {
			dsn = filepath.Join(data_path, db_config)
		}
	}
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
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
	m.logger.Debugf("sqlite stats: %+v", sqldb.Stats())

	// Check table for `Block` exists or not
	if !db.Migrator().HasTable(&model.Block{}) {
		err = db.Migrator().CreateTable(&model.Block{})
		if err != nil {
			m.logger.Errorf("Failed to create table: %s", err.Error())
			return err
		}
	}

	// Check table for `UTXO` exists or not
	if !db.Migrator().HasTable(&model.UTXO{}) {
		err = db.Migrator().CreateTable(&model.UTXO{})
		if err != nil {
			m.logger.Errorf("Failed to create table: %s", err.Error())
			return err
		}
	}

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

func (m *SqliteManager) Create(model any) error {
	result := m.db.Create(model)
	return result.Error
}

func (m *SqliteManager) Read(model any) error {
	result := m.db.Last(model)
	return result.Error
}

func (m *SqliteManager) Read_Cond(model any, cond []any) (any, error) {
	result := m.db.Where(cond[0], cond[1:]...).Find(model)
	return result.Statement.Dest, result.Error
}

func (m *SqliteManager) Read_All_Cond(model any, cond []any) ([]any, error) {
	batch_size := 100
	payload := []any{}
	result := m.db.Where(cond[0], cond[1:]...).
		FindInBatches(model, batch_size,
			func(tx *gorm.DB, batch int) error {
				payload = append(payload, tx.Statement.Dest)
				return nil
			},
		)

	return payload, result.Error
}

func (m *SqliteManager) Update(model any) error {
	result := m.db.Save(model)
	return result.Error
}

func (m *SqliteManager) Delete(model any) error {
	gm, ok := model.(*gorm.Model)
	if ok && gm != nil {
		result := m.db.Delete(model, gm.ID)
		return result.Error
	}
	return errors.New("not a gorm model based data structure")
}
