package sqlite

import (
	"database/sql"
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

func (m *SqliteManager) GetDB() any {
	return m.db
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

func (m *SqliteManager) UpdateBatch(table string, cond string, values []interface{}, toupdate map[string]interface{}) error {
	//Table("users").Where("id IN ?", []int{10, 11}).Updates(map[string]interface{}{"name": "hello", "age": 18})
	// UPDATE users SET name='hello', age=18 WHERE id IN (10, 11);
	tx := m.db.Table(table).Where(cond, values...).Updates(toupdate)
	return tx.Error
}

func (m *SqliteManager) TxBegin(opts ...*sql.TxOptions) (any, error) {
	return m.db.Begin(opts...), nil
}

func (m *SqliteManager) TxCommit(i any) error {
	if i == nil {
		return nil
	}
	tx, ok := i.(*gorm.DB)
	if !ok {
		return errors.New("not a gorm database object")
	}
	tx.Commit()
	return nil
}

func (m *SqliteManager) TxRollback(i any) error {
	if i == nil {
		return nil
	}
	var tx *gorm.DB
	var ok bool
	if i == nil {
		tx = m.db
	} else {
		tx, ok = i.(*gorm.DB)
		if !ok {
			return errors.New("not a gorm database object")
		}
	}
	tx.Rollback()
	return nil
}

func (m *SqliteManager) TxUpdate(i any, model any) error {
	var tx *gorm.DB
	var ok bool
	if i == nil {
		tx = m.db
	} else {
		tx, ok = i.(*gorm.DB)
		if !ok {
			return errors.New("not a gorm database object")
		}
	}
	result := tx.Save(model)
	return result.Error
}

func (m *SqliteManager) TxDelete(i any, model any) error {
	var tx *gorm.DB
	var ok bool
	if i == nil {
		tx = m.db
	} else {
		tx, ok = i.(*gorm.DB)
		if !ok {
			return errors.New("not a gorm database object")
		}
	}
	gm, ok := model.(*gorm.Model)
	if ok && gm != nil {
		result := tx.Delete(model, gm.ID)
		return result.Error
	}
	return errors.New("not a gorm model based data structure")
}

func (m *SqliteManager) TxCreate(i any, model any) error {
	var tx *gorm.DB
	var ok bool
	if i == nil {
		tx = m.db
	} else {
		tx, ok = i.(*gorm.DB)
		if !ok {
			return errors.New("not a gorm database object")
		}
	}
	result := tx.Create(model)
	return result.Error
}

func (m *SqliteManager) TxRead(i any, model any) error {
	var tx *gorm.DB
	var ok bool
	if i == nil {
		tx = m.db
	} else {
		tx, ok = i.(*gorm.DB)
		if !ok {
			return errors.New("not a gorm database object")
		}
	}
	result := tx.Last(model)
	return result.Error
}

func (m *SqliteManager) TxRead_Cond(i any, model any, cond []any) (any, error) {
	var tx *gorm.DB
	var ok bool
	if i == nil {
		tx = m.db
	} else {
		tx, ok = i.(*gorm.DB)
		if !ok {
			return nil, errors.New("not a gorm database object")
		}
	}
	result := tx.Where(cond[0], cond[1:]...).Find(model)
	return result.Statement.Dest, result.Error
}

func (m *SqliteManager) TxSelectForUpdate(i any, stmt string, vals []interface{}) ([]any, error) {
	var tx *gorm.DB
	var ok bool
	if i == nil {
		tx = m.db
	} else {
		tx, ok = i.(*gorm.DB)
		if !ok {
			return nil, errors.New("not a gorm database object")
		}
	}
	rows, err := tx.Raw(stmt, vals...).Rows()
	if err != nil {
		m.logger.Errorf("failed to select tx: %v", err)
		return nil, err
	}
	defer rows.Close()
	payload := []any{}
	// ID: uint CreatedAt: time.Time UpdatedAt: time.Time DeletedAt: DeleteAt (gorm.io/gorm)
	// Txid: string Vout uint32 LockingScript: string Satoshis uint64 Address: string Spent bool Reserved bool
	utxo := &model.UTXO{}
	for rows.Next() {
		err = rows.Scan(&utxo.ID, &utxo.Txid, &utxo.Vout, &utxo.LockingScript, &utxo.Satoshis)
		if err != nil {
			m.logger.Errorf("failed to scan: %v", err)
			return nil, err
		}
		payload = append(payload, utxo)
	}

	return payload, nil
}

func (m *SqliteManager) TxRead_All_Cond(i any, model any, cond []any) ([]any, error) {
	var tx *gorm.DB
	var ok bool
	if i == nil {
		tx = m.db
	} else {
		tx, ok = i.(*gorm.DB)
		if !ok {
			return nil, errors.New("not a gorm database object")
		}
	}

	// SELECT FOR UPDATE... -> Rows-> rows.Next()
	batch_size := 100
	payload := []any{}
	result := tx.Where(cond[0], cond[1:]...).
		FindInBatches(model, batch_size,
			func(tx *gorm.DB, batch int) error {
				payload = append(payload, tx.Statement.Dest)
				return nil
			},
		)

	return payload, result.Error
}

func (m *SqliteManager) TxUpdateBatch(i any, model any, cond string, values []interface{}, toupdate map[string]interface{}) error {
	var tx *gorm.DB
	var ok bool
	if i == nil {
		tx = m.db
	} else {
		tx, ok = i.(*gorm.DB)
		if !ok {
			return errors.New("not a gorm database object")
		}
	}
	txr := tx.Model(model).Where(cond, values...).Updates(toupdate)
	return txr.Error
}
