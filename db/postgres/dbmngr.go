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

func (m *PostgresManager) Read_Cond(model any, cond []any) (any, error) {
	return nil, nil
}

func (m *PostgresManager) Read_All_Cond(model any, cond []any) ([]any, error) {
	return nil, nil
}

func (m *PostgresManager) Update(model any) error {
	return nil
}

func (m *PostgresManager) Delete(model any) error {
	return nil
}

func (m *PostgresManager) UpdateBatch(table string, cond string, values []interface{}, updates map[string]interface{}) error {
	return nil
}

func (m *PostgresManager) TxBegin() (any, error) {
	return nil, nil
}

func (m *PostgresManager) TxCommit(any) error {
	return nil
}

func (m *PostgresManager) TxRollback(any) error {
	return nil
}

func (m *PostgresManager) TxCreate(i any, model any) error {
	panic(errors.New("PostgresManager - Postgres not implemented"))
}

func (m *PostgresManager) TxRead(i any, model any) error {
	return nil
}

func (m *PostgresManager) TxRead_Cond(i any, model any, cond []any) (any, error) {
	return nil, nil
}

func (m *PostgresManager) TxRead_All_Cond(i any, model any, cond []any) ([]any, error) {
	return nil, nil
}

func (m *PostgresManager) TxUpdate(i any, model any) error {
	return nil
}

func (m *PostgresManager) TxDelete(i any, model any) error {
	return nil
}

func (m *PostgresManager) TxUpdateBatch(i any, table string, cond string, values []interface{}, updates map[string]interface{}) error {
	return nil
}
