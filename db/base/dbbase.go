package base

import "database/sql"

type DbManagerBase interface {
	Connect(db_config string) error
	Disconnect() error
}

type DbFactory interface {
	Create(db_config string) DbManager
}

type DbFactories struct {
	Factories map[string]DbFactory
}

type CRUD interface {
	Create(any) error
	Read(any) error
	Read_Cond(any, []any) (any, error)
	Read_All_Cond(any, []any) ([]any, error)
	Update(any) error
	Delete(any) error
	UpdateBatch(string, string, []interface{}, map[string]interface{}) error

	TxBegin(opts ...*sql.TxOptions) (any, error)
	TxRollback(any) error
	TxCommit(any) error
	TxCreate(any, any) error
	TxRead(any, any) error
	TxRead_Cond(any, any, []any) (any, error)
	TxRead_All_Cond(any, any, []any) ([]any, error)
	TxUpdate(any, any) error
	TxDelete(any, any) error
	TxUpdateBatch(any, string, string, []interface{}, map[string]interface{}) error
	TxSelectForUpdate(any, string, []interface{}) ([]any, error)
}

type DbManager interface {
	DbManagerBase
	CRUD
}
