package base

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
}

type DbManager interface {
	DbManagerBase
	CRUD
}
