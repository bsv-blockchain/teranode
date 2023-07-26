package base

type DbManagerBase interface {
	Connect() error
	Disconnect() error
}

type DbFactory interface {
	Create() DbManager
}

type DbFactories struct {
	Factories map[string]DbFactory
}

type CRUD interface {
}

type DbManager interface {
	DbManagerBase
	CRUD
}
