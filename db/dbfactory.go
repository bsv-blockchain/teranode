package db

import (
	"github.com/TAAL-GmbH/ubsv/db/base"
	"github.com/TAAL-GmbH/ubsv/db/mongo"
	"github.com/TAAL-GmbH/ubsv/db/postgres"
	"github.com/TAAL-GmbH/ubsv/db/sqlite"
	"github.com/ordishs/gocore"
)

var factories *base.DbFactories

func init() {
	factories = initDbFactories()
}

func initDbFactories() *base.DbFactories {
	logLevel, _ := gocore.Config().Get("logLevel")
	logger := gocore.Log("ubsv.db", gocore.NewLogLevelFromString(logLevel))
	return &base.DbFactories{
		Factories: map[string]base.DbFactory{
			"postgres": postgres.New(logger),
			"mongo":    mongo.New(logger),
			"sqlite":   sqlite.New(logger),
		},
	}
}

func Create(db_type string) base.DbManager {
	val, ok := factories.Factories[db_type]
	if ok {
		return val.Create()
	}
	return nil
}
