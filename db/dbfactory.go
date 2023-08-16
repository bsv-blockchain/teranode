package db

import (
	"github.com/bitcoin-sv/ubsv/db/base"
	"github.com/bitcoin-sv/ubsv/db/mongo"
	"github.com/bitcoin-sv/ubsv/db/postgres"
	"github.com/bitcoin-sv/ubsv/db/sqlite"
	"github.com/ordishs/gocore"
)

var factories *base.DbFactories

func init() {
	factories = initDbFactories()
}

func initDbFactories() *base.DbFactories {
	logger := gocore.Log("ubsv.db")
	return &base.DbFactories{
		Factories: map[string]base.DbFactory{
			"postgres": postgres.New(logger),
			"mongo":    mongo.New(logger),
			"sqlite":   sqlite.New(logger),
		},
	}
}

func Create(db_type string, db_config string) base.DbManager {
	val, ok := factories.Factories[db_type]
	if ok {
		return val.Create(db_config)
	}
	return nil
}
