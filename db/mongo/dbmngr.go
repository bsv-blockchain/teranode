package mongo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	u "github.com/ordishs/go-utils"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type MongoManager struct {
	dbclient *mongo.Client
	dbcoll   *mongo.Collection
	logger   u.Logger
}

const (
	//MONGODB_URI          = "mongodb://lunadb:lunapwd@localhost:27017/?maxPoolSize=20&w=majority"
	STATS_COLLECTION     = "txstats"
	MONGODB_URI_TEMPLATE = "mongodb://%s:%s@%s:%s/?maxPoolSize=20&w=majority"
)

func (m *MongoManager) Connect(db_config string) error {
	// We assume two separate criteria are to be present to enable stats collection
	// and writing to mongodb: 1. stats option to enable stats 2. env vars to point
	// to a working/accessable mongodb instance
	var (
		username, passwd, database, db_ip, db_port string
		flag                                       bool
	)

	if username, flag = os.LookupEnv("MONGO_INITDB_ROOT_USERNAME"); !flag {
		m.logger.Errorf("Env var %s not set", "MONGO_INITDB_ROOT_USERNAME")
		return nil
	}
	if passwd, flag = os.LookupEnv("MONGO_INITDB_ROOT_PASSWORD"); !flag {
		m.logger.Errorf("Env var %s not set", "MONGO_INITDB_ROOT_PASSWORD")
		return nil
	}
	if database, flag = os.LookupEnv("MONGO_INITDB_DATABASE"); !flag {
		m.logger.Errorf("Env var %s not set", "MONGO_INITDB_DATABASE")
		return nil
	}
	if db_ip, flag = os.LookupEnv("MONGO_INITDB_IP"); !flag {
		m.logger.Errorf("Env var %s not set", "MONGO_INITDB_IP")
		return nil
	}
	if db_port, flag = os.LookupEnv("MONGO_INITDB_PORT"); !flag {
		m.logger.Errorf("Env var %s not set", "MONGO_INITDB_PORT")
		return nil
	}

	MONGODB_URI := fmt.Sprintf(MONGODB_URI_TEMPLATE, username, passwd, db_ip, db_port)

	client, err := mongo.Connect(
		context.TODO(),
		options.Client().ApplyURI(MONGODB_URI),
	)
	if err != nil {
		m.logger.Errorf("Connect to %s failed. %s", MONGODB_URI, err)
		return err
	}
	// If the connection is established, wait for ping for no more than 5 sec
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		m.logger.Errorf("Failed to ping %s. %v", MONGODB_URI, err)
		return err
	}

	coll := client.Database(database).Collection(STATS_COLLECTION)
	m.logger.Infof("[SUCCESS] Connected and pinged the db")
	m.dbclient, m.dbcoll = client, coll
	return nil
}

func (m *MongoManager) Disconnect() error {
	if m != nil && m.dbclient != nil {
		if err := m.dbclient.Disconnect(context.Background()); err != nil {
			m.logger.Errorf("Failed to disconnect from db. %v", err)
			return err
		}
	}
	return nil
}

func (m *MongoManager) Create(model any) error {
	panic(errors.New("MongoManager - Mongodb not implemented"))
}

func (m *MongoManager) Read(model any) error {
	return nil
}

func (m *MongoManager) Read_All_Cond(model any, cond *[]string) error {
	return nil
}

func (m *MongoManager) Update(model any) error {
	return nil
}

func (m *MongoManager) Delete(model any) error {
	return nil
}
