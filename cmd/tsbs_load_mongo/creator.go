package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type dbCreator struct {
	client *mongo.Client
}

func (d *dbCreator) Init() {
	var err error
	opts := options.Client().ApplyURI(daemonURL).SetSocketTimeout(writeTimeout).SetRetryWrites(retryableWrites)
	d.client, err = mongo.Connect(context.Background(), opts)
	if err != nil {
		log.Fatal(err)
	}
}

func (d *dbCreator) DBExists(dbName string) bool {
	dbs, err := d.client.ListDatabaseNames(context.Background(), bson.D{})
	if err != nil {
		log.Fatal(err)
	}
	for _, name := range dbs {
		if name == dbName {
			return true
		}
	}
	return false
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	collections, err := d.client.Database(dbName).ListCollectionNames(context.Background(), bson.D{})
	if err != nil {
		return err
	}
	for _, name := range collections {
		d.client.Database(dbName).Collection(name).Drop(context.Background())
	}

	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	cmd := make(bson.D, 0, 4)
	cmd = append(cmd, bson.E{"create", collectionName})

	if timeseriesCollection {
		cmd = append(cmd, bson.E{"timeseries", bson.M{
			"timeField": timestampField,
			"metaField": "tags",
		}})
	}

	res := d.client.Database(dbName).RunCommand(context.Background(), cmd)
	if res.Err() != nil {
		if strings.Contains(res.Err().Error(), "already exists") {
			return nil
		}
		return fmt.Errorf("create collection err: %v", res.Err().Error())
	}

	// To make updates for new records more efficient, we need an efficient doc
	// lookup index
	if !documentPer {
		model := []mongo.IndexModel{
			{
				Keys: bson.D{{aggDocID, 1}},
			},
			{
				Keys: bson.D{{aggKeyID, 1}, {"measurement", 1}, {"tags.hostname", 1}},
			},
		}
		opts := options.CreateIndexes()
		_, err := d.client.Database(dbName).Collection(collectionName).Indexes().CreateMany(context.Background(), model, opts)
		if err != nil {
			return fmt.Errorf("create indexes err: %v", err.Error())
		}
	}

	return nil
}

func (d *dbCreator) Close() {
	d.client.Disconnect(context.Background())
}
