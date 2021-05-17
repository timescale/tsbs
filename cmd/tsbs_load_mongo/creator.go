package main

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"log"
	"strings"
)

type dbCreator struct {
	client *mongo.Client
	ctx    context.Context
	cancel context.CancelFunc
}

func (d *dbCreator) Init() {
	//log.Println("tsbs_load_mongo/creator/Init")
	var err error
	d.ctx, d.cancel = context.WithTimeout(context.Background(), writeTimeout)
	d.client, err = mongo.Connect(d.ctx, options.Client().ApplyURI(daemonURL))
	if err != nil {
		log.Println("Can't establish connection with", daemonURL)
		log.Fatal(err)
		}
	}
}

func (d *dbCreator) DBExists(dbName string) bool {
	//log.Println("tsbs_load_mongo/creator/DBExists")
	dbs, err := d.client.ListDatabaseNames(d.ctx, bson.D{})
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
	//log.Println("tsbs_load_mongo/creator/RemoveOldDB")
	collection_names, err := d.client.Database(dbName).ListCollectionNames(d.ctx, bson.D{})
	log.Printf("collection_names : %s", collection_names)
	if err != nil {
		return err
	}
	for _, coll := range collection_names {
		log.Printf("collection found :  %s", coll)
		log.Println("deleting the previous collection")
		err := d.client.Database(dbName).Collection(coll).Drop(d.ctx)
		if err != nil {
			log.Printf("Could not delete collection : %s", err.Error())
		}
	}
	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	//Starting in MongoDB 3.2, the WiredTiger storage engine is the default storage engine
	err := d.client.Database(dbName).CreateCollection(d.ctx, collectionName)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			log.Printf("collection %s already exists", dbName)
			return nil
		}
		log.Printf("create collection err: %v", err)
		return fmt.Errorf("create collection err: %v", err)
	}
	collection := d.client.Database(dbName).Collection(collectionName)
	var key bson.D
	if documentPer {
		key = bson.D{{"measurement", 1}, {"tags.hostname", 1}, {timestampField, 1}}
	} else {
		key = bson.D{{aggKeyID, 1}, {"measurement", 1}, {"tags.hostname", 1}}
	}
	index := mongo.IndexModel{
		Keys:    key,
		Options: options.Index().SetName("default_index"),
	}
	idxview := collection.Indexes()
	_, err = idxview.CreateOne(d.ctx, index)
	if err != nil {
		log.Printf("create index err: %v", err)
		panic(err)
	}
	// To make updates for new records more efficient, we need a efficient doc
	// lookup index
	if !documentPer {
		_, err := idxview.CreateOne(d.ctx, mongo.IndexModel{
			Keys:    bson.D{{aggDocID, 1}},
			Options: options.Index().SetName("doc_lookup_index"),
		})
		if err != nil {
			log.Printf("create index err: %v", err)
			panic(err)
		}
	}
	return nil
}

func (d *dbCreator) Close() {
	//closing the database connection here
	//causes an error in the bulk loading
}
