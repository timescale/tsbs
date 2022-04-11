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
	createCollCmd := make(bson.D, 0, 4)
	createCollCmd = append(createCollCmd, bson.E{"create", collectionName})

	if timeseriesCollection {
		createCollCmd = append(createCollCmd, bson.E{"timeseries", bson.M{
			"timeField": timestampField,
			"metaField": "tags",
		}})
	}

	createCollRes := d.client.Database(dbName).RunCommand(context.Background(), createCollCmd)

	if createCollRes.Err() != nil {
		if strings.Contains(createCollRes.Err().Error(), "already exists") {
			return nil
		}
		return fmt.Errorf("create collection err: %v", createCollRes.Err().Error())
	}

	if collectionSharded {
	        // first enable sharding on dbName
		enableShardingCmd := make(bson.D, 0, 4)
		enableShardingCmd = append(enableShardingCmd, bson.E{"enableSharding", dbName})
		
	        renableShardingRes :=
			d.client.Database("admin").RunCommand(context.Background(), enableShardingCmd)
	        if renableShardingRes.Err() != nil {
			return fmt.Errorf("enableSharding err: %v", renableShardingRes.Err().Error())
		}

		// then shard the collection
		shardCollCmd := make(bson.D, 0, 4)
		shardCollCmd = append(shardCollCmd, bson.E{"shardCollection",dbName+"."+collectionName})
		var shardKey interface{}
		
		err := bson.UnmarshalExtJSON([]byte(shardKeySpec), true, &shardKey)
		if err != nil {
		   err = bson.UnmarshalExtJSON([]byte("{\"time\":1}"), true, &shardKey)		       
		}
		shardCollCmd = append(shardCollCmd, bson.E{"key", shardKey})

		if numInitChunks > 0 {
		   	shardCollCmd = append(shardCollCmd, bson.E{"numInitialChunks", numInitChunks})
		}
	   	shardCollRes := d.client.Database("admin").RunCommand(context.Background(), shardCollCmd)
	
		if shardCollRes.Err() != nil {
		        return fmt.Errorf("shard collection err: %v", shardCollRes.Err().Error())
	        }

		balancerCmd := make(bson.D, 0, 4)
		if balancerOn {
		        balancerCmd = append(balancerCmd, bson.E{"balancerStart", 1})		
		} else {
		        balancerCmd = append(balancerCmd, bson.E{"balancerStop", 1})		
		}
	        balancerRes := d.client.Database("admin").RunCommand(context.Background(), balancerCmd)
	        if balancerRes.Err() != nil {
			return fmt.Errorf("balancerStart/Stop err: %v", balancerRes.Err().Error())
		}
	}

 	var model []mongo.IndexModel
	if documentPer {
		model = []mongo.IndexModel{
			{
				Keys: bson.D{{"tags.hostname", 1}, {"time", -1}},
			},
		}
	} else {
		// To make updates for new records more efficient, we need an efficient doc
		// lookup index
		model = []mongo.IndexModel{
			{
				Keys: bson.D{{aggDocID, 1}},
			},
			{
				Keys: bson.D{{aggKeyID, 1}, {"measurement", 1}, {"tags.hostname", 1}},
			},
		}
	}
	opts := options.CreateIndexes()
	_, err := d.client.Database(dbName).Collection(collectionName).Indexes().CreateMany(context.Background(), model, opts)
	if err != nil {
		return fmt.Errorf("create indexes err: %v", err.Error())
	}

	return nil
}

func (d *dbCreator) Close() {
	d.client.Disconnect(context.Background())
}
