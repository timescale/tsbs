package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

type dbCreator struct {
	session *mgo.Session
}

func (d *dbCreator) Init() {
	var err error
	d.session, err = mgo.DialWithTimeout(daemonURL, writeTimeout)
	if err != nil {
		log.Fatal(err)
	}
	d.session.SetMode(mgo.Eventual, false)
}

func (d *dbCreator) DBExists(dbName string) bool {
	dbs, err := d.session.DatabaseNames()
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
	collections, err := d.session.DB(dbName).CollectionNames()
	if err != nil {
		return err
	}
	for _, name := range collections {
		d.session.DB(dbName).C(name).DropCollection()
	}

	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	cmd := make(bson.D, 0, 4)
	cmd = append(cmd, bson.DocElem{Name: "create", Value: collectionName})

	// wiredtiger settings
	cmd = append(cmd, bson.DocElem{
		Name: "storageEngine", Value: map[string]interface{}{
			"wiredTiger": map[string]interface{}{
				"configString": "block_compressor=snappy",
			},
		},
	})

	err := d.session.DB(dbName).Run(cmd, nil)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return nil
		}
		return fmt.Errorf("create collection err: %v", err)
	}

	collection := d.session.DB(dbName).C(collectionName)
	var key []string
	if documentPer {
		key = []string{"measurement", "tags.hostname", timestampField}
	} else {
		key = []string{aggKeyID, "measurement", "tags.hostname"}
	}

	index := mgo.Index{
		Key:        key,
		Unique:     false, // Unique does not work on the entire array of tags!
		Background: false,
		Sparse:     false,
	}
	err = collection.EnsureIndex(index)
	if err != nil {
		return fmt.Errorf("create basic index err: %v", err)
	}

	// To make updates for new records more efficient, we need a efficient doc
	// lookup index
	if !documentPer {
		err = collection.EnsureIndex(mgo.Index{
			Key:        []string{aggDocID},
			Unique:     false,
			Background: false,
			Sparse:     false,
		})
		if err != nil {
			return fmt.Errorf("create agg doc index err: %v", err)
		}
	}

	return nil
}

func (d *dbCreator) Close() {
	d.session.Close()
}
