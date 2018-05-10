// bulk_load_mongo loads a Mongo daemon with data from stdin.
//
// Any existing collections in the database will be removed.
package main

import (
	"flag"
	"log"
	"strings"
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/load"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

const (
	collectionName     = "point_data"
	aggDocID           = "doc_id"
	aggDateFmt         = "20060102_15" // see Go docs for how we arrive at this time format
	aggKeyID           = "key_id"
	aggInsertBatchSize = 500 // found via trial-and-error
	timestampField     = "timestamp_ns"
)

// Program option vars:
var (
	daemonURL    string
	documentPer  bool
	writeTimeout time.Duration
)

// Global vars
var (
	metricCount uint64
	loader      *load.BenchmarkRunner
)

// Parse args:
func init() {
	loader = load.GetBenchmarkRunner()

	flag.StringVar(&daemonURL, "url", "localhost:27017", "Mongo URL.")
	flag.DurationVar(&writeTimeout, "write-timeout", 10*time.Second, "Write timeout.")
	flag.BoolVar(&documentPer, "document-per-event", false, "Whether to use one document per event or aggregate by hour")

	flag.Parse()
}

func main() {
	var session *mgo.Session
	if loader.DoLoad() && loader.DoInit() {
		var err error
		session, err = mgo.DialWithTimeout(daemonURL, writeTimeout)
		if err != nil {
			log.Fatal(err)
		}
		session.SetMode(mgo.Eventual, false)
		defer session.Close()

		cleanupCollections(session)
		createCollection(session, collectionName)
	}

	var benchmark load.Benchmark
	if documentPer {
		benchmark = newNaiveBenchmark(loader, session)
	} else {
		// Pre-create the needed empty subdoc for new aggregate docs
		generateEmptyHourDoc()

		benchmark = newAggBenchmark(loader, session)
	}

	loader.RunBenchmark(benchmark, &metricCount, nil)
}

func createCollection(session *mgo.Session, collectionName string) {
	cmd := make(bson.D, 0, 4)
	cmd = append(cmd, bson.DocElem{"create", collectionName})

	// wiredtiger settings
	cmd = append(cmd, bson.DocElem{
		"storageEngine", map[string]interface{}{
			"wiredTiger": map[string]interface{}{
				"configString": "block_compressor=snappy",
			},
		},
	})

	dbName := loader.DatabaseName()
	err := session.DB(dbName).Run(cmd, nil)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return
		}
		log.Fatalf("Create collection err: %v\n", err)
	}

	collection := session.DB(dbName).C(collectionName)
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
		log.Fatalf("Create basic index err: %v\n", err)
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
			log.Fatalf("Create agg doc index err: %v\n", err)
		}
	}
}

func cleanupCollections(session *mgo.Session) {
	dbName := loader.DatabaseName()
	collections, err := session.DB(dbName).CollectionNames()
	if err != nil {
		log.Fatal(err)
	}
	for _, name := range collections {
		session.DB(dbName).C(name).DropCollection()
	}
}
