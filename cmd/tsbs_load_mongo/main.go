// bulk_load_mongo loads a Mongo daemon with data from stdin.
//
// Any existing collections in the database will be removed.
package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/cmd/tsbs_generate_data/serialize"
	"bitbucket.org/440-labs/influxdb-comparisons/load"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	flatbuffers "github.com/google/flatbuffers/go"
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
	daemonURL       string
	dbName          string
	workers         int
	batchSize       int
	limit           int64
	documentPer     bool
	doLoad          bool
	writeTimeout    time.Duration
	reportingPeriod time.Duration
)

// Global vars
var (
	metricCount = uint64(0)
)

// Parse args:
func init() {
	flag.StringVar(&daemonURL, "url", "localhost:27017", "Mongo URL.")
	flag.StringVar(&dbName, "db-name", "benchmark", "Name of database to store data")

	flag.IntVar(&batchSize, "batch-size", 10000, "Batch size (input items).")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")
	flag.Int64Var(&limit, "limit", -1, "Number of items to insert (default unlimited).")
	flag.DurationVar(&writeTimeout, "write-timeout", 10*time.Second, "Write timeout.")
	flag.DurationVar(&reportingPeriod, "reporting-period", 10*time.Second, "Period to report write stats")

	flag.BoolVar(&documentPer, "document-per-event", false, "Whether to use one document per event or aggregate by hour")
	flag.BoolVar(&doLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed.")

	flag.Parse()
}

func main() {
	var session *mgo.Session
	if doLoad {
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

	var closerFn func()
	var workerFn func(*sync.WaitGroup, int)
	channels := []*duplexChannel{}
	if documentPer {
		channels = append(channels, newDuplexChannel(workers))
		closerFn = func() { channels[0].close() }
		workerFn = func(wg *sync.WaitGroup, _ int) {
			go processBatchesPerEvent(wg, session, channels[0])
		}
	} else {
		// To avoid workers overlapping on the documents they are working on,
		// we have one channel per worker so we can uniformly & consistently
		// spread the workload across workers in a non-overlapping fashion.
		for i := 0; i < workers; i++ {
			channels = append(channels, newDuplexChannel(1))
		}
		closerFn = func() {
			for i := 0; i < workers; i++ {
				channels[i].close()
			}
		}
		workerFn = func(wg *sync.WaitGroup, i int) {
			go processBatchesAggregate(wg, session, channels[i])
		}

		// Pre-create the needed empty subdoc for new aggregate docs
		generateEmptyHourDoc()
	}

	scanFn := func() (int64, int64) {
		if documentPer {
			return scan(channels, batchSize), 0
		}
		return scanConsistent(channels, batchSize), 0
	}

	dr := load.NewDataReader(workers, workerFn, scanFn)
	dr.Start(reportingPeriod, closerFn, &metricCount, nil)
	dr.Summary(workers, &metricCount, nil)
}

func decodeMongoPoint(r *bufio.Reader, lenBuf []byte) *serialize.MongoPoint {
	item := &serialize.MongoPoint{}

	_, err := r.Read(lenBuf)
	if err == io.EOF {
		return nil
	}
	if err != nil {
		log.Fatal(err.Error())
	}

	// ensure correct len of receiving buffer
	l := int(binary.LittleEndian.Uint64(lenBuf))
	itemBuf := make([]byte, l)

	// read the bytes and init the flatbuffer object
	totRead := 0
	for totRead < l {
		m, err := r.Read(itemBuf[totRead:])
		// (EOF is also fatal)
		if err != nil {
			log.Fatal(err.Error())
		}
		totRead += m
	}
	if totRead != len(itemBuf) {
		panic(fmt.Sprintf("reader/writer logic error, %d != %d", totRead, len(itemBuf)))
	}
	n := flatbuffers.GetUOffsetT(itemBuf)
	item.Init(itemBuf, n)

	return item
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
	collections, err := session.DB(dbName).CollectionNames()
	if err != nil {
		log.Fatal(err)
	}
	for _, name := range collections {
		session.DB(dbName).C(name).DropCollection()
	}
}
