// bulk_load_mongo loads a Mongo daemon with data from stdin.
//
// Any existing collections in the database will be removed.
package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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
	channels := []chan []*serialize.MongoPoint{}
	if documentPer {
		channels = append(channels, make(chan []*serialize.MongoPoint, workers))
		closerFn = func() { close(channels[0]) }
		workerFn = func(wg *sync.WaitGroup, _ int) {
			go processBatchesPerEvent(wg, session, channels[0])
		}
	} else {
		// To avoid workers overlapping on the documents they are working on,
		// we have one channel per worker so we can uniformly & consistently
		// spread the workload across workers in a non-overlapping fashion.
		for i := 0; i < workers; i++ {
			channels = append(channels, make(chan []*serialize.MongoPoint, 1))
		}
		closerFn = func() {
			for i := 0; i < workers; i++ {
				close(channels[i])
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

// scan reads length-delimited flatbuffers items from stdin.
func scan(channels []chan []*serialize.MongoPoint, itemsPerBatch int) int64 {
	var itemsRead int64
	r := bufio.NewReaderSize(os.Stdin, 1<<20)
	lenBuf := make([]byte, 8)

	b := make([]*serialize.MongoPoint, 0)
	batchChan := channels[0]
	for {
		if itemsRead == limit {
			break
		}
		item := &serialize.MongoPoint{}

		_, err := r.Read(lenBuf)
		if err == io.EOF {
			break
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

		b = append(b, item)

		itemsRead++
		if len(b) >= itemsPerBatch {
			batchChan <- b
			b = make([]*serialize.MongoPoint, 0)
		}
	}
	// Finished reading input, make sure last batch goes out.
	if len(b) > 0 {
		batchChan <- b
	}

	return itemsRead
}

func scanConsistent(channels []chan []*serialize.MongoPoint, itemsPerBatch int) int64 {
	var itemsRead int64
	r := bufio.NewReaderSize(os.Stdin, 1<<20)
	lenBuf := make([]byte, 8)

	batches := make([][]*serialize.MongoPoint, workers)
	hash := func(s string) int {
		h := fnv.New32a()
		h.Write([]byte(s))
		return int(h.Sum32())
	}
	for {
		if itemsRead == limit {
			break
		}

		item := &serialize.MongoPoint{}

		_, err := r.Read(lenBuf)
		if err == io.EOF {
			break
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

		// TODO - This is not portable across use cases
		var idx int
		t := &serialize.MongoTag{}
		for j := 0; j < item.TagsLength(); j++ {
			item.Tags(t, j)
			if string(t.Key()) == "hostname" {
				idx = hash(string(t.Value())) % workers
				break
			}
		}
		batches[idx] = append(batches[idx], item)

		itemsRead++
		if len(batches[idx]) >= itemsPerBatch {
			channels[idx] <- batches[idx]
			batches[idx] = batches[idx][:0]
		}
	}
	// Finished reading input, make sure last batch goes out.
	for i, val := range batches {
		if len(val) > 0 {
			channels[i] <- val
		}
	}

	return itemsRead
}

type singlePoint struct {
	Measurement string                 `bson:"measurement"`
	Timestamp   int64                  `bson:"timestamp_ns"`
	Fields      map[string]interface{} `bson:"fields"`
	Tags        map[string]string      `bson:"tags"`
}

var spPool = &sync.Pool{New: func() interface{} { return &singlePoint{} }}

// processBatchesPerEvent creates a new document for each incoming event for a simpler
// approach to storing the data. This is _NOT_ the default since the aggregation method
// is recommended by Mongo and other blogs
func processBatchesPerEvent(wg *sync.WaitGroup, session *mgo.Session, c chan []*serialize.MongoPoint) {
	var sess *mgo.Session
	var db *mgo.Database
	var collection *mgo.Collection
	if doLoad {
		sess = session.Copy()
		db = sess.DB(dbName)
		collection = db.C(collectionName)
	}

	pvs := []interface{}{}
	for batch := range c {
		//start := time.Now()
		if cap(pvs) < len(batch) {
			pvs = make([]interface{}, len(batch))
		}
		pvs = pvs[:len(batch)]

		for i, event := range batch {
			x := spPool.Get().(*singlePoint)

			x.Measurement = string(event.MeasurementName())
			x.Timestamp = event.Timestamp()
			x.Fields = map[string]interface{}{}
			x.Tags = map[string]string{}
			f := &serialize.MongoReading{}
			for j := 0; j < event.FieldsLength(); j++ {
				event.Fields(f, j)
				x.Fields[string(f.Key())] = f.Value()
			}
			t := &serialize.MongoTag{}
			for j := 0; j < event.TagsLength(); j++ {
				event.Tags(t, j)
				x.Tags[string(t.Key())] = string(t.Value())
			}
			pvs[i] = x
			atomic.AddUint64(&metricCount, uint64(event.FieldsLength()))
		}

		if doLoad {
			bulk := collection.Bulk()
			bulk.Insert(pvs...)
			_, err := bulk.Run()
			if err != nil {
				log.Fatalf("Bulk insert docs err: %s\n", err.Error())
			}
		}
		for _, p := range pvs {
			spPool.Put(p)
		}
	}
	wg.Done()
}

// point is a reusable data structure to store a BSON data document for Mongo,
// that can then be manipulated for bookkeeping and final document preparation
type point struct {
	Timestamp int64                  `bson:"timestamp_ns"`
	Fields    map[string]interface{} `bson:"fields"`
}

var emptyDoc [][]bson.M

func generateEmptyHourDoc() {
	emptyDoc = make([][]bson.M, 60)
	for j := range emptyDoc {
		emptyDoc[j] = make([]bson.M, 60)
	}
}

var pPool = &sync.Pool{New: func() interface{} { return &point{} }}

// processBatchesAggregate receives batches of bson.M documents (BSON maps) that
// each correspond to a datapoint and puts the points in the appropriate aggregated
// document. Documents are aggregated on a per-sensor, per-hour basis, meaning
// each document can hold up to 3600 readings (one per second) that only need
// to be updated after initial creation (when the new per-sensor, per-host combination
// is first encountered)
//
// A document is structured like so:
//  {
//    "doc_id": "day_x_00",
//    "key_id": "x_00",
//    "measurement": "cpu",
//    "tags": {
//      "hostname": "host0",
//      ...
//    },
//    "events": [
//      [
//        {
//          "field1": 0.0,
//          ...
//		  }
//      ]
//    ]
//  }
func processBatchesAggregate(wg *sync.WaitGroup, session *mgo.Session, c chan []*serialize.MongoPoint) {
	var sess *mgo.Session
	var db *mgo.Database
	var collection *mgo.Collection
	if doLoad {
		sess = session.Copy()
		db = sess.DB(dbName)
		collection = db.C(collectionName)
	}
	var createdDocs = make(map[string]bool)
	var createQueue = []interface{}{}

	for batch := range c {
		docToEvents := make(map[string][]*point)

		eventCnt := uint64(0)
		for _, event := range batch {
			tagsMap := map[string]string{}
			t := &serialize.MongoTag{}
			for j := 0; j < event.TagsLength(); j++ {
				event.Tags(t, j)
				tagsMap[string(t.Key())] = string(t.Value())
			}

			// Determine which document this event belongs too
			ts := event.Timestamp()
			dateKey := time.Unix(0, ts).UTC().Format(aggDateFmt)
			docKey := fmt.Sprintf("day_%s_%s", tagsMap["hostname"], dateKey)

			// Check that it has been created using a cached map, if not, add
			// to creation queue
			_, ok := createdDocs[docKey]
			if !ok {
				if _, ok := createdDocs[docKey]; !ok {
					createQueue = append(createQueue, bson.M{
						aggDocID:      docKey,
						aggKeyID:      dateKey,
						"measurement": string(event.MeasurementName()),
						"tags":        tagsMap,
						"events":      emptyDoc,
					})
				}
				createdDocs[docKey] = true
			}

			// Cache events to be updated on a per-document basis for efficient
			// batching later
			if _, ok := docToEvents[docKey]; !ok {
				docToEvents[docKey] = []*point{}
			}
			x := pPool.Get().(*point)
			x.Fields = map[string]interface{}{}
			f := &serialize.MongoReading{}
			for j := 0; j < event.FieldsLength(); j++ {
				event.Fields(f, j)
				x.Fields[string(f.Key())] = f.Value()
			}
			x.Timestamp = ts
			eventCnt += uint64(len(x.Fields))

			docToEvents[docKey] = append(docToEvents[docKey], x)
		}

		if doLoad {
			// Checks if any new documents need to be made and does so
			bulk := collection.Bulk()
			bulk = insertNewAggregateDocs(collection, bulk, createQueue)
			createQueue = createQueue[:0]

			// For each document, create one 'set' command for all records
			// that belong to the document
			for docKey, events := range docToEvents {
				selector := bson.M{aggDocID: docKey}
				updateMap := bson.M{}
				for _, event := range events {
					minKey := (event.Timestamp / (1e9 * 60)) % 60
					secKey := (event.Timestamp / 1e9) % 60
					key := fmt.Sprintf("events.%d.%d", minKey, secKey)
					val := event.Fields

					val[timestampField] = event.Timestamp
					updateMap[key] = val
				}

				update := bson.M{"$set": updateMap}
				bulk.Update(selector, update)
			}

			// All documents accounted for, finally run the operation
			_, err := bulk.Run()
			if err != nil {
				log.Fatalf("Bulk aggregate update err: %s\n", err.Error())
			}

			for _, events := range docToEvents {
				for _, e := range events {
					delete(e.Fields, timestampField)
					pPool.Put(e)
				}
			}
		}
		// Update count of metrics inserted
		atomic.AddUint64(&metricCount, eventCnt)
	}
	wg.Done()
}

// insertNewAggregateDocs handles creating new aggregated documents when new devices
// or time periods are encountered
func insertNewAggregateDocs(collection *mgo.Collection, bulk *mgo.Bulk, createQueue []interface{}) *mgo.Bulk {
	b := bulk
	if len(createQueue) > 0 {
		off := 0
		for off < len(createQueue) {
			l := off + aggInsertBatchSize
			if l > len(createQueue) {
				l = len(createQueue)
			}

			b.Insert(createQueue[off:l]...)
			_, err := b.Run()
			if err != nil {
				log.Fatalf("Bulk aggregate docs err: %s\n", err.Error())
			}
			b = collection.Bulk()

			off = l
		}
	}

	return b
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
