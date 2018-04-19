package main

import (
	"fmt"
	"hash/fnv"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/cmd/tsbs_generate_data/serialize"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

type hostnameIndexer struct {
	partitions int
}

func (i *hostnameIndexer) GetIndex(item *serialize.MongoPoint) int {
	t := &serialize.MongoTag{}
	for j := 0; j < item.TagsLength(); j++ {
		item.Tags(t, j)
		if string(t.Key()) == "hostname" {
			h := fnv.New32a()
			h.Write([]byte(string(t.Value())))
			return int(h.Sum32()) % i.partitions
		}
	}
	return -1
}

func scanConsistent(channels []*duplexChannel, itemsPerBatch int) int64 {
	return scanWithIndexer(channels, itemsPerBatch, &hostnameIndexer{partitions: len(channels)})
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
func processBatchesAggregate(wg *sync.WaitGroup, session *mgo.Session, dc *duplexChannel) {
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
	c := dc.toWorker

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

		dc.sendToScanner()
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
