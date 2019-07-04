package main

import (
	"fmt"
	"hash/fnv"
	"log"
	"sync"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
	"github.com/timescale/tsbs/load"
)

type hostnameIndexer struct {
	partitions uint
}

func (i *hostnameIndexer) GetIndex(item *load.Point) int {
	p := item.Data.(*serialize.MongoPoint)
	t := &serialize.MongoTag{}
	for j := 0; j < p.TagsLength(); j++ {
		p.Tags(t, j)
		key := string(t.Key())
		if key == "hostname" || key == "name" {
			// the hostame is the defacto index for devops tags
			// the truck name is the defacto index for iot tags
			h := fnv.New32a()
			h.Write([]byte(string(t.Value())))
			return int(h.Sum32()) % int(i.partitions)
		}
	}
	// name tag may be skipped in iot use-case
	return 0
}

// aggBenchmark allows you to run a benchmark using the aggregated document format
// for Mongo
type aggBenchmark struct {
	mongoBenchmark
}

func newAggBenchmark(l *load.BenchmarkRunner) *aggBenchmark {
	// Pre-create the needed empty subdoc for new aggregate docs
	generateEmptyHourDoc()

	return &aggBenchmark{mongoBenchmark{l, &dbCreator{}}}
}

func (b *aggBenchmark) GetProcessor() load.Processor {
	return &aggProcessor{dbc: b.dbc}
}

func (b *aggBenchmark) GetPointIndexer(maxPartitions uint) load.PointIndexer {
	return &hostnameIndexer{partitions: maxPartitions}
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

type aggProcessor struct {
	dbc        *dbCreator
	collection *mgo.Collection

	createdDocs map[string]bool
	createQueue []interface{}
}

func (p *aggProcessor) Init(workerNum int, doLoad bool) {
	if doLoad {
		sess := p.dbc.session.Copy()
		db := sess.DB(loader.DatabaseName())
		p.collection = db.C(collectionName)
	}
	p.createdDocs = make(map[string]bool)
	p.createQueue = []interface{}{}

}

// ProcessBatch receives a batch of bson.M documents (BSON maps) that
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
func (p *aggProcessor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	docToEvents := make(map[string][]*point)
	batch := b.(*batch)

	eventCnt := uint64(0)
	for _, event := range batch.arr {
		tagsMap := map[string]string{}
		t := &serialize.MongoTag{}
		for j := 0; j < event.TagsLength(); j++ {
			event.Tags(t, j)
			tagsMap[string(t.Key())] = string(t.Value())
		}

		// Determine which document this event belongs too
		ts := event.Timestamp()
		dateKey := time.Unix(0, ts).UTC().Format(aggDateFmt)
		docKey := fmt.Sprintf("day_%s_%s_%s", tagsMap["hostname"], dateKey, string(event.MeasurementName()))

		// Check that it has been created using a cached map, if not, add
		// to creation queue
		_, ok := p.createdDocs[docKey]
		if !ok {
			if _, ok := p.createdDocs[docKey]; !ok {
				p.createQueue = append(p.createQueue, bson.M{
					aggDocID:      docKey,
					aggKeyID:      dateKey,
					"measurement": string(event.MeasurementName()),
					"tags":        tagsMap,
					"events":      emptyDoc,
				})
			}
			p.createdDocs[docKey] = true
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
		bulk := p.collection.Bulk()
		bulk = insertNewAggregateDocs(p.collection, bulk, p.createQueue)
		p.createQueue = p.createQueue[:0]

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
	return eventCnt, 0
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
