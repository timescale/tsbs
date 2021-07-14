package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/targets"
	tsbsMongo "github.com/timescale/tsbs/pkg/targets/mongo"
)

type hostnameIndexer struct {
	partitions uint
}

func (i *hostnameIndexer) GetIndex(item data.LoadedPoint) uint {
	p := item.Data.(*tsbsMongo.MongoPoint)
	t := &tsbsMongo.MongoTag{}
	for j := 0; j < p.TagsLength(); j++ {
		p.Tags(t, j)
		key := string(t.Key())
		if key == "hostname" || key == "name" {
			// the hostame is the defacto index for devops tags
			// the truck name is the defacto index for iot tags
			h := fnv.New32a()
			h.Write([]byte(string(t.Value())))
			return uint(h.Sum32()) % i.partitions
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

func newAggBenchmark(l load.BenchmarkRunner, conf *load.BenchmarkRunnerConfig) *aggBenchmark {
	// Pre-create the needed empty subdoc for new aggregate docs
	generateEmptyHourDoc()

	return &aggBenchmark{mongoBenchmark{conf.FileName, l, &dbCreator{}}}
}

func (b *aggBenchmark) GetProcessor() targets.Processor {
	return &aggProcessor{dbc: b.dbc}
}

func (b *aggBenchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	return &hostnameIndexer{partitions: maxPartitions}
}

// point is a reusable data structure to store a BSON data document for Mongo,
// that can then be manipulated for bookkeeping and final document preparation
type point struct {
	Timestamp time.Time              `bson:"timestamp_ns"`
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
	collection *mongo.Collection

	createdDocs map[string]bool
	createQueue []interface{}
}

func (p *aggProcessor) Init(_ int, doLoad, _ bool) {
	if doLoad {
		p.collection = p.dbc.client.Database(loader.DatabaseName()).Collection(collectionName)
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
func (p *aggProcessor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	docToEvents := make(map[string][]*point)
	batch := b.(*batch)

	eventCnt := uint64(0)
	for _, event := range batch.arr {
		tagsSlice := bson.D{}
		tagsMap := map[string]string{}
		t := &tsbsMongo.MongoTag{}
		for j := 0; j < event.TagsLength(); j++ {
			event.Tags(t, j)
			tagsMap[string(t.Key())] = string(t.Value())
			tagsSlice = append(tagsSlice, bson.E{string(t.Key()), string(t.Value())})
		}

		// Determine which document this event belongs too
		ts := time.Unix(0, event.Timestamp())
		dateKey := ts.UTC().Format(aggDateFmt)
		docKey := fmt.Sprintf("day_%s_%s_%s", tagsMap["hostname"], dateKey, string(event.MeasurementName()))

		// Check that it has been created using a cached map, if not, add
		// to creation queue
		_, ok := p.createdDocs[docKey]
		if !ok {
			if _, ok := p.createdDocs[docKey]; !ok {
				if randomFieldOrder {
					p.createQueue = append(p.createQueue, bson.M{
						aggDocID:      docKey,
						aggKeyID:      dateKey,
						"measurement": string(event.MeasurementName()),
						"tags":        tagsMap,
						"events":      emptyDoc,
					})
				} else {
					p.createQueue = append(p.createQueue, bson.D{
						{aggDocID,      docKey},
						{aggKeyID,      dateKey},
						{"measurement", string(event.MeasurementName())},
						{"tags",        tagsSlice},
						{"events",      emptyDoc},
					})
				}
				
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
		f := &tsbsMongo.MongoReading{}
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
		insertNewAggregateDocs(p.collection, p.createQueue)
		models := make([]mongo.WriteModel, len(docToEvents))
		p.createQueue = p.createQueue[:0]

		// For each document, create one 'set' command for all records
		// that belong to the document
		i := 0
		for docKey, events := range docToEvents {
			selector := bson.M{aggDocID: docKey}
			updateMap := bson.M{}
			for _, event := range events {
				minKey := event.Timestamp.Minute()
				secKey := event.Timestamp.Second()
				key := fmt.Sprintf("events.%d.%d", minKey, secKey)
				val := event.Fields

				val[timestampField] = event.Timestamp
				updateMap[key] = val
			}

			models[i] = mongo.NewUpdateOneModel().SetFilter(selector).SetUpdate(bson.M{"$set": updateMap})
			i++
		}

		// All documents accounted for, finally run the operation
		opts := options.BulkWrite().SetOrdered(orderedInserts)
		_, err := p.collection.BulkWrite(context.Background(), models, opts)
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
func insertNewAggregateDocs(collection *mongo.Collection, createQueue []interface{}) {
	if len(createQueue) > 0 {
		off := 0
		for off < len(createQueue) {
			l := off + aggInsertBatchSize
			if l > len(createQueue) {
				l = len(createQueue)
			}

			_, err := collection.InsertMany(context.Background(), createQueue[off:l])
			if err != nil {
				log.Fatalf("Bulk aggregate docs err: %s\n", err.Error())
			}

			off = l
		}
	}
}
