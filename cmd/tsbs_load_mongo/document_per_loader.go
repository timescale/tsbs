package main

import (
	"log"
	"sync"
	"sync/atomic"

	"bitbucket.org/440-labs/influxdb-comparisons/cmd/tsbs_generate_data/serialize"
	"github.com/globalsign/mgo"
)

type singleIndexer struct{}

func (i *singleIndexer) GetIndex(_ *serialize.MongoPoint) int { return 0 }

// scan reads length-delimited flatbuffers items from stdin.
func scan(channels []*duplexChannel, itemsPerBatch int) int64 {
	return scanWithIndexer(channels, itemsPerBatch, &singleIndexer{})
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
func processBatchesPerEvent(wg *sync.WaitGroup, session *mgo.Session, dc *duplexChannel) {
	var sess *mgo.Session
	var db *mgo.Database
	var collection *mgo.Collection
	if doLoad {
		sess = session.Copy()
		db = sess.DB(dbName)
		collection = db.C(collectionName)
	}
	c := dc.toWorker

	pvs := []interface{}{}
	for batch := range c {
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
		dc.sendToScanner()
	}
	wg.Done()
}
