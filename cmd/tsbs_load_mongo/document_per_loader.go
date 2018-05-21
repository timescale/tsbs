package main

import (
	"log"
	"sync"

	"bitbucket.org/440-labs/influxdb-comparisons/cmd/tsbs_generate_data/serialize"
	"bitbucket.org/440-labs/influxdb-comparisons/load"
	"github.com/globalsign/mgo"
)

// naiveBenchmark allows you to run a benchmark using the naive, one document per
// event Mongo approach
type naiveBenchmark struct {
	mongoBenchmark
}

func newNaiveBenchmark(l *load.BenchmarkRunner, session *mgo.Session) *naiveBenchmark {
	return &naiveBenchmark{mongoBenchmark{l, session}}
}

func (b *naiveBenchmark) GetProcessor() load.Processor {
	return &naiveProcessor{session: b.session}
}

func (b *naiveBenchmark) GetPointIndexer(_ uint) load.PointIndexer {
	return &load.ConstantIndexer{}
}

type singlePoint struct {
	Measurement string                 `bson:"measurement"`
	Timestamp   int64                  `bson:"timestamp_ns"`
	Fields      map[string]interface{} `bson:"fields"`
	Tags        map[string]string      `bson:"tags"`
}

var spPool = &sync.Pool{New: func() interface{} { return &singlePoint{} }}

type naiveProcessor struct {
	session    *mgo.Session
	collection *mgo.Collection

	pvs []interface{}
}

func (p *naiveProcessor) Init(workerNUm int, doLoad bool) {
	if doLoad {
		sess := p.session.Copy()
		db := sess.DB(loader.DatabaseName())
		p.collection = db.C(collectionName)
	}
	p.pvs = []interface{}{}
}

// ProcessBatch creates a new document for each incoming event for a simpler
// approach to storing the data. This is _NOT_ the default since the aggregation method
// is recommended by Mongo and other blogs
func (p *naiveProcessor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	batch := b.(*batch).arr
	if cap(p.pvs) < len(batch) {
		p.pvs = make([]interface{}, len(batch))
	}
	p.pvs = p.pvs[:len(batch)]
	var metricCnt uint64
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
		p.pvs[i] = x
		metricCnt += uint64(event.FieldsLength())
	}

	if doLoad {
		bulk := p.collection.Bulk()
		bulk.Insert(p.pvs...)
		_, err := bulk.Run()
		if err != nil {
			log.Fatalf("Bulk insert docs err: %s\n", err.Error())
		}
	}
	for _, p := range p.pvs {
		spPool.Put(p)
	}

	return metricCnt, 0
}
