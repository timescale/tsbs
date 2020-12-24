package main

import (
	"log"
	"sync"

	"github.com/globalsign/mgo"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/mongo"
)

// naiveBenchmark allows you to run a benchmark using the naive, one document per
// event Mongo approach
type naiveBenchmark struct {
	mongoBenchmark
}

func newNaiveBenchmark(l load.BenchmarkRunner, loaderConf *load.BenchmarkRunnerConfig) *naiveBenchmark {
	return &naiveBenchmark{mongoBenchmark{loaderConf.FileName, l, &dbCreator{}}}
}

func (b *naiveBenchmark) GetProcessor() targets.Processor {
	return &naiveProcessor{dbc: b.dbc}
}

func (b *naiveBenchmark) GetPointIndexer(_ uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}

type singlePoint struct {
	Measurement string                 `bson:"measurement"`
	Timestamp   int64                  `bson:"timestamp_ns"`
	Fields      map[string]interface{} `bson:"fields"`
	Tags        map[string]string      `bson:"tags"`
}

var spPool = &sync.Pool{New: func() interface{} { return &singlePoint{} }}

type naiveProcessor struct {
	dbc        *dbCreator
	collection *mgo.Collection

	pvs []interface{}
}

func (p *naiveProcessor) Init(_ int, doLoad, _ bool) {
	if doLoad {
		sess := p.dbc.session.Copy()
		db := sess.DB(loader.DatabaseName())
		p.collection = db.C(collectionName)
	}
	p.pvs = []interface{}{}
}

// ProcessBatch creates a new document for each incoming event for a simpler
// approach to storing the data. This is _NOT_ the default since the aggregation method
// is recommended by Mongo and other blogs
func (p *naiveProcessor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
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
		f := &mongo.MongoReading{}
		for j := 0; j < event.FieldsLength(); j++ {
			event.Fields(f, j)
			x.Fields[string(f.Key())] = f.Value()
		}
		t := &mongo.MongoTag{}
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
