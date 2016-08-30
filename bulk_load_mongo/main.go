// bulk_load_mongo loads a Mongo daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/pkg/profile"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/influxdata/influxdb-comparisons/mongo_serialization"
)

// Program option vars:
var (
	daemonUrl    string
	workers      int
	batchSize    int
	limit        int64
	doLoad       bool
	writeTimeout time.Duration
)

// Global vars
var (
	batchChan    chan *Batch
	inputDone    chan struct{}
	workersGroup sync.WaitGroup
)

// Magic database constants
const (
	dbName              = "benchmark_db"
	pointCollectionName = "point_data"
)

// bufPool holds []byte instances to reduce heap churn.
var bufPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 1024)
	},
}

// Batch holds byte slices that will become mongo_serialization.Item instances.
type Batch [][]byte

func (b *Batch) ClearReferences() {
	*b = (*b)[:0]
}

// batchPool holds *Batch instances to reduce heap churn.
var batchPool = &sync.Pool{
	New: func() interface{} {
		return &Batch{}
	},
}

// Parse args:
func init() {
	flag.StringVar(&daemonUrl, "url", "localhost:27017", "Mongo URL.")

	flag.IntVar(&batchSize, "batch-size", 100, "Batch size (input items).")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")
	flag.Int64Var(&limit, "limit", -1, "Number of items to insert (default unlimited).")
	flag.DurationVar(&writeTimeout, "write-timeout", 10*time.Second, "Write timeout.")

	flag.BoolVar(&doLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed.")

	flag.Parse()

	for i := 0; i < workers*batchSize; i++ {
		bufPool.Put(bufPool.New())
	}
}

func main() {
	//_ = profile.Start
	p := profile.Start(profile.MemProfile)
	defer p.Stop()
	if doLoad {
		mustCreateCollections(daemonUrl)
	}

	var session *mgo.Session

	if doLoad {
		var err error
		session, err = mgo.Dial(daemonUrl)
		if err != nil {
			log.Fatal(err)
		}

		session.SetMode(mgo.Eventual, false)

		defer session.Close()
	}

	batchChan = make(chan *Batch, workers*10)
	inputDone = make(chan struct{})

	for i := 0; i < workers; i++ {
		workersGroup.Add(1)
		go processBatches(session)
	}

	start := time.Now()
	itemsRead := scan(session, batchSize)

	<-inputDone
	close(batchChan)
	workersGroup.Wait()
	end := time.Now()
	took := end.Sub(start)
	rate := float64(itemsRead) / float64(took.Seconds())

	fmt.Printf("loaded %d values in %fsec with %d workers (mean rate %f values/sec)\n", itemsRead, took.Seconds(), workers, rate)
}

// scan reads length-delimited flatbuffers items from stdin.
func scan(session *mgo.Session, itemsPerBatch int) int64 {
	//var batch *gocql.Batch
	if doLoad {
		//batch = session.NewBatch(gocql.LoggedBatch)
	}

	var n int
	var itemsRead int64
	r := bufio.NewReaderSize(os.Stdin, 32<<20)

	start := time.Now()
	batch := batchPool.Get().(*Batch)
	lenBuf := make([]byte, 8)

	for {
		if itemsRead == limit {
			break
		}
		// get the serialized item length (this is the framing format)
		_, err := r.Read(lenBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err.Error())
		}

		// ensure correct len of receiving buffer
		l := int(binary.LittleEndian.Uint64(lenBuf))
		itemBuf := bufPool.Get().([]byte)
		if cap(itemBuf) < l {
			itemBuf = make([]byte, l)
		}
		itemBuf = itemBuf[:l]

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
			panic(fmt.Sprintf("reader/writer logic error, %d != %d", n, len(itemBuf)))
		}

		*batch = append(*batch, itemBuf)

		itemsRead++
		n++

		if n >= batchSize {
			batchChan <- batch
			n = 0
			batch = batchPool.Get().(*Batch)
		}

		_ = start
		//if itemsRead > 0 && itemsRead%100000 == 0 {
		//	_ = start
		//	//took := (time.Now().UnixNano() - start.UnixNano())
		//	//if took >= 1e9 {
		//	//	tookUs := float64(took) / 1e3
		//	//	tookSec := float64(took) / 1e9
		//	//	fmt.Fprintf(os.Stderr, "itemsRead: %d, rate: %.0f/sec, lag: %.2fus/op\n",
		//	//		itemsRead, float64(itemsRead)/tookSec, tookUs/float64(itemsRead))
		//	//}
		//}
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(inputDone)

	return itemsRead
}

// processBatches reads byte buffers from batchChan, interprets them and writes
// them to the target server. Note that mgo forcibly incurs serialization
// overhead (it always encodes to BSON).
func processBatches(session *mgo.Session) {
	db := session.DB(dbName)

	type Tag struct {
		Key string `bson:"key"`
		Val string `bson:"val"`
	}

	type Point struct {
		// Use `string` here even though they are really `[]byte`.
		// This is so the mongo data is human-readable.
		MeasurementName string      `bson:"measurement"`
		FieldName       string      `bson:"field"`
		Timestamp       int64       `bson:"timestamp_ns"`
		Tags            []Tag       `bson:"tags"`
		Value           interface{} `bson:"value"`

		// a private union-like section
		longValue int64
		doubleValue float64
	}
	pPool := &sync.Pool{New: func() interface{} { return &Point{} }}
	pvs := []interface{}{}

	item := &mongo_serialization.Item{}
	destTag := &mongo_serialization.Tag{}
	collection := db.C(pointCollectionName)
	for batch := range batchChan {
		bulk := collection.Bulk()

		if cap(pvs) < len(*batch) {
			pvs = make([]interface{}, len(*batch))
		}
		pvs = pvs[:len(*batch)]

		for i, itemBuf := range *batch {
			// this ui could be improved on the library side:
			n := flatbuffers.GetUOffsetT(itemBuf)
			item.Init(itemBuf, n)
			x := pPool.Get().(*Point)

			x.MeasurementName = unsafeBytesToString(item.MeasurementNameBytes())
			x.FieldName = unsafeBytesToString(item.FieldNameBytes())
			x.Timestamp = item.TimestampNanos()

			tagLength := item.TagsLength()
			if cap(x.Tags) < tagLength {
				x.Tags = make([]Tag, 0, tagLength)
			}
			x.Tags = x.Tags[:tagLength]
			for i := 0; i < tagLength; i++ {
				*destTag = mongo_serialization.Tag{} // clear
				item.Tags(destTag, i)
				x.Tags[i].Key = unsafeBytesToString(destTag.KeyBytes())
				x.Tags[i].Val = unsafeBytesToString(destTag.ValBytes())
			}

			// this complexity is the result of trying to minimize
			// allocs while using an interface{} type for
			// (*Point).Value.
			switch item.ValueType() {
			case mongo_serialization.ValueTypeLong:
				x.longValue = item.LongValue()
				x.Value = &x.longValue
			case mongo_serialization.ValueTypeDouble:
				x.doubleValue = item.DoubleValue()
				x.Value = &x.doubleValue
			default:
				panic("logic error")
			}
			pvs[i] = x

		}
		bulk.Insert(pvs...)

		if doLoad {
			_, err := bulk.Run()
			if err != nil {
				log.Fatalf("Bulk err: %s\n", err.Error())
			}

		}

		// cleanup pvs
		for _, x := range pvs {
			p := x.(*Point)
			p.Timestamp = 0
			p.Value = nil
			p.longValue = 0
			p.doubleValue = 0
			p.Tags = p.Tags[:0]
			pPool.Put(p)
		}

		// cleanup item data
		for _, itemBuf := range *batch {
			bufPool.Put(itemBuf)
		}
		batch.ClearReferences()
		batchPool.Put(batch)
	}
	workersGroup.Done()
}

func mustCreateCollections(daemonUrl string) {
	session, err := mgo.Dial(daemonUrl)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	// collection C: point data
	// from (*mgo.Collection).Create
	cmd := make(bson.D, 0, 4)
	cmd = append(cmd, bson.DocElem{"create", pointCollectionName})

	// wiredtiger settings
	cmd = append(cmd, bson.DocElem{
		"storageEngine", map[string]interface{}{
			"wiredTiger": map[string]interface{}{
				"configString": "block_compressor=snappy",
			},
		},
	})

	err = session.DB("benchmark_db").Run(cmd, nil)
	if err != nil {
		log.Fatal(err)
	}

	collection := session.DB("benchmark_db").C("point_data")
	index := mgo.Index{
		Key: []string{"measurement", "tags", "field", "timestamp_ns"},
		Unique: false, // Unique does not work on the entire array of tags!
		DropDups: true,
		Background: false,
		Sparse: false,
	}
	err = collection.EnsureIndex(index)
	if err != nil {
		log.Fatal(err)
	}
}
