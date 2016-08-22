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
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/influxdata/influxdb-comparisons/mongo_serialization"
)

// Program option vars:
var (
	daemonUrl    string
	workers      int
	batchSize    int
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
		return make([]byte, 0, 256)
	},
}

// Batch holds byte slices that will become mongo_serialization.Item instances.
type Batch [][]byte

func (b *Batch) ClearReferences() {
	*b = (*b)[:0]
}

// bufPool holds *Batch instances to reduce heap churn.
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
	flag.DurationVar(&writeTimeout, "write-timeout", 10*time.Second, "Write timeout.")

	flag.BoolVar(&doLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed.")

	flag.Parse()
}

func main() {
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

	fmt.Printf("loaded %d items in %fsec with %d workers (mean rate %f/sec)\n", itemsRead, took.Seconds(), workers, rate)
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

		if itemsRead > 0 && itemsRead%100000 == 0 {
			took := (time.Now().UnixNano() - start.UnixNano())
			if took >= 1e9 {
				tookUs := took / 1e3
				tookSec := took / 1e9
				fmt.Fprintf(os.Stderr, "itemsRead: %d, rate: %d/sec, lag: %dus/op\n",
					itemsRead, int64(itemsRead)/tookSec, tookUs/int64(itemsRead))
			}
		}
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(inputDone)

	return itemsRead
}

// processBatches reads byte buffers from batchChan, interprets them and writes
// them to the target server. Note that mgo incurs a lot of overhead.
func processBatches(session *mgo.Session) {
	db := session.DB(dbName)

	type pointValue struct {
		id struct {
			internalSeriesId []byte
			timestamp        int64
		} `bson:"_id" json:"id"`
		value interface{} `bson:"v" json:"v"`
	}
	var pvs []interface{}

	item := &mongo_serialization.Item{}
	collection := db.C(pointCollectionName)
	for batch := range batchChan {
		bulk := collection.Bulk()

		if cap(pvs) < len(*batch) {
			pvs = make([]interface{}, len(*batch))
			for i := range pvs {
				pvs[i] = &pointValue{}
			}
		}
		pvs = pvs[:len(*batch)]

		i := 0
		for _, itemBuf := range *batch {
			// this ui could be improved on the library side:
			n := flatbuffers.GetUOffsetT(itemBuf)
			item.Init(itemBuf, n)

			pv := pvs[i].(*pointValue)
			pv.id.internalSeriesId = item.SeriesIdBytes()
			pv.id.timestamp = item.TimestampNanos()

			switch item.ValueType() {
			case mongo_serialization.ValueTypeLong:
				pv.value = item.LongValue()
			case mongo_serialization.ValueTypeDouble:
				pv.value = item.DoubleValue()
			default:
				panic("logic error")
			}

			//fmt.Fprintf(os.Stderr, "%s - %d\n", pv.id.internalSeriesId, pv.id.timestamp)
			i++
		}
		bulk.Insert(pvs...)

		if doLoad {
			_, err := bulk.Run()
			if err != nil {
				log.Fatalf("Bulk err: %s\n", err.Error())
			}

		}

		// cleanup pvs
		for i := range pvs {
			pv := pvs[i].(*pointValue)
			pv.id.internalSeriesId = nil
			pv.id.timestamp = 0
			pv.value = nil
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

	// mmapv1 settings
	//cmd = append(cmd, bson.DocElem{
	//		"mmapv1", map[string]interface{}{
	//			"usePowerOf2Sizes": false,
	//			"noPadding": true,
	//		},
	//})
	err = session.DB("benchmark_db").Run(cmd, nil)
	if err != nil {
		log.Fatal(err)
	}
}

//func ensureCollectionExists(session *mgo.Session, name []byte) {
//	globalCollectionMappingMutex.RLock()
//	_, ok := globalCollectionMapping[unsafeBytesToString(name)]
//	globalCollectionMappingMutex.RUnlock()
//	if ok {
//		// nothing to do
//		return
//	}
//
//	globalCollectionMappingMutex.Lock()
//	_, ok = globalCollectionMapping[unsafeBytesToString(name)]
//	if ok {
//		// another goroutine inserted this, nothing to do:
//		globalCollectionMappingMutex.Unlock()
//		return
//	}
//
//	_, ok = globalCollectionMapping[unsafeBytesToString(name)]
//
//	globalCollectionMappingMutex.Unlock()
//}
