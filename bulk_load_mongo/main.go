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
		//createKeyspace(daemonUrl)
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

	batchChan = make(chan *Batch, workers*100)
	inputDone = make(chan struct{})

	for i := 0; i < workers; i++ {
		workersGroup.Add(1)
		go processItems(session)
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

		// read the bytes and init the flatbuffer
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
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(inputDone)

	return itemsRead
}

// processItems reads byte buffers from itemChan, interprets them and writes
// them to the target server. Note that mgo incurs a lot of overhead.
func processItems(session *mgo.Session) {
	db := session.DB("benchmark_db")
	item := &mongo_serialization.Item{}

	type pointValue struct {
		timestamp int64
		value     interface{}
	}

	pv := &pointValue{}
	for batch := range batchChan {
		for _, itemBuf := range *batch {
			// this ui could be improved on the library side:
			n := flatbuffers.GetUOffsetT(itemBuf)
			item.Init(itemBuf, n)

			pv.timestamp = item.TimestampNanos()

			switch item.ValueType() {
			case mongo_serialization.ValueTypeLong:
				pv.value = item.LongValue()
			case mongo_serialization.ValueTypeDouble:
				pv.value = item.DoubleValue()
			default:
				panic("logic error")
			}

			if doLoad {
				collection := db.C(unsafeBytesToString(item.SeriesIdBytes()))
				err := collection.Insert(pv)
				if err != nil {
					log.Fatalf("Error writing: %s\n", err.Error())
				}
			}
		}

		for _, itemBuf := range *batch {
			bufPool.Put(itemBuf)
		}
		batch.ClearReferences()
		batchPool.Put(batch)
	}
	workersGroup.Done()
}
