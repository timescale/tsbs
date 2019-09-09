// bulk_load_akumuli loads an akumlid daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bufio"
	"bytes"
	"flag"
	"log"
	"sync"

	"github.com/timescale/tsbs/load"
)

// Program option vars:
var (
	endpoint string
)

// Global vars
var (
	loader  *load.BenchmarkRunner
	bufPool sync.Pool
)

// allows for testing
var fatal = log.Fatalf

// Parse args:
func init() {
	loader = load.GetBenchmarkRunner()

	flag.StringVar(&endpoint, "endpoint", "http://localhost:8282", "Akumuli RESP endpoint IP address.")
	flag.Parse()
}

type benchmark struct{}

func (b *benchmark) GetPointDecoder(br *bufio.Reader) load.PointDecoder {
	return &decoder{reader: br}
}

func (b *benchmark) GetBatchFactory() load.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(n uint) load.PointIndexer {
	return &pointIndexer{nchan: n}
}

func (b *benchmark) GetProcessor() load.Processor {
	return &processor{endpoint: endpoint}
}

func (b *benchmark) GetDBCreator() load.DBCreator {
	return &dbCreator{}
}

func main() {
	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}
	loader.RunBenchmark(&benchmark{}, load.WorkerPerQueue)
}