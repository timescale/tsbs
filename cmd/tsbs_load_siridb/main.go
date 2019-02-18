package main

import (
	"bufio"
	"flag"
	"log"

	"github.com/timescale/tsbs/load"
)

// Program option vars:
var (
	hosts        string
	writeTimeout int
	dbUser       string
	dbPass       string
	logBatches   bool
	replica      bool
)

// Global vars
var (
	loader *load.BenchmarkRunner
)

// allows for testing
var fatal = log.Fatal

// Parse args:
func init() {
	loader = load.GetBenchmarkRunner()

	flag.StringVar(&dbUser, "dbuser", "iris", "Username to enter SiriDB")
	flag.StringVar(&dbPass, "dbpass", "siri", "Password to enter SiriDB")

	flag.StringVar(&hosts, "hosts", "localhost:9000", "Provide 1 or 2 (comma seperated) SiriDB hosts. If 2 hosts are provided, 2 pools are created.")
	flag.BoolVar(&replica, "replica", false, "Whether to create a replica instead of a second pool, when two hosts are provided.")

	flag.BoolVar(&logBatches, "log-batches", false, "Whether to time individual batches.")
	flag.IntVar(&writeTimeout, "write-timeout", 10, "Write timeout.")

	flag.Parse()
}

type benchmark struct{}

func (b *benchmark) GetPointDecoder(br *bufio.Reader) load.PointDecoder {
	return &decoder{
		buf: make([]byte, 0),
		len: 0,
	}
}

func (b *benchmark) GetBatchFactory() load.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) load.PointIndexer {
	return &load.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() load.Processor {
	return &processor{}
}

func (b *benchmark) GetDBCreator() load.DBCreator {
	return &dbCreator{}
}

func main() {
	loader.RunBenchmark(&benchmark{}, load.SingleQueue)
}
