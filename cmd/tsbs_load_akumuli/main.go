// bulk_load_akumuli loads an akumlid daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bytes"
	"fmt"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"log"
	"sync"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/internal/utils"
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
	var config load.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.StringVar(&endpoint, "endpoint", "http://localhost:8282", "Akumuli RESP endpoint IP address.")
	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	endpoint = viper.GetString("endpoint")
	loader = load.GetBenchmarkRunner(config)
}

type benchmark struct{}

func (b *benchmark) GetDataSource() source.DataSource {
	return &fileDataSource{reader: load.GetBufferedReader(loader.FileName)}
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(n uint) targets.PointIndexer {
	return &pointIndexer{nchan: n}
}

func (b *benchmark) GetProcessor() targets.Processor {
	return &processor{endpoint: endpoint}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
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
