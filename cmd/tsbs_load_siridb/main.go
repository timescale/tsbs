package main

import (
	"bufio"
	"fmt"
	"log"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/internal/utils"
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
	var config load.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("dbuser", "iris", "Username to enter SiriDB")
	pflag.String("dbpass", "siri", "Password to enter SiriDB")

	pflag.String("hosts", "localhost:9000", "Provide 1 or 2 (comma seperated) SiriDB hosts. If 2 hosts are provided, 2 pools are created.")
	pflag.Bool("replica", false, "Whether to create a replica instead of a second pool, when two hosts are provided.")

	pflag.Bool("log-batches", false, "Whether to time individual batches.")
	pflag.Int("write-timeout", 10, "Write timeout.")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	dbUser = viper.GetString("dbuser")
	dbPass = viper.GetString("dbpass")
	hosts = viper.GetString("hosts")
	replica = viper.GetBool("replica")
	logBatches = viper.GetBool("log-batches")
	writeTimeout = viper.GetInt("write-timeout")

	loader = load.GetBenchmarkRunner(config)
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
