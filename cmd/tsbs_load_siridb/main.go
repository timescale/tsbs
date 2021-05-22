package main

import (
	"fmt"
	"log"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"github.com/timescale/tsbs/pkg/targets/initializers"
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
	loader load.BenchmarkRunner
	config load.BenchmarkRunnerConfig
	target targets.ImplementedTarget
)

// allows for testing
var fatal = log.Fatal

// Parse args:
func init() {
	target = initializers.GetTarget(constants.FormatSiriDB)
	config = load.BenchmarkRunnerConfig{}
	config.AddToFlagSet(pflag.CommandLine)
	target.TargetSpecificFlags("", pflag.CommandLine)

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
	config.HashWorkers = false
	loader = load.GetBenchmarkRunner(config)
}

type benchmark struct{}

func (b *benchmark) GetDataSource() targets.DataSource {
	return &fileDataSource{
		buf: make([]byte, 0),
		len: 0,
		br:  load.GetBufferedReader(config.FileName),
	}
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() targets.Processor {
	return &processor{}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{}
}

func main() {
	loader.RunBenchmark(&benchmark{})
}
