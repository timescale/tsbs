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

// Global vars
var (
	loader    *load.BenchmarkRunner
	creator   *dbCreator
	hyprToken string
	db        string
	host      string
)

// Parse args:
func init() {

	var config load.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)
	pflag.String("token", "", "Hyprcubd API Token")
	pflag.String("host", "https://api.hyprcubd.com", "")

	pflag.Parse()
	if err := utils.SetupConfigFile(); err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}
	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	hyprToken = viper.GetString("token")
	if len(hyprToken) == 0 {
		log.Fatalf("missing `token` flag")
	}

	db = viper.GetString("db-name")
	if len(db) == 0 {
		log.Fatalf("missing `db` flag")
	}
	host = viper.GetString("host")

	loader = load.GetBenchmarkRunner(config)
}

// loader.Benchmark interface implementation
type benchmark struct{}

// loader.Benchmark interface implementation
func (b *benchmark) GetPointDecoder(br *bufio.Reader) load.PointDecoder {
	return &decoder{
		scanner: bufio.NewScanner(br),
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
	creator = &dbCreator{
		br: loader.GetBufferedReader(),
	}
	return creator
}

func main() {
	loader.RunBenchmark(&benchmark{}, load.SingleQueue)
}
