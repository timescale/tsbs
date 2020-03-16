package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/jackc/pgconn"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"github.com/timescale/tsbs/pkg/targets/initializers"
	"log"

	"github.com/blagojts/viper"
	"github.com/jackc/pgx/v4"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
)

var loader *load.BenchmarkRunner
var target targets.ImplementedTarget

// the logger is used in implementations of interface methods that
// do not return error on failures to allow testing such methods
var fatal = log.Fatalf

type benchmark struct {
	dbc *dbCreator
	ds  targets.DataSource
}

func (b *benchmark) GetDataSource() targets.DataSource {
	return b.ds
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	return &targets.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() targets.Processor {
	return &processor{
		tableDefs: b.dbc.tableDefs,
		connCfg:   b.dbc.cfg,
	}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return b.dbc
}

func main() {
	target = initializers.GetTarget(constants.FormatCrateDB)
	var config load.BenchmarkRunnerConfig
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

	hosts := viper.GetString("hosts")
	port := viper.GetUint("port")
	user := viper.GetString("user")
	pass := viper.GetString("pass")

	numReplicas := flag.Int("replicas", 0, "Number of replicas per a metric table")
	numShards := flag.Int("shards", 5, "Number of shards per a metric table")
	config.HashWorkers = false
	loader = load.GetBenchmarkRunner(config)

	connConfig := &pgx.ConnConfig{
		Config: pgconn.Config{Host: hosts,
			Port:     uint16(port),
			User:     user,
			Password: pass,
			Database: "doc",
		},
	}

	// TODO implement or check if anything has to be done to support WorkerPerQueue mode
	loader.RunBenchmark(&benchmark{
		dbc: &dbCreator{
			cfg:         connConfig,
			numReplicas: *numReplicas,
			numShards:   *numShards,
		},
		ds: &fileDataSource{scanner: bufio.NewScanner(load.GetBufferedReader(loader.FileName))},
	})
}
