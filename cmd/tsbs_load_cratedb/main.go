package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/jackc/pgconn"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"log"

	"github.com/jackc/pgx/v4"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
)

var loader *load.BenchmarkRunner

// the logger is used in implementations of interface methods that
// do not return error on failures to allow testing such methods
var fatal = log.Fatalf

type benchmark struct {
	dbc *dbCreator
	ds  source.DataSource
}

func (b *benchmark) GetDataSource() source.DataSource {
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
	var config load.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("hosts", "localhost", "CrateDB hostnames")
	pflag.Uint("port", 5432, "A port to connect to database instances")
	pflag.String("user", "crate", "User to connect to CrateDB")
	pflag.String("pass", "", "Password for user connecting to CrateDB")

	pflag.Int("replicas", 0, "Number of replicas per a metric table")
	pflag.Int("shards", 5, "Number of shards per a metric table")

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
	}, load.SingleQueue)
}
