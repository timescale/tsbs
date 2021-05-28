package main

import (
	"crypto/md5"
	"fmt"
	"github.com/blagojts/viper"
	"github.com/mediocregopher/radix/v3"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"github.com/timescale/tsbs/pkg/targets/initializers"
	"log"

	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/targets"
)

// Program option vars:
var (
	host               string
	connections        uint64
	pipeline           uint64
	checkChunks        uint64
	singleQueue        bool
	dataModel          string
	compressionEnabled bool
	clusterMode        bool
)

// Global vars
var (
	loader     load.BenchmarkRunner
	config     load.BenchmarkRunnerConfig
	target     targets.ImplementedTarget
	cluster    *radix.Cluster
	standalone *radix.Pool
	addresses  []string
	slots      [][][2]uint16
	conns      []radix.Client
)

// allows for testing
var fatal = log.Fatal
var md5h = md5.New()
var errorTsCreate = errors.New("ERR TSDB: key already exists")

// Parse args:
func init() {
	target = initializers.GetTarget(constants.FormatRedisTimeSeries)
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
	host = viper.GetString("host")
	connections = viper.GetUint64("connections")
	pipeline = viper.GetUint64("pipeline")
	dataModel = "redistimeseries"
	compressionEnabled = true
	clusterMode = viper.GetBool("cluster")
	config.NoFlowControl = true
	config.HashWorkers = true
	loader = load.GetBenchmarkRunner(config)

	opts := make([]radix.DialOpt, 0)
	if clusterMode {
		cluster = getOSSClusterConn(host, opts, connections)
		cluster.Sync()
		topology := cluster.Topo().Primaries().Map()
		addresses = make([]string, 0)
		slots = make([][][2]uint16, 0)
		conns = make([]radix.Client, 0)
		for nodeAddress, node := range topology {
			addresses = append(addresses, nodeAddress)
			slots = append(slots, node.Slots)
			conn, _ := cluster.Client(nodeAddress)
			conns = append(conns, conn)
		}
	} else {
		standalone = getStandaloneConn(host, opts, connections)
	}
}

func main() {
	log.Println("Starting benchmark")

	b := benchmark{dbc: &dbCreator{}}
	//if config.Workers > 1 {
	//	panic(fmt.Errorf("You should only use 1 worker and multiple connections per worker (set via --connections)"))
	//}

	loader.RunBenchmark(&b)
	log.Println("finished benchmark")
}
