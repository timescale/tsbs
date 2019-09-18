// tsbs_load_clickhouse loads a ClickHouse instance with data from stdin.
//
// If the database exists beforehand, it will be *DROPPED*.
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

const (
	dbType       = "clickhouse"
	timeValueIdx = "TIME-VALUE"
	valueTimeIdx = "VALUE-TIME"
)

// Program option vars:
var (
	host     string
	user     string
	password string

	logBatches  bool
	inTableTag  bool
	hashWorkers bool

	debug int
)

// String values of tags and fields to insert - string representation
type insertData struct {
	tags   string // hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,rack=67,os=Ubuntu16.10,arch=x86,team=NYC,service=7,service_version=0,service_environment=production
	fields string // 1451606400000000000,58,2,24,61,22,63,6,44,80,38
}

// Global vars
var (
	loader         *load.BenchmarkRunner
	tableCols      map[string][]string
	tagColumnTypes []string
)

// allows for testing
var fatal = log.Fatalf

// Parse args:
func init() {
	var config load.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("host", "localhost", "Hostname of ClickHouse instance")
	pflag.String("user", "default", "User to connect to ClickHouse as")
	pflag.String("password", "", "Password to connect to ClickHouse")

	pflag.Bool("log-batches", false, "Whether to time individual batches.")

	// TODO - This flag could potentially be done as a string/enum with other options besides no-hash, round-robin, etc
	pflag.Bool("hash-workers", false, "Whether to consistently hash insert data to the same workers (i.e., the data for a particular host always goes to the same worker)")

	pflag.Int("debug", 0, "Debug printing (choices: 0, 1, 2). (default 0)")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	host = viper.GetString("host")
	user = viper.GetString("user")
	password = viper.GetString("password")

	logBatches = viper.GetBool("log-batches")
	hashWorkers = viper.GetBool("hash-workers")
	debug = viper.GetInt("debug")

	loader = load.GetBenchmarkRunner(config)
	tableCols = make(map[string][]string)
}

// loader.Benchmark interface implementation
type benchmark struct{}

// loader.Benchmark interface implementation
func (b *benchmark) GetPointDecoder(br *bufio.Reader) load.PointDecoder {
	return &decoder{
		scanner: bufio.NewScanner(br),
	}
}

// loader.Benchmark interface implementation
func (b *benchmark) GetBatchFactory() load.BatchFactory {
	return &factory{}
}

// loader.Benchmark interface implementation
func (b *benchmark) GetPointIndexer(maxPartitions uint) load.PointIndexer {
	if hashWorkers {
		return &hostnameIndexer{
			partitions: maxPartitions,
		}
	}
	return &load.ConstantIndexer{}
}

// loader.Benchmark interface implementation
func (b *benchmark) GetProcessor() load.Processor {
	return &processor{}
}

// loader.Benchmark interface implementation
func (b *benchmark) GetDBCreator() load.DBCreator {
	return &dbCreator{}
}

func main() {
	if hashWorkers {
		loader.RunBenchmark(&benchmark{}, load.WorkerPerQueue)
	} else {
		loader.RunBenchmark(&benchmark{}, load.SingleQueue)
	}
}
