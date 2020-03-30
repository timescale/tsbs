// tsbs_load_clickhouse loads a ClickHouse instance with data from stdin.
//
// If the database exists beforehand, it will be *DROPPED*.
package main

import (
	"fmt"
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/clickhouse"
)

<<<<<<< HEAD
=======
const (
	dbType       = "clickhouse"
	timeValueIdx = "TIME-VALUE"
	valueTimeIdx = "VALUE-TIME"
)

// Program option vars:
var (
	host     string
	port     int32
	user     string
	password string

	logBatches  bool
	inTableTag  bool
	hashWorkers bool
	useHTTP     bool

	debug int
)

// String values of tags and fields to insert - string representation
type insertData struct {
	tags   string // hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,rack=67,os=Ubuntu16.10,arch=x86,team=NYC,service=7,service_version=0,service_environment=production
	fields string // 1451606400000000000,58,2,24,61,22,63,6,44,80,38
}

>>>>>>> add IoT case for ClickHouse
// Global vars
var (
	target targets.ImplementedTarget
)

var loader load.BenchmarkRunner
var loaderConf load.BenchmarkRunnerConfig
var conf *clickhouse.ClickhouseConfig

// Parse args:
func init() {
<<<<<<< HEAD
	loaderConf = load.BenchmarkRunnerConfig{}
	target := clickhouse.NewTarget()
	loaderConf.AddToFlagSet(pflag.CommandLine)
	target.TargetSpecificFlags("", pflag.CommandLine)
=======
	var config load.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("host", "localhost", "Hostname of ClickHouse instance")
	pflag.String("port", "9000", "Port of ClickHouse instance")
	pflag.String("user", "default", "User to connect to ClickHouse as")
	pflag.String("password", "", "Password to connect to ClickHouse")

	pflag.Bool("log-batches", false, "Whether to time individual batches.")

	// TODO - This flag could potentially be done as a string/enum with other options besides no-hash, round-robin, etc
	pflag.Bool("hash-workers", false, "Whether to consistently hash insert data to the same workers (i.e., the data for a particular host always goes to the same worker)")
	pflag.Bool("use-http", false, "Whether to use http driver, default false.")

	pflag.Int("debug", 0, "Debug printing (choices: 0, 1, 2). (default 0)")

>>>>>>> add IoT case for ClickHouse
	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&loaderConf); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}
<<<<<<< HEAD
	conf = &clickhouse.ClickhouseConfig{
		Host:       viper.GetString("host"),
		User:       viper.GetString("user"),
		Password:   viper.GetString("password"),
		LogBatches: viper.GetBool("log-batches"),
		Debug:      viper.GetInt("debug"),
		DbName:     loaderConf.DBName,
=======

	host = viper.GetString("host")
	port = viper.GetInt32("port")
	user = viper.GetString("user")
	password = viper.GetString("password")

	logBatches = viper.GetBool("log-batches")
	hashWorkers = viper.GetBool("hash-workers")
	useHTTP = viper.GetBool("use-http")
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
>>>>>>> add IoT case for ClickHouse
	}

	loader = load.GetBenchmarkRunner(loaderConf)
}

func main() {
	loader.RunBenchmark(clickhouse.NewBenchmark(loaderConf.FileName, loaderConf.HashWorkers, conf))
}
