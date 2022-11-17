// tsbs_load_iotdb loads an IoTDB daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// tsbs load.
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

	"github.com/apache/iotdb-client-go/client"
)

// database option vars
var (
	clientConfig      client.Config
	timeoutInMs       int    // 0 for no timeout
	recordsMaxRows    int    // max rows of records in 'InsertRecords'
	loadToSCV         bool   // if true, do NOT insert into databases, but generate csv files instead.
	csvFilepathPrefix string // Prefix of filepath for csv files. Specific a folder or a folder with filename prefix.
)

// Global vars
var (
	target targets.ImplementedTarget

	loaderConfig load.BenchmarkRunnerConfig
	loader       load.BenchmarkRunner
)

// allows for testing
var fatal = log.Fatalf

// Parse args:
func init() {
	target = initializers.GetTarget(constants.FormatIoTDB)
	loaderConfig = load.BenchmarkRunnerConfig{}
	loaderConfig.AddToFlagSet(pflag.CommandLine)
	target.TargetSpecificFlags("", pflag.CommandLine)
	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}
	if err := viper.Unmarshal(&loaderConfig); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	host := viper.GetString("host")
	port := viper.GetString("port")
	user := viper.GetString("user")
	password := viper.GetString("password")
	timeoutInMs = viper.GetInt("timeout")
	recordsMaxRows = viper.GetInt("records-max-rows")
	loadToSCV = viper.GetBool("to-csv")
	csvFilepathPrefix = viper.GetString("csv-prefix")

	workers := viper.GetUint("workers")

	timeoutStr := fmt.Sprintf("timeout for session opening check: %d ms", timeoutInMs)
	if timeoutInMs <= 0 {
		timeoutInMs = 0 // 0 for no timeout.
		timeoutStr = "no timeout for session opening check"
	}
	log.Printf("tsbs_load_iotdb target: %s:%s, %s. Loading with %d workers.\n", host, port, timeoutStr, workers)
	if workers < 2 {
		log.Println("Insertion throughput is strongly related to the number of threads. Use more workers for better performance.")
	}
	if loadToSCV && workers != 1 {
		err_msg := "Arguments conflicts! When using csv export method, `workers` should NOT be set more than 1. "
		err_msg += fmt.Sprintf("Current setting: `to-csv`=%v, `workers`=%d.", loadToSCV, workers)
		log.Println(err_msg)
		panic(err_msg)
	}

	clientConfig = client.Config{
		Host:     host,
		Port:     port,
		UserName: user,
		Password: password,
	}

	loader = load.GetBenchmarkRunner(loaderConfig)
}

func main() {
	benchmark := newBenchmark(clientConfig, loaderConfig)

	loader.RunBenchmark(benchmark)
}
