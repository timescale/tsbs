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
	clientConfig   client.Config
	timeoutInMs    int // 0 for no timeout
	recordsMaxRows int // max rows of records in 'InsertRecords'
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
	workers := viper.GetUint("workers")
	recordsMaxRows = viper.GetInt("records-max-rows")
	timeoutInMs = viper.GetInt("timeout")

	timeoutStr := fmt.Sprintf("timeout for session opening check: %d ms", timeoutInMs)
	if timeoutInMs <= 0 {
		timeoutInMs = 0 // 0 for no timeout.
		timeoutStr = "no timeout for session opening check"
	}
	log.Printf("tsbs_load_iotdb target: %s:%s, %s. Loading with %d workers.\n", host, port, timeoutStr, workers)
	if workers < 5 {
		log.Println("Insertion throughput is strongly related to the number of threads. Use more workers for better performance.")
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
