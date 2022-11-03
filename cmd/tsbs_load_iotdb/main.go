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
	clientConfig client.Config
	timeoutInMs  int // 0 for no timeout
)

// Global vars
var (
	target targets.ImplementedTarget
	// CRTODO 如果没用就移除它
	loaderConfig load.BenchmarkRunnerConfig
	loader       load.BenchmarkRunner
	// bufPool      sync.Pool
)

// allows for testing
var fatal = log.Fatalf // CRTODO 如果没用就移除它

// Parse args:
func init() {
	timeoutInMs = int(2000)
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

	clientConfig = client.Config{
		Host:     host,
		Port:     port,
		UserName: user,
		Password: password,
	}

	loader = load.GetBenchmarkRunner(loaderConfig)
}

func main() {
	// bufPool = sync.Pool{
	// 	New: func() interface{} {
	// 		return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
	// 	},
	// }
	benchmark := newBenchmark(clientConfig, loaderConfig)

	loader.RunBenchmark(benchmark)
}
