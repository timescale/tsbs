// bulk_load_akumuli loads an akumlid daemon with data from stdin.
//
// The caller is responsible for assuring that the database is empty before
// bulk load.
package main

import (
	"bytes"
	"fmt"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/akumuli"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"github.com/timescale/tsbs/pkg/targets/initializers"
	"log"
	"sync"

	"github.com/spf13/pflag"
	"github.com/blagojts/viper"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
)

// Program option vars:
var (
	endpoint string
)

// Global vars
var (
	loader *load.BenchmarkRunner
)

// allows for testing
var fatal = log.Fatalf
var target targets.ImplementedTarget

// Parse args:
func init() {
	target = initializers.GetTarget(constants.FormatAkumuli)
	var config load.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)
	target.TargetSpecificFlags("",pflag.CommandLine)

	pflag.Parse()
	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	endpoint = viper.GetString("endpoint")
	config.HashWorkers = false
	loader = load.GetBenchmarkRunner(config)
}

func main() {
	bufPool := sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}
	benchmark := akumuli.NewBenchmark(loader.FileName, endpoint, &bufPool)
	loader.RunBenchmark(benchmark)
}
