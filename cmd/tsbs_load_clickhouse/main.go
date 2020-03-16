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

const (
	timeValueIdx = "TIME-VALUE"
	valueTimeIdx = "VALUE-TIME"
)

// Global vars
var (
	target targets.ImplementedTarget
)

var loader *load.BenchmarkRunner
var conf *clickhouse.ClickhouseConfig

// Parse args:
func init() {
	var config load.BenchmarkRunnerConfig
	target := clickhouse.NewTarget()
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
	conf = &clickhouse.ClickhouseConfig{
		Host:       viper.GetString("host"),
		User:       viper.GetString("user"),
		Password:   viper.GetString("password"),
		LogBatches: viper.GetBool("log-batches"),
		Debug:      viper.GetInt("debug"),
		DbName:     loader.DBName,
	}

	loader = load.GetBenchmarkRunner(config)
}

func main() {
	loader.RunBenchmark(clickhouse.NewBenchmark(loader.FileName, loader.HashWorkers, conf))
}
