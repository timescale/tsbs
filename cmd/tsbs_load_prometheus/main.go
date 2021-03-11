package main

import (
	"fmt"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/prometheus"
)

// runs the benchmark
var (
	target targets.ImplementedTarget
	loader load.BenchmarkRunner
	config load.BenchmarkRunnerConfig
)
var adapterWriteUrl string

func init() {
	target = prometheus.NewTarget()
	config = load.BenchmarkRunnerConfig{}
	config.AddToFlagSet(pflag.CommandLine)
	target.TargetSpecificFlags("", pflag.CommandLine)
	pflag.Parse()
	err := utils.SetupConfigFile()
	if err != nil {
		panic(fmt.Errorf("error setting up a config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}
	adapterWriteUrl = viper.GetString("adapter-write-url")
	loader = load.GetBenchmarkRunner(config)
}

func main() {
	benchmark, err := prometheus.NewBenchmark(
		&prometheus.SpecificConfig{AdapterWriteURL: adapterWriteUrl},
		&source.DataSourceConfig{
			Type: source.FileDataSourceType,
			File: &source.FileDataSourceConfig{Location: config.FileName},
		},
	)
	if err != nil {
		panic(err)
	}
	loader.RunBenchmark(benchmark)
}
