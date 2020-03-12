package main

import (
	"fmt"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets/prometheus"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// runs the benchmark
var loader *load.BenchmarkRunner

var adapterWriteUrl string

func init() {
	var config load.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.StringVar(&adapterWriteUrl, "adapter-write-url", "", "Prometheus adapter url to send data to")
	pflag.Parse()

	err := utils.SetupConfigFile()
	if err != nil {
		panic(fmt.Errorf("error setting up a config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	loader = load.GetBenchmarkRunner(config)
}

func main() {
	benchmark, err := prometheus.NewBenchmark(
		&prometheus.SpecificConfig{AdapterWriteURL: adapterWriteUrl},
		&source.DataSourceConfig{
			Type: source.FileDataSourceType,
			File: &source.FileDataSourceConfig{Location: loader.FileName},
		},
	)
	if err != nil {
		panic(err)
	}
	loader.RunBenchmark(benchmark)
}
