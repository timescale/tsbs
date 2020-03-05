package main

import (
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/initializers"
)

type loadConfig struct {
	format string `yaml:"format"`
	dataSource *source.DataSourceConfig `yaml:"data-source,omitempty"`
	loader *loaderConfig `yaml:"loader",omitempty`
}

type loaderConfig struct {
	runner *load.BenchmarkRunnerConfig `yaml:"runner"`
	dbSpecific interface{} `yaml:"db-specific"`
}

func ParseConfig(v *viper.Viper) (targets.Benchmark, *load.BenchmarkRunner, error) {
	format := v.GetString("format")
	target := initializers.GetTarget(format)

	dataSourceViper := v.Sub("data-source")
	dataSource, err := source.ParseDataSourceConfig(dataSourceViper)
	if err != nil {
		return nil, nil, err
	}

	loaderViper := v.Sub("loader")
	loaderConfig, err := parseLoaderConfig(loaderViper)
	if err != nil {
		return nil, nil, err
	}

	benchmark, err := target.Benchmark(dataSource, v.Sub("db-specific"))
	if err != nil {
		return nil, nil, err
	}
	return benchmark, load.GetBenchmarkRunner(*loaderConfig), nil
}

func parseLoaderConfig(v *viper.Viper) (*load.BenchmarkRunnerConfig, error) {
	runnerConfigViper := v.Sub("runner")
	var runnerConfig load.BenchmarkRunnerConfig
	if err := runnerConfigViper.UnmarshalExact(&runnerConfig); err != nil {
		return nil, err
	}
	return &runnerConfig, nil
}
