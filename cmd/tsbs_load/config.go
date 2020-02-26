package main

import (
	"errors"
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/initializers"
	"strings"
)

type LoadConfig struct {
	Format     string                   `yaml:"format"`
	DataSource *source.DataSourceConfig `yaml:"data-source"`
	Loader     *LoaderConfig            `yaml:"loader"`
}

type LoaderConfig struct {
	Runner     *load.BenchmarkRunnerConfig `yaml:"runner"`
	DBSpecific interface{}                 `yaml:"db-specific"`
}

func ParseLoadConfig(v *viper.Viper) (*LoadConfig, targets.ImplementedTarget, error) {
	format := v.GetString("format")
	if !utils.IsIn(format, targets.SupportedFormats()) {
		return nil, nil, errors.New("unsupported target, supported: " + strings.Join(targets.SupportedFormats(), ","))
	}
	if format == "" {
		return nil, nil, errors.New("load config -> format unspecified")
	}
	dataSourceViper := v.Sub("data-source")
	dataSource, err := source.ParseDataSourceConfig(dataSourceViper)
	if err != nil {
		return nil, nil, err
	}

	target := initializers.GetTarget(format)
	loaderViper := v.Sub("loader")
	loaderConfig, err := parseLoaderConfig(target, loaderViper)
	if err != nil {
		return nil, nil, err
	}
	return &LoadConfig{
		Format:     format,
		DataSource: dataSource,
		Loader:     loaderConfig,
	}, target, nil
}

func parseLoaderConfig(target targets.ImplementedTarget, v *viper.Viper) (*LoaderConfig, error) {
	runnerConfigViper := v.Sub("runner")
	var runnerConfig load.BenchmarkRunnerConfig
	if err := runnerConfigViper.UnmarshalExact(&runnerConfig); err != nil {
		return nil, err
	}
	specific, err := target.ParseLoaderConfig(v.Sub("db-specific"))
	if err != nil {
		return nil, err
	}
	return &LoaderConfig{
		Runner:     &runnerConfig,
		DBSpecific: specific,
	}, nil
}
