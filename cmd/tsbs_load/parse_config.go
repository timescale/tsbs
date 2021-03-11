package main

import (
	"errors"
	"fmt"
	"github.com/blagojts/viper"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
)

func parseConfig(target targets.ImplementedTarget, v *viper.Viper) (targets.Benchmark, load.BenchmarkRunner, error) {
	dataSourceViper := v.Sub("data-source")
	if dataSourceViper == nil {
		return nil, nil, fmt.Errorf("config file didn't have a top-level 'data-source' object")
	}
	dataSource, err := parseDataSourceConfig(dataSourceViper)
	if err != nil {
		return nil, nil, err
	}
	dataSourceInternal := convertDataSourceConfigToInternalRepresentation(target.TargetName(), dataSource)

	loaderViper := v.Sub("loader")
	if loaderViper == nil {
		return nil, nil, fmt.Errorf("config file didn't have a top-level 'loader' object")
	}

	runnerViper := loaderViper.Sub("runner")
	if runnerViper == nil {
		return nil, nil, fmt.Errorf("config file didn't have loader.runner specified")
	}

	loaderConfig, err := parseRunnerConfig(runnerViper)
	if err != nil {
		return nil, nil, err
	}

	loaderConfigInternal := convertRunnerConfigToInternalRep(loaderConfig)

	dbSpecificViper := loaderViper.Sub("db-specific")
	if dbSpecificViper == nil {
		return nil, nil, fmt.Errorf("config file didn't have loader.db-specific specified")
	}

	benchmark, err := target.Benchmark(loaderConfigInternal.DBName, dataSourceInternal, dbSpecificViper)
	if err != nil {
		return nil, nil, err
	}

	return benchmark, load.GetBenchmarkRunner(*loaderConfigInternal), nil
}

func parseRunnerConfig(v *viper.Viper) (*RunnerConfig, error) {
	var runnerConfig RunnerConfig
	if err := v.Unmarshal(&runnerConfig); err != nil {
		return nil, err
	}
	return &runnerConfig, nil
}

func convertRunnerConfigToInternalRep(r *RunnerConfig) *load.BenchmarkRunnerConfig {
	return &load.BenchmarkRunnerConfig{
		DBName:          r.DBName,
		BatchSize:       r.BatchSize,
		Workers:         r.Workers,
		Limit:           r.Limit,
		DoLoad:          r.DoLoad,
		DoCreateDB:      r.DoCreateDB,
		DoAbortOnExist:  r.DoAbortOnExist,
		ReportingPeriod: r.ReportingPeriod,
		Seed:            r.Seed,
		HashWorkers:     r.HashWorkers,
		InsertIntervals: r.InsertIntervals,
		NoFlowControl:   !r.FlowControl,
		ChannelCapacity: r.ChannelCapacity,
	}
}

func validateSourceType(t string) error {
	for _, validType := range source.ValidDataSourceTypes {
		if t == validType {
			return nil
		}
	}
	return errors.New(fmt.Sprintf("data source type '%s' unrecognized; allowed: %v", t, source.ValidDataSourceTypes))
}

func parseDataSourceConfig(v *viper.Viper) (*DataSourceConfig, error) {
	var conf DataSourceConfig
	if err := v.Unmarshal(&conf); err != nil {
		return nil, err
	}
	if err := validateSourceType(conf.Type); err != nil {
		return nil, err
	}

	if conf.Type == source.FileDataSourceType {
		if conf.File == nil {
			errStr := fmt.Sprintf(
				"specified type %s, but no file data source config provided",
				source.FileDataSourceType,
			)
			return nil, errors.New(errStr)
		}
		return &conf, nil
	}

	if conf.Simulator == nil {
		errStr := fmt.Sprintf(
			"specified type %s, but no simulator data source config provided",
			source.SimulatorDataSourceType,
		)
		return nil, errors.New(errStr)
	}
	return &conf, nil
}

func convertDataSourceConfigToInternalRepresentation(format string, d *DataSourceConfig) *source.DataSourceConfig {
	var file *source.FileDataSourceConfig
	var simulator *common.DataGeneratorConfig
	if d.Type == source.FileDataSourceType {
		file = &source.FileDataSourceConfig{
			Location: d.File.Location,
		}
	} else {
		simulator = &common.DataGeneratorConfig{
			BaseConfig: common.BaseConfig{
				Format:    format,
				Seed:      d.Simulator.Seed,
				Use:       d.Simulator.Use,
				Scale:     d.Simulator.Scale,
				TimeStart: d.Simulator.TimeStart,
				TimeEnd:   d.Simulator.TimeEnd,
				Debug:     d.Simulator.Debug,
			},
			Limit:                 d.Simulator.Limit,
			LogInterval:           d.Simulator.LogInterval,
			MaxMetricCountPerHost: d.Simulator.MaxMetricCountPerHost,
			InterleavedNumGroups:  1,
		}
	}
	return &source.DataSourceConfig{
		Type:      d.Type,
		File:      file,
		Simulator: simulator,
	}
}
