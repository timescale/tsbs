package main

import (
	"fmt"
	"log"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets/ceresdb"
)

func initProgramOptions() (*ceresdb.SpecificConfig, load.BenchmarkRunner, *load.BenchmarkRunnerConfig) {
	target := ceresdb.NewTarget()

	loaderConf := load.BenchmarkRunnerConfig{}
	loaderConf.AddToFlagSet(pflag.CommandLine)
	target.TargetSpecificFlags("", pflag.CommandLine)
	pflag.Parse()

	if err := utils.SetupConfigFile(); err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}
	if err := viper.Unmarshal(&loaderConf); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}
	ceresdbAddr := viper.GetString("ceresdb-addr")
	if len(ceresdbAddr) == 0 {
		log.Fatalf("missing `ceresdb-addr` flag")
	}
	storageFormat := viper.GetString("storage-format")
	primaryKeys := viper.GetString("primary-keys")
	rowGroupSize := viper.GetInt64("row-group-size")
	partitionKeys := viper.GetString("partition-keys")
	partitionNum := viper.GetUint32("partition-num")
	accessMode := viper.GetString("access-mode")
	updateMode := viper.GetString("update-mode")
	loader := load.GetBenchmarkRunner(loaderConf)
	return &ceresdb.SpecificConfig{
		CeresdbAddr:   ceresdbAddr,
		StorageFormat: storageFormat,
		RowGroupSize:  rowGroupSize,
		PrimaryKeys:   primaryKeys,
		PartitionKeys: partitionKeys,
		PartitionNum:  partitionNum,
		AccessMode:    accessMode,
		UpdateMode:    updateMode,
	}, loader, &loaderConf
}

func main() {
	vmConf, loader, loaderConf := initProgramOptions()
	benchmark, err := ceresdb.NewBenchmark(vmConf, &source.DataSourceConfig{
		Type: source.FileDataSourceType,
		File: &source.FileDataSourceConfig{Location: loaderConf.FileName},
	})

	if err != nil {
		panic(err)
	}
	loader.RunBenchmark(benchmark)
}
