package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets/timescaledb"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"strings"
	"time"
)

const (
	dataSourceFlag = "data-source"
)

var (
	exampleLoaderConfig = LoaderConfig{
		Runner: &RunnerConfig{
			DBName:          "benchmark",
			BatchSize:       10000,
			Workers:         8,
			Limit:           0,
			DoLoad:          true,
			DoCreateDB:      true,
			DoAbortOnExist:  true,
			ReportingPeriod: 10 * time.Second,
			Seed:            1234,
			HashWorkers:     true,
		},
		DBSpecific: &timescaledb.LoadingOptions{
			PostgresConnect:    "sslmode=disable",
			Host:               "localhost",
			DBname:             "benchmark",
			User:               "postgres",
			Pass:               "postgres",
			Port:               "5432",
			ConnDB:             "postgres",
			Driver:             "pgx",
			UseHypertable:      true,
			LogBatches:         true,
			UseJSON:            false,
			InTableTag:         true,
			NumberPartitions:   1,
			ChunkTime:          10 * time.Hour,
			TimeIndex:          false,
			TimePartitionIndex: true,
			PartitionIndex:     false,
			CreateMetricsTable: true,
			ForceTextFormat:    false,
		},
	}
	exampleConfigFromSimulator = LoadConfig{
		Format: "timescaledb",
		DataSource: &DataSourceConfig{
			Type: source.SimulatorDataSourceType,
			Simulator: &SimulatorDataSourceConfig{
				Use:                   "devops",
				Scale:                 10,
				Seed:                  1234,
				TimeEnd:               "2020-01-02T00:00:00Z",
				TimeStart:             "2020-01-01T00:00:00Z",
				Limit:                 0,
				LogInterval:           10 * time.Second,
				Debug:                 0,
				MaxMetricCountPerHost: 10,
			},
		},
		Loader: &exampleLoaderConfig,
	}

	exampleConfigFromFile = LoadConfig{
		Format: "timescaledb",
		DataSource: &DataSourceConfig{
			Type: source.FileDataSourceType,
			File: &FileDataSourceConfig{Location: "some/location/to/file/generated/with/tsbs_generate_data"},
		},
		Loader: &exampleLoaderConfig,
	}
)

func initConfigCMD() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Generate example config yaml file and print it to STDOUT",
		Run:   config,
	}

	cmd.PersistentFlags().String(
		dataSourceFlag,
		source.SimulatorDataSourceType,
		"specify data source, valid:"+strings.Join(source.ValidDataSourceTypes, ", "),
	)
	return cmd
}

func config(cmd *cobra.Command, _ []string) {
	dataSourceSelected, err := cmd.PersistentFlags().GetString(dataSourceFlag)
	if err != nil {
		panic(fmt.Sprintf("could not read value for %s flag: %v", dataSourceFlag, err))
	}
	var exampleConfig LoadConfig
	if dataSourceSelected == source.SimulatorDataSourceType {
		exampleConfig = exampleConfigFromSimulator
	} else {
		exampleConfig = exampleConfigFromFile
	}
	serializedConfig, err := yaml.Marshal(&exampleConfig)
	if err != nil {
		panic(err)
	}
	if err := ioutil.WriteFile("./config.yaml", serializedConfig, 0644); err != nil {
		fmt.Printf("could not write example config in ./config.yaml: %v", err)
	} else {
		fmt.Println("Example config written in ./config.yaml")
	}
}
