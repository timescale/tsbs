package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"github.com/timescale/tsbs/pkg/targets/prometheus"
	"github.com/timescale/tsbs/pkg/targets/timescaledb"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"strings"
	"time"
)

const (
	dataSourceFlag = "data-source"
	targetDbFlag   = "target"
	useCaseFlag    = "use-case"
)

var (
	exampleTimescaleLoadOptions = &timescaledb.LoadingOptions{
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
	}
	examplePromLoadOptions = &prometheus.SpecificConfig{AdapterWriteURL: "http://localhost:9201/write"}
	exampleLoaderConfig    = LoaderConfig{
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
	}
	exampleConfigFromSimulator = LoadConfig{
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
	cmd.PersistentFlags().String(
		targetDbFlag,
		constants.FormatPrometheus,
		"specify target db, valid: "+strings.Join(constants.SupportedFormats(), ", "),
	)
	cmd.PersistentFlags().String(
		useCaseFlag,
		"devops-generic",
		"specify use case to be simulated, only valid with SIMULATOR data source",
	)
	return cmd
}

func config(cmd *cobra.Command, _ []string) {
	dataSourceSelected, err := cmd.PersistentFlags().GetString(dataSourceFlag)
	if err != nil {
		panic(fmt.Sprintf("could not read value for %s flag: %v", dataSourceFlag, err))
	}
	targetSelected, err := cmd.PersistentFlags().GetString(targetDbFlag)
	if err != nil {
		panic(fmt.Sprintf("could not read value for %s flag; %v", targetDbFlag, err))
	}

	useCaseSelected, err := cmd.PersistentFlags().GetString(useCaseFlag)
	if err != nil {
		panic(fmt.Sprintf("could not read value for %s flag; %v", useCaseFlag, err))
	}
	var exampleConfig LoadConfig
	if dataSourceSelected == source.SimulatorDataSourceType {
		exampleConfig = exampleConfigFromSimulator
		exampleConfig.DataSource.Simulator.Use = useCaseSelected
	} else {
		exampleConfig = exampleConfigFromFile
	}
	dbSpecificConfig := getDBSpecificConfig(targetSelected)
	exampleConfig.Loader.DBSpecific = dbSpecificConfig
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

func getDBSpecificConfig(target string) interface{} {
	if target == constants.FormatPrometheus {
		return examplePromLoadOptions
	} else if target == constants.FormatTimescaleDB {
		return exampleTimescaleLoadOptions
	}
	panic("no example loader config for format:" + target + "; only for timescaledb and prometheus")
}
