package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets/timescaledb"
	"gopkg.in/yaml.v2"
)

var (
	cfgFile string
	rootCmd = &cobra.Command{
		Use:   "tsbs_load",
		Short: "Load data inside a db",
	}
	configFromSimCmd = &cobra.Command{
		Use:   "tsbs_load config-from-sim",
		Short: "Generate example config yaml that generates the data on the fly",
		Run:   generateConfigForSim,
	}
	configFromFileCmd = &cobra.Command{
		Use:   "tsbs_load config-from-file",
		Short: "Generate example config yaml that loads the data from a pre-generated file",
		Run:   generateConfigForFile,
	}
)

func generateConfigForSim(cmd *cobra.Command, args []string) {

}

func generateConfigForFile(cmd *cobra.Command, args []string) {
	exampleLoadConfig := loadConfig{
		format: "timescaledb",
		dataSource: &source.DataSourceConfig{
			Type: source.FileDataSourceType,
			File: &source.FileDataSourceConfig{Location: "some/location/to/file/generated/with/tsbs_generate_data"},
		},
		loader: &loaderConfig{
			runner: &load.BenchmarkRunnerConfig{
				DBName:          "target-db",
				BatchSize:       10000,
				Workers:         8,
				Limit:           0,
				DoLoad:          true,
				DoCreateDB:      true,
				DoAbortOnExist:  true,
				ReportingPeriod: 10,
				Seed:            1234,
			},
			dbSpecific: &timescaledb.LoadingOptions{
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
				HashWorkers:        false,
				NumberPartitions:   1,
				ChunkTime:          100,
				TimeIndex:          false,
				TimePartitionIndex: true,
				PartitionIndex:     false,
				CreateMetricsTable: true,
				ForceTextFormat:    false,
			},
		},
	}
	serializedConfig, err := yaml.Marshal(exampleLoadConfig)
	if err != nil {
		panic(err)
	}
	fmt.Println("Example config for reading a timescaledb format file, and loading it into TimescaleDB")
	fmt.Println("-------------------------------------------------------------------------------------")
	fmt.Print(string(serializedConfig))
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.cobra.yaml)")
	rootCmd.PersistentFlags().Bool("viper", true, "use Viper for configuration")
	viper.BindPFlag("useViper", rootCmd.PersistentFlags().Lookup("viper"))

	rootCmd.AddCommand(configFromFileCmd)
	rootCmd.AddCommand(configFromSimCmd)
}

func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Search config in execution directory with name "cobra.yaml" (without extension).
		viper.AddConfigPath(".")
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
	}

	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
