package main

import (
	"bytes"
	"fmt"
	"github.com/blagojts/viper"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"github.com/timescale/tsbs/pkg/targets/initializers"
	"gopkg.in/yaml.v2"
	"strings"
)

const (
	dataSourceFlag = "data-source"
	targetDbFlag   = "target"

	writeConfigTo = "./config.yaml"
)

func initConfigCMD() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Generate example config yaml file and save it to" + writeConfigTo,
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
	return cmd
}

func config(cmd *cobra.Command, _ []string) {
	dataSourceSelected := readFlag(cmd, dataSourceFlag)
	targetSelected := readFlag(cmd, targetDbFlag)

	exampleConfig := getEmptyConfigWithoutDbSpecifics(targetSelected, dataSourceSelected)
	target := initializers.GetTarget(targetSelected)
	v := setExampleConfigInViper(exampleConfig, target)

	if err := v.WriteConfigAs(writeConfigTo); err != nil {
		panic(fmt.Errorf("could not write sample config to file %s: %v", writeConfigTo, err))
	}
	fmt.Printf("Wrote example config to: %s\n", writeConfigTo)
}

func getEmptyConfigWithoutDbSpecifics(target, dataSource string) *LoadConfig {
	loadConfig := &LoadConfig{
		Loader: &LoaderConfig{
			Target: target,
		},
	}
	switch dataSource {
	case source.FileDataSourceType:
		loadConfig.DataSource = &DataSourceConfig{
			Type: source.FileDataSourceType,
		}
	case source.SimulatorDataSourceType:
		loadConfig.DataSource = &DataSourceConfig{
			Type: source.SimulatorDataSourceType,
		}
	}
	return loadConfig
}

func readFlag(cmd *cobra.Command, flag string) string {
	val, err := cmd.PersistentFlags().GetString(flag)
	if err != nil {
		panic(fmt.Sprintf("could not read value for %s flag: %v", dataSourceFlag, err))
	}
	return val
}

func setExampleConfigInViper(confWithoutDBSpecifics *LoadConfig, t targets.ImplementedTarget) *viper.Viper {
	v := viper.New()
	v.SetConfigType("yaml")

	// convert LoaderConfig to yaml to load into viper
	configInBytes, err := yaml.Marshal(confWithoutDBSpecifics)
	if err != nil {
		panic(fmt.Errorf("could not convert example config to yaml: %v", err))
	}

	if err := v.ReadConfig(bytes.NewBuffer(configInBytes)); err != nil {
		panic(fmt.Errorf("could not load example config in viper: %v", err))
	}

	// get loader.runner and data-source flags
	// and remove either data-source.file or data-source.simulator depending on selected
	// data source type
	loadCmdFlagSet := cleanDataSourceFlags(confWithoutDBSpecifics.DataSource.Type, loadCmdFlags())

	// bind loader.runner and data-source flags
	if err := v.BindPFlags(loadCmdFlagSet); err != nil {
		panic(fmt.Errorf("could not bind loader.runner and data-source flags in viper: %v", err))
	}

	// get target specific flags
	flagSet := pflag.NewFlagSet("", pflag.ContinueOnError)
	t.TargetSpecificFlags("loader.db-specific.", flagSet)
	// bind target specific flags
	if err := v.BindPFlags(flagSet); err != nil {
		panic(fmt.Errorf("could not bind target specific config flags in viper: %v", err))
	}

	return v
}

func cleanDataSourceFlags(dataSource string, fs *pflag.FlagSet) *pflag.FlagSet {
	var unwantedPrefix string
	switch dataSource {
	case source.FileDataSourceType:
		unwantedPrefix = "data-source.simulator"
	case source.SimulatorDataSourceType:
		unwantedPrefix = "data-source.file"
	default:
		panic("unsupported data source type: " + dataSource)
	}
	reducedFs := pflag.NewFlagSet("", pflag.ContinueOnError)
	fs.VisitAll(func(f *pflag.Flag) {
		if !strings.HasPrefix(f.Name, unwantedPrefix) {
			reducedFs.AddFlag(f)
		}
	})
	return reducedFs
}
