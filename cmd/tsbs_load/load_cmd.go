package main

import (
	"fmt"
	"github.com/blagojts/viper"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"github.com/timescale/tsbs/pkg/targets/initializers"
)

type cmdRunner func(*cobra.Command, []string)

func initLoadCMD() (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:              "load",
		Short:            "Load data into a specified target database",
		PersistentPreRun: initViperConfig,
	}
	loadCmdFlagSet := loadCmdFlags()
	cmd.PersistentFlags().AddFlagSet(loadCmdFlagSet)
	err := viper.BindPFlags(cmd.PersistentFlags())
	// don't bind --config which specifies the file from where to read config
	cmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./config.yaml)")

	if err != nil {
		return nil, fmt.Errorf("could not bind flags to configuration: %v", err)
	}

	subCommands := initLoadSubCommands()
	cmd.AddCommand(subCommands...)
	return cmd, nil
}

func loadCmdFlags() *pflag.FlagSet {
	fs := pflag.NewFlagSet("", pflag.ContinueOnError)
	addDataSourceFlags(fs)
	addLoaderRunnerFlags(fs)
	return fs
}

func initLoadSubCommands() []*cobra.Command {
	allFormats := constants.SupportedFormats()
	commands := make([]*cobra.Command, len(allFormats))
	for i, format := range allFormats {
		target := initializers.GetTarget(format)
		cmd := &cobra.Command{
			Use:   format,
			Short: "Load data into " + format + " as a target db",
			Run:   createRunLoad(target),
		}

		target.TargetSpecificFlags("loader.db-specific.", cmd.PersistentFlags())
		commands[i] = cmd
	}

	return commands
}

func createRunLoad(target targets.ImplementedTarget) cmdRunner {
	return func(cmd *cobra.Command, args []string) {
		// bind only the flags of the executed sub-command
		// if we bind them at the time when the flags are defined in initLoadSubCommand()
		// then viper will have all the flags for all targets]
		if err := viper.BindPFlags(cmd.PersistentFlags()); err != nil {
			panic(fmt.Errorf("could not bind db-specific flags for %s: %v", target.TargetName(), err))
		}
		bench, runner, err := parseConfig(target, viper.GetViper())
		if err != nil {
			panic(err)
		}
		runner.RunBenchmark(bench)
	}
}

func initViperConfig(*cobra.Command, []string) {
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
