package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/blagojts/viper"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"github.com/timescale/tsbs/pkg/targets/initializers"
	"strings"
	"time"
)

type cmdRunner func(*cobra.Command, []string)

func initLoadCMD() (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:   "load",
		Short: "Load data into a specified target database",
	}
	err := addFlagsToLoadCommand(cmd)
	if err != nil {
		return nil, err
	}

	subCommands := initLoadSubCommands()
	cmd.AddCommand(subCommands...)
	return cmd, nil
}

func addFlagsToLoadCommand(cmd *cobra.Command) error {
	fs := cmd.PersistentFlags()
	fs.String(
		"data-source.type",
		source.SimulatorDataSourceType,
		"Where to load the data from. Valid: "+strings.Join(source.ValidDataSourceTypes, ", "),
	)
	fs.String(
		"data-source.file.location",
		"./file-from-tsbs-generate-data",
		"If data-source.type=FILE, load the data from this file location",
	)
	fs.String("loader.runner.db-name", "benchmark", "Name of database")
	fs.Uint(
		"loader.runner.batch-size",
		10000,
		"Number of items to batch together in a single insert",
	)
	fs.Uint("loader.runner.workers", 1, "Number of parallel clients inserting")
	fs.Uint64("loader.runner.limit", 0, "Number of items to insert (0 = all of them).")
	fs.Bool(
		"loader.runner.do-load",
		true,
		"Whether to write data. Set this flag to false to check input read speed.",
	)
	fs.Bool(
		"loader.runner.do-create-db",
		true,
		"Whether to create the database. Disable on all but one client if running on a multi client setup.",
	)
	fs.Bool(
		"loader.runner.do-abort-on-exist",
		false,
		"Whether to abort if a database with the given name already exists.",
	)
	fs.Duration("loader.runner.reporting-period", 10*time.Second, "Period to report write stats")
	fs.String("loader.runner.file", "", "File name to read data from")
	fs.Int64("loader.runner.seed", 0, "PRNG seed (default: 0, which uses the current timestamp)")
	fs.String(
		"loader.runner.insert-intervals",
		"",
		"Time to wait between each insert, default '' => all workers insert ASAP. '1,2' = worker 1 waits 1s "+
			"between inserts, worker 2 and others wait 2s",
	)
	fs.Bool(
		"loader.runner.hash-workers",
		false,
		"Whether to consistently hash insert data to the same workers (i.e., the data for a particular host "+
			"always goes to the same worker)")
	err := viper.BindPFlags(cmd.PersistentFlags())
	if err != nil {
		return fmt.Errorf("could not bind flags to configuration: %v", err)
	}
	return nil
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
