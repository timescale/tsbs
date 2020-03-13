package main

import (
	"github.com/spf13/cobra"
)

var (
	cfgFile string
	rootCmd = &cobra.Command{
		Use:   "tsbs_load",
		Short: "Load data inside a db",
	}
)

func init() {
	loadCmd, err := initLoadCMD()
	if err != nil {
		panic(err)
	}
	rootCmd.AddCommand(loadCmd)
	configCmd := initConfigCMD()
	rootCmd.AddCommand(configCmd)
}
