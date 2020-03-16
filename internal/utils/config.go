package utils

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
)

// SetupConfigFile defines the settings for the configuration file support.
func SetupConfigFile() error {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")

	viper.BindPFlags(pflag.CommandLine)

	if err := viper.ReadInConfig(); err != nil {
		// Ignore error if config file not found.
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return err
		}
	}

	return nil
}
