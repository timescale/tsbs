package timestream

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
)

type SpecificConfig struct {
	UseCommonAttributes      bool   `yaml:"use-common-attributes" mapstructure:"use-common-attributes"`
	AwsRegion                string `yaml:"aws-region" mapstructure:"aws-region"`
	HashProperty             string `yaml:"hash-property" mapstructure:"hash-property"`
	UseCurrentTime           bool   `yaml:"use-current-time" mapstructure:"use-current-time"`
	MagStoreRetentionInDays  int64  `yaml:"mag-store-retention-in-days" mapstructure:"mag-store-retention-in-days"`
	MemStoreRetentionInHours int64  `yaml:"mem-store-retention-in-hours" mapstructure:"mem-store-retention-in-hours"`
}

func parseSpecificConfig(v *viper.Viper) (*SpecificConfig, error) {
	var conf SpecificConfig
	if err := v.Unmarshal(&conf); err != nil {
		return nil, err
	}
	return &conf, nil
}

func targetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.Bool(
		flagPrefix+"use-common-attributes",
		true,
		"Timestream client makes write requests with common attributes. "+
			"If false, each value is written as a separate Record and a request of 100 records at once is sent")
	flagSet.String(flagPrefix+"aws-region", "us-east-1", "AWS region where the db is located")
	flagSet.String(
		flagPrefix+"hash-property",
		"hostname",
		"Dimension to use when hashing points to different workers",
	)
	flagSet.Bool(
		flagPrefix+"use-current-time",
		false,
		"Use the local current timestamp when generating the records to load")
	flagSet.Int64(
		"mag-store-retention-in-days",
		180,
		"The duration for which data must be stored in the magnetic store",
	)
	flagSet.Int64(
		flagPrefix+"mem-store-retention-in-hours",
		12,
		"The duration for which data must be stored in the memory store")
}
