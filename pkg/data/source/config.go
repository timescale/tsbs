package source

import (
	"errors"
	"fmt"
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
)

const (
	FileDataSourceType      = "FILE"
	SimulatorDataSourceType = "SIMULATOR"
)

var (
	validDataSourceTypes = [2]string{FileDataSourceType, SimulatorDataSourceType}
)

type DataSourceConfig struct {
	Type      string                      `yaml:"type"`
	File      *FileDataSourceConfig       `yaml:"file,omitempty"`
	Simulator *common.DataGeneratorConfig `yaml:"simulator,omitempty"`
}

func validateSourceType(t string) error {
	for _, validType := range validDataSourceTypes {
		if t == validType {
			return nil
		}
	}
	return errors.New(fmt.Sprintf("data source type '%s' unrecognized; allowed: %v", t, validDataSourceTypes))
}

func ParseDataSourceConfig(v *viper.Viper) (*DataSourceConfig, error) {
	var conf DataSourceConfig
	if err := v.UnmarshalExact(&conf); err != nil {
		return nil, err
	}
	if err := validateSourceType(conf.Type); err != nil {
		return nil, err
	}

	if conf.Type == FileDataSourceType {
		if conf.File == nil {
			errStr := fmt.Sprintf(
				"specified type %s, but no file data source config provided",
				FileDataSourceType,
			)
			return nil, errors.New(errStr)
		}
		return &conf, nil
	}

	if conf.Simulator == nil {
		errStr := fmt.Sprintf(
			"specified type %s, but no simulator data source config provided",
			SimulatorDataSourceType,
		)
		return nil, errors.New(errStr)
	}
	return &conf, nil
}

