package source

import (
	"errors"
	"fmt"
)

const (
	FileDataSourceType      = "FILE"
	SimulatorDataSourceType = "SIMULATOR"
)

var (
	validDataSourceTypes = [2]string{FileDataSourceType, SimulatorDataSourceType}
)

type DataSourceConfig struct {
	Type      string                     `yaml:"type"`
	File      *FileDataSourceConfig      `yaml:"file,omitempty"`
	Simulator *SimulatorDataSourceConfig `yaml:"simulator,omitempty"`
}

type SimulatorDataSourceConfig struct{}

func validateSourceType(t string) error {
	for _, validType := range validDataSourceTypes {
		if t == validType {
			return nil
		}
	}
	return errors.New(fmt.Sprintf("data source type '%s' unrecognized; allowed: %v", t, validDataSourceTypes))
}

// Validate a DataSourceConfig object returning the first problem
// it encounters as an error.
func (d *DataSourceConfig) Validate() error {
	if err := validateSourceType(d.Type); err != nil {
		return err
	}

	if d.Type == FileDataSourceType {
		if d.File == nil {
			errStr := fmt.Sprintf(
				"specified type %s, but no file data source config provided",
				FileDataSourceType,
			)
			return errors.New(errStr)
		}
		return d.File.Validate()
	}

	if d.Simulator == nil {
		errStr := fmt.Sprintf(
			"specified type %s, but no simulator data source config provided",
			SimulatorDataSourceType,
		)
		return errors.New(errStr)
	}
	return d.Simulator.Validate()
}

func (f *SimulatorDataSourceConfig) Validate() error {
	return nil
}
