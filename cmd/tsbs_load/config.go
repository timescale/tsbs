package main

import "github.com/timescale/tsbs/pkg/data/source"

type LoadConfig struct {
	DataSource *source.DataSourceConfig `yaml:"dataSource"`
}

// Validate a LoadConfig object, it returns the first
// error it encounters
func (l *LoadConfig) Validate() error {
	return l.DataSource.Validate()
}
