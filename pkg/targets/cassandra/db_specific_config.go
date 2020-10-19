package cassandra

import (
	"github.com/blagojts/viper"
	"time"
)

type SpecificConfig struct {
	Hosts             string        `yaml:"hosts" mapstructure:"hosts"`
	ReplicationFactor int           `yaml:"replication-factor" mapstructure:"replication-factor"`
	ConsistencyLevel  string        `yaml:"consistency" mapstructure:"consistency"`
	WriteTimeout      time.Duration `yaml:"write-timeout" mapstructureL:"write-timeout"`
}

func parseSpecificConfig(v *viper.Viper) (*SpecificConfig, error) {
	var conf SpecificConfig
	if err := v.Unmarshal(&conf); err != nil {
		return nil, err
	}
	return &conf, nil
}
