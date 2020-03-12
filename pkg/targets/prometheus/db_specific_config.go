package prometheus

import "github.com/spf13/viper"

type SpecificConfig struct {
	AdapterWriteURL string `yaml:"adapter-write-url" mapstructure:"adapter-write-url"`
}

func parseSpecificConfig(v *viper.Viper) (*SpecificConfig, error) {
	var conf SpecificConfig
	if err := v.UnmarshalExact(&conf); err != nil {
		return nil, err
	}
	return &conf, nil
}
