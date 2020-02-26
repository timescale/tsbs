package timescaledb

import (
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/pkg/targets"
)

func NewTarget() targets.ImplementedTarget {
	return &timescaleTarget{}
}

type timescaleTarget struct {
}

func (t *timescaleTarget) Benchmark() targets.Benchmark {
	return nil
}

func (t *timescaleTarget) ParseLoaderConfig(v *viper.Viper) (interface{}, error) {
	var loadingOptions LoadingOptions
	if err := v.UnmarshalExact(&loadingOptions); err != nil {
		return nil, err
	}
	return &loadingOptions, nil
}
