package timescaledb

import (
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
)

func NewTarget() targets.ImplementedTarget {
	return &timescaleTarget{}
}

type timescaleTarget struct {
}

func (t *timescaleTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *timescaleTarget) Benchmark(dataSourceConfig *source.DataSourceConfig, v *viper.Viper) (targets.Benchmark, error) {
	var loadingOptions LoadingOptions
	if err := v.UnmarshalExact(&loadingOptions); err != nil {
		return nil, err
	}
	return newBenchmark(&loadingOptions, dataSourceConfig)
}
