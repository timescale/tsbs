package mongo

import (
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
)

func NewTarget() targets.ImplementedTarget {
	return &mongoTarget{}
}

type mongoTarget struct {
}

func (t *mongoTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *mongoTarget) Benchmark(dataSourceConfig *source.DataSourceConfig, v *viper.Viper) (targets.Benchmark, error) {
	panic("not implemented")
}
