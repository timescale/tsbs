package timestream

import (
	"github.com/blagojts/viper"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
)

type implementedTarget struct{}

func NewTarget() targets.ImplementedTarget {
	return implementedTarget{}
}

func (i implementedTarget) Benchmark(targetDb string, dataSourceConfig *source.DataSourceConfig, v *viper.Viper) (targets.Benchmark, error) {
	specificConfig, err := parseSpecificConfig(v)
	if err != nil {
		return nil, errors.Wrap(err, "could not create benchmark")
	}
	return newBenchmark(targetDb, specificConfig, dataSourceConfig)
}

func (i implementedTarget) Serializer() serialize.PointSerializer {
	return &serializer{}
}

func (i implementedTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	targetSpecificFlags(flagPrefix, flagSet)
}

func (i implementedTarget) TargetName() string {
	return constants.FormatTimestream
}
