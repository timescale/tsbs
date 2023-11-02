package openmetrics

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
)

func NewTarget() targets.ImplementedTarget {
	return &openMetricsTarget{}
}

type openMetricsTarget struct {
}

func (t *openMetricsTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"url", "http://localhost:9091/", "Prometheus Pushgateway endpoint")
}

func (t *openMetricsTarget) TargetName() string {
	return constants.FormatOpenMetrics
}

func (t *openMetricsTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *openMetricsTarget) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	panic("not implemented")
}
