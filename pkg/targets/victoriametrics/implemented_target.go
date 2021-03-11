package victoriametrics

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"github.com/timescale/tsbs/pkg/targets/influx"
)

func NewTarget() targets.ImplementedTarget {
	return &vmTarget{}
}

type vmTarget struct {
}

func (vm vmTarget) Benchmark(_ string, dataSourceConfig *source.DataSourceConfig, v *viper.Viper) (targets.Benchmark, error) {
	vmSpecificConfig, err := parseSpecificConfig(v)
	if err != nil {
		return nil, err
	}

	return NewBenchmark(vmSpecificConfig, dataSourceConfig)
}

func (vm vmTarget) Serializer() serialize.PointSerializer {
	return &influx.Serializer{}
}

func (vm vmTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(
		flagPrefix+"urls",
		"http://localhost:8428/write",
		"Comma-separated list of VictoriaMetrics ingestion URLs(single-node or VMInsert)",
	)
}

func (vm vmTarget) TargetName() string {
	return constants.FormatVictoriaMetrics
}
