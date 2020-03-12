package akumuli

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
)

func NewTarget() targets.ImplementedTarget {
	return &akumuliTarget{}
}

type akumuliTarget struct {
}

func (t *akumuliTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"endpoint", "http://localhost:8282", "Akumuli RESP endpoint IP address.")
}

func (t *akumuliTarget) TargetName() string {
	return constants.FormatAkumuli
}

func (t *akumuliTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *akumuliTarget) Benchmark(*source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	panic("not implemented")
}
