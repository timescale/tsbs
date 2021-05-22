package siridb

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
)

func NewTarget() targets.ImplementedTarget {
	return &siriTarget{}
}

type siriTarget struct {
}

func (t *siriTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"dbuser", "iris", "Username to enter SiriDB")
	flagSet.String(flagPrefix+"dbpass", "siri", "Password to enter SiriDB")

	flagSet.String(flagPrefix+"hosts", "localhost:9000", "Provide 1 or 2 (comma seperated) SiriDB hosts. If 2 hosts are provided, 2 pools are created.")
	flagSet.Bool(flagPrefix+"replica", false, "Whether to create a replica instead of a second pool, when two hosts are provided.")

	flagSet.Bool(flagPrefix+"log-batches", false, "Whether to time individual batches.")
	flagSet.Int(flagPrefix+"write-timeout", 10, "Write timeout.")
}

func (t *siriTarget) TargetName() string {
	return constants.FormatSiriDB
}

func (t *siriTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *siriTarget) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	panic("not implemented")
}
