package crate

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
)

func NewTarget() targets.ImplementedTarget {
	return &crateTarget{}
}

type crateTarget struct {
}

func (t *crateTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"hosts", "localhost", "CrateDB hostnames")
	flagSet.Uint(flagPrefix+"port", 5432, "A port to connect to database instances")
	flagSet.String(flagPrefix+"user", "crate", "User to connect to CrateDB")
	flagSet.String(flagPrefix+"pass", "", "Password for user connecting to CrateDB")
	flagSet.Int(flagPrefix+"replicas", 0, "Number of replicas per a metric table")
	flagSet.Int(flagPrefix+"shards", 5, "Number of shards per a metric table")
}

func (t *crateTarget) TargetName() string {
	return constants.FormatCrateDB
}

func (t *crateTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *crateTarget) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	panic("not implemented")
}
