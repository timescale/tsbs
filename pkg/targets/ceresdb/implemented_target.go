package ceresdb

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
		flagPrefix+"ceresdb-addr",
		"127.0.0.1:8831",
		"ceresdb gRPC endpoint",
	)
	flagSet.String(
		flagPrefix+"storage-format",
		"columnar",
		"columnar or hybrid",
	)
	flagSet.Int64(flagPrefix+"row-group-size", 8192, "row num per row group in parquet")
	flagSet.String(
		flagPrefix+"primary-keys",
		"tsid,timestamp",
		"Primary keys used when create table",
	)
	flagSet.String(
		flagPrefix+"partition-keys",
		"",
		"Partition keys used when create partitioned table",
	)
	flagSet.Uint32(
		flagPrefix+"partition-num",
		4,
		"Partition keys used when create partitioned table",
	)
	flagSet.String(
		flagPrefix+"access-mode",
		"direct",
		"Access mode of ceresdb client",
	)
	flagSet.String(
		flagPrefix+"update-mode",
		"OVERWRITE",
		"Update mode when insert",
	)
}

func (vm vmTarget) TargetName() string {
	return constants.FormatCeresDB
}
