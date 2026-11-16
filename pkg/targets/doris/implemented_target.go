package doris

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"github.com/timescale/tsbs/pkg/targets/timescaledb"
)

func NewTarget() targets.ImplementedTarget {
	return &dorisTarget{}
}

type dorisTarget struct{}

func (c dorisTarget) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	panic("you must implement me")
}

func (c dorisTarget) Serializer() serialize.PointSerializer {
	return &timescaledb.Serializer{}
}

func (c dorisTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"host", "localhost", "Hostname of Doris instance")
	flagSet.String(flagPrefix+"port", "9030", "Port of Doris's mysql client")
	flagSet.String(flagPrefix+"user", "root", "User to connect to Doris as(default: root)")
	flagSet.String(flagPrefix+"password", "", "Password for user connecting to Doris(default: null)")
	flagSet.Bool(flagPrefix+"log-batches", false, "Whether to time individual batches.")
	flagSet.Int(flagPrefix+"debug", 0, "Debug printing (choices: 0, 1, 2). (default 0)")
}

func (c dorisTarget) TargetName() string {
	return constants.FormatDoris
}
