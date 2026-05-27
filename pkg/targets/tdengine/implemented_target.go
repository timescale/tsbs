package tdengine

import (
	"bytes"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
)

func NewTarget() targets.ImplementedTarget {
	return &tdengineTarget{}
}

type tdengineTarget struct {
}

func (t *tdengineTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	TargetSpecificFlags(flagPrefix, flagSet)
}

func TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"user", "root", "User to connect to TDengine")
	flagSet.String(flagPrefix+"pass", "taosdata", "Password for user connecting to TDengine")
	flagSet.String(flagPrefix+"host", "", "TDengine host")
	flagSet.Int(flagPrefix+"port", 6030, "TDengine Port")
	flagSet.Int(flagPrefix+"vgroups", 0, "TDengine DB vgroups")
	flagSet.Int(flagPrefix+"pages", 0, "TDengine DB pages")
	flagSet.Int(flagPrefix+"buffer", 0, "TDengine DB buffer")
	flagSet.Int(flagPrefix+"stt_trigger", 0, "TDengine DB stt_trigger")
	flagSet.Int(flagPrefix+"wal_fsync_period", 3000, "TDengine DB wal_fsync_period")
	flagSet.Int(flagPrefix+"wal_level", 1, "TDengine DB wal_level")
	flagSet.String(flagPrefix+"db_parameters", "", "TDengine DB parameters")
}

func (t *tdengineTarget) TargetName() string {
	return constants.FormatTDengine
}

func (t *tdengineTarget) Serializer() serialize.PointSerializer {
	return &Serializer{
		tableMap:   map[string]struct{}{},
		superTable: map[string]*Table{},
		tmpBuf:     &bytes.Buffer{},
	}
}

func (t *tdengineTarget) Benchmark(targetDB string, dataSourceConfig *source.DataSourceConfig, v *viper.Viper,
) (targets.Benchmark, error) {
	var loadingOptions LoadingOptions
	if err := v.Unmarshal(&loadingOptions); err != nil {
		return nil, err
	}
	return NewBenchmark(targetDB, &loadingOptions, dataSourceConfig)
}
