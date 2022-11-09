package iotdb

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
)

func NewTarget() targets.ImplementedTarget {
	return &iotdbTarget{}
}

type iotdbTarget struct {
}

func (t *iotdbTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"host", "localhost", "Hostname of IoTDB instance")
	flagSet.String(flagPrefix+"port", "6667", "Which port to connect to on the database host")
	flagSet.String(flagPrefix+"user", "root", "The user who connect to IoTDB")
	flagSet.String(flagPrefix+"password", "root", "The password for user connecting to IoTDB")
	flagSet.Int(flagPrefix+"timeout", 0, "Session timeout check in millisecond. Use 0 for no timeout.")
}

func (t *iotdbTarget) TargetName() string {
	return constants.FormatIoTDB
}

func (t *iotdbTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *iotdbTarget) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	// benchmark for tsbs_load_iotdb is implemented in "cmd/tsbs_load_iotdb/main.go/main()"
	panic("Benchmark() not implemented! Benchmark for tsbs_load_iotdb is implemented in \"cmd/tsbs_load_iotdb/main.go/main()\"")
}
