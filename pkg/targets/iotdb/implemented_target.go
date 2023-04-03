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
	return &iotdbTarget{
		BasicPath:      "root",
		BasicPathLevel: 0,
	}
}

type iotdbTarget struct {
	BasicPath      string // e.g. "root.sg" is basic path of "root.sg.device". default : "root"
	BasicPathLevel int32  // e.g. 0 for "root", 1 for "root.device"
}

func (t *iotdbTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"host", "localhost", "Hostname of IoTDB instance")
	flagSet.String(flagPrefix+"port", "6667", "Which port to connect to on the database host")
	flagSet.String(flagPrefix+"user", "root", "The user who connect to IoTDB")
	flagSet.String(flagPrefix+"password", "root", "The password for user connecting to IoTDB")
	flagSet.Int(flagPrefix+"timeout", 0, "Session timeout check in millisecond. Use 0 for no timeout.")
	flagSet.Int(flagPrefix+"records-max-rows", 0, "Max rows of 'InsertRecords'. Use 0 for no limit.")
	flagSet.Bool(flagPrefix+"to-csv", false, "Do not insert into database, but to some CSV files.")
	flagSet.String(flagPrefix+"csv-prefix", "./", "Prefix of filepath for CSV files. Specific a folder or a folder with filename prefix.")
	flagSet.Bool(flagPrefix+"aligned-timeseries", true, "Using aligned timeseries for all metrics if set true.")
	flagSet.Bool(flagPrefix+"store-tags", false, "Store tags if set true. Can NOT be used if aligned-timeseries is set true.")
}

func (t *iotdbTarget) TargetName() string {
	return constants.FormatIoTDB
}

func (t *iotdbTarget) Serializer() serialize.PointSerializer {
	return &Serializer{
		BasicPath:      t.BasicPath,
		BasicPathLevel: t.BasicPathLevel,
	}
}

func (t *iotdbTarget) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	// benchmark for tsbs_load_iotdb is implemented in "cmd/tsbs_load_iotdb/main.go/main()"
	panic("Benchmark() not implemented! Benchmark for tsbs_load_iotdb is implemented in \"cmd/tsbs_load_iotdb/main.go/main()\"")
}
