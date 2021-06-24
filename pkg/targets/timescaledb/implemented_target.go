package timescaledb

import (
	"time"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
)

func NewTarget() targets.ImplementedTarget {
	return &timescaleTarget{}
}

type timescaleTarget struct {
}

func (t *timescaleTarget) TargetName() string {
	return constants.FormatTimescaleDB
}

func (t *timescaleTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *timescaleTarget) Benchmark(
	targetDB string, dataSourceConfig *source.DataSourceConfig, v *viper.Viper,
) (targets.Benchmark, error) {
	var loadingOptions LoadingOptions
	if err := v.Unmarshal(&loadingOptions); err != nil {
		return nil, err
	}
	return NewBenchmark(targetDB, &loadingOptions, dataSourceConfig)
}

func (t *timescaleTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"postgres", "sslmode=disable", "PostgreSQL connection string")
	flagSet.String(flagPrefix+"host", "localhost", "Hostname of TimescaleDB (PostgreSQL) instance")
	flagSet.String(flagPrefix+"port", "5432", "Which port to connect to on the database host")
	flagSet.String(flagPrefix+"user", "postgres", "User to connect to PostgreSQL as")
	flagSet.String(flagPrefix+"pass", "", "Password for user connecting to PostgreSQL (leave blank if not password protected)")
	flagSet.String(flagPrefix+"admin-db-name", "postgres", "Database to connect to in order to create additional benchmark databases.\n"+
		"By default this is the same as the `user` (i.e., `postgres` if neither is set),\n"+
		"but sometimes a user does not have its own database.")

	flagSet.Bool(flagPrefix+"log-batches", false, "Whether to time individual batches.")

	flagSet.Bool(flagPrefix+"use-hypertable", true, "Whether to make the table a hypertable. Set this flag to false to check input write speed against regular PostgreSQL.")
	flagSet.Bool(flagPrefix+"use-jsonb-tags", false, "Whether tags should be stored as JSONB (instead of a separate table with schema)")
	flagSet.Bool(flagPrefix+"in-table-partition-tag", false, "Whether the partition key (e.g. hostname) should also be in the metrics hypertable")

	flagSet.Int(flagPrefix+"replication-factor", 0, "Setting replication factor >= 1 will create a distributed hypertable")
	flagSet.Int(flagPrefix+"partitions", 0, "Number of partitions")
	flagSet.Duration(flagPrefix+"chunk-time", 12*time.Hour, "Duration that each chunk should represent, e.g., 12h")

	flagSet.Bool(flagPrefix+"time-index", true, "Whether to build an index on the time dimension")
	flagSet.Bool(flagPrefix+"time-partition-index", false, "Whether to build an index on the time dimension, compounded with partition")
	flagSet.Bool(flagPrefix+"partition-index", true, "Whether to build an index on the partition key")
	flagSet.String(flagPrefix+"field-index", ValueTimeIdx, "index types for tags (comma delimited)")
	flagSet.Int(flagPrefix+"field-index-count", 0, "Number of indexed fields (-1 for all)")

	flagSet.String(flagPrefix+"write-profile", "", "File to output CPU/memory profile to")
	flagSet.String(flagPrefix+"write-replication-stats", "", "File to output replication stats to")
	flagSet.Bool(flagPrefix+"create-metrics-table", true, "Drops existing and creates new metrics table. Can be used for both regular and hypertable")

	flagSet.Bool(flagPrefix+"use-insert", false, "Provides the option to test data inserts with batched INSERT commands rather than the preferred COPY function")
	flagSet.Bool(flagPrefix+"force-text-format", false, "Send/receive data in text format")
}
