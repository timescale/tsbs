package mongo

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
	return &mongoTarget{}
}

type mongoTarget struct {
}

func (t *mongoTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"url", "mongodb://localhost:27017/", "Mongo URL.")
	flagSet.Duration(flagPrefix+"write-timeout", 10*time.Second, "Write timeout.")
	flagSet.Bool(flagPrefix+"document-per-event", false, "Whether to use one document per event or aggregate by hour")
	flagSet.Bool(flagPrefix+"timeseries-collection", false, "Whether to use a time-series collection")
	flagSet.Bool(flagPrefix+"retryable-writes", true, "Whether to use retryable writes")
	flagSet.Bool(flagPrefix+"ordered-inserts", true, "Whether to use ordered inserts")
	flagSet.Bool(flagPrefix+"random-field-order", true, "Whether to use random field order")
	flagSet.Bool(flagPrefix+"collection-sharded", false, "Whether to shard the collection")
	flagSet.Uint(flagPrefix+"number-initial-chunks", 0, "number of initial chunks to create and distribute for an empty collection;" +
														"if 0 then do not specifiy any initial chunks and let the system default to 2 per shard")
	flagSet.String(flagPrefix+"shard-key-spec", "{time:1}", "shard key spec")
	flagSet.Bool(flagPrefix+"balancer-on", true, "whether to keep shard re-balancer on")
}

func (t *mongoTarget) TargetName() string {
	return constants.FormatMongo
}

func (t *mongoTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *mongoTarget) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	panic("not implemented")
}
