package zigtsdb

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
	return &zigtsdbTarget{}
}

type zigtsdbTarget struct{}

func (t *zigtsdbTarget) Benchmark(_ string, dataSourceConfig *source.DataSourceConfig, v *viper.Viper) (targets.Benchmark, error) {
	conf, err := parseSpecificConfig(v)
	if err != nil {
		return nil, err
	}
	return NewBenchmark(conf, dataSourceConfig)
}

// Serializer reuses the InfluxDB line-protocol serializer: the generated
// data file is byte-identical to `--format influx`, and the loader expands
// each multi-field line into one zigtsdb line per field at load time.
func (t *zigtsdbTarget) Serializer() serialize.PointSerializer {
	return &influx.Serializer{}
}

func (t *zigtsdbTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"urls", "http://localhost:8080",
		"Comma-separated list of zigtsdb node URLs, used round-robin by workers")
	flagSet.String(flagPrefix+"ingest-path", defaultIngestPath,
		"Ingest endpoint path appended to each URL (e.g. /ingest for the auth-gated replicated path)")
	flagSet.Int(flagPrefix+"max-body-bytes", defaultMaxBodyBytes,
		"Maximum bytes per ingest POST; batches are split on line boundaries to stay under the server's 2 MiB cap")
	flagSet.Int(flagPrefix+"max-retries", defaultMaxRetries,
		"Maximum retries per chunk on 429 (quota/backlog/memory pressure) or 503 (HA transition)")
}

func (t *zigtsdbTarget) TargetName() string {
	return constants.FormatZigTSDB
}
