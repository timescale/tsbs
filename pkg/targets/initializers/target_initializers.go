package initializers

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/akumuli"
	"github.com/timescale/tsbs/pkg/targets/cassandra"
	"github.com/timescale/tsbs/pkg/targets/clickhouse"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"github.com/timescale/tsbs/pkg/targets/crate"
	"github.com/timescale/tsbs/pkg/targets/influx"
	"github.com/timescale/tsbs/pkg/targets/mongo"
	"github.com/timescale/tsbs/pkg/targets/prometheus"
	"github.com/timescale/tsbs/pkg/targets/questdb"
	"github.com/timescale/tsbs/pkg/targets/siridb"
	"github.com/timescale/tsbs/pkg/targets/timescaledb"
	"github.com/timescale/tsbs/pkg/targets/timestream"
	"github.com/timescale/tsbs/pkg/targets/victoriametrics"
	"strings"
)

func GetTarget(format string) targets.ImplementedTarget {
	switch format {
	case constants.FormatTimescaleDB:
		return timescaledb.NewTarget()
	case constants.FormatAkumuli:
		return akumuli.NewTarget()
	case constants.FormatCassandra:
		return cassandra.NewTarget()
	case constants.FormatClickhouse:
		return clickhouse.NewTarget()
	case constants.FormatCrateDB:
		return crate.NewTarget()
	case constants.FormatInflux:
		return influx.NewTarget()
	case constants.FormatMongo:
		return mongo.NewTarget()
	case constants.FormatPrometheus:
		return prometheus.NewTarget()
	case constants.FormatSiriDB:
		return siridb.NewTarget()
	case constants.FormatVictoriaMetrics:
		return victoriametrics.NewTarget()
	case constants.FormatTimestream:
		return timestream.NewTarget()
	case constants.FormatQuestDB:
		return questdb.NewTarget()
	}

	supportedFormatsStr := strings.Join(constants.SupportedFormats(), ",")
	panic(fmt.Sprintf("Unrecognized format %s, supported: %s", format, supportedFormatsStr))
}
