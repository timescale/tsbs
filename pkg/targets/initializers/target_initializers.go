package initializers

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/timescaledb"
	"strings"
)

func GetTarget(format string) targets.ImplementedTarget {
	switch format {
	case targets.FormatTimescaleDB:
		return timescaledb.NewTarget()
	}

	supportedFormatsStr := strings.Join(targets.SupportedFormats(), ",")
	panic(fmt.Sprintf("Unrecognized format %s, supported: %s", format, supportedFormatsStr))
}
