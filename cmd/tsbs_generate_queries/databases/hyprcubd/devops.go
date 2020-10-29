package hyprcubd

import (
	"fmt"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/query"
)

// Devops produces Hyprcubd-specific queries for all the devops query types.
type Devops struct {
	*BaseGenerator
	*devops.Core
}

// getHostsExpression selects nHosts random hosts and builds an or group
// e.g. (hostname = 'host_0' or hostname = 'host_1')
func (d *Devops) getHostsExpression(nHosts int) string {
	var s strings.Builder
	s.WriteString("(")
	hosts, err := d.GetRandomHosts(nHosts)
	if err != nil {
		panic(err)
	}
	for i, h := range hosts {
		if i > 0 {
			s.WriteString(" or ")
		}
		s.WriteString(fmt.Sprintf("hostname = '%s'", h))
	}
	s.WriteString(") ")
	return s.String()
}

// GroupByTime satisfies the following query types:
// single-groupby-1-1-1	 Simple aggregrate (MAX) on one metric for 1 host, every 5 mins for 1 hour
// single-groupby-1-1-12 Simple aggregrate (MAX) on one metric for 1 host, every 5 mins for 12 hours
// single-groupby-1-8-1	 Simple aggregrate (MAX) on one metric for 8 hosts, every 5 mins for 1 hour
// single-groupby-5-1-1	 Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 1 hour
// single-groupby-5-1-12 Simple aggregrate (MAX) on 5 metrics for 1 host, every 5 mins for 12 hours
// single-groupby-5-8-1	 Simple aggregrate (MAX) on 5 metrics for 8 hosts, every 5 mins for 1 hour
func (d *Devops) GroupByTime(qq query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	hostExpr := d.getHostsExpression(nHosts)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	if err != nil {
		panic(err.Error())
	}

	var sql strings.Builder
	sql.WriteString("select time")

	for _, m := range metrics {
		sql.WriteString(", ")
		sql.WriteString(m)
	}

	sql.WriteString(fmt.Sprintf(" from %s where ", devops.TableName))
	sql.WriteString(hostExpr)

	sql.WriteString(fmt.Sprintf("and time > '%s' and time < '%s' ", interval.Start().Format(time.RFC3339Nano), interval.End().Format(time.RFC3339Nano)))

	sql.WriteString("timeseries 5m")

	humanLabel := fmt.Sprintf("Hyprcubd %d cpu metric(s), random %4d hosts, random %s by 5m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qq, humanLabel, humanDesc, sql.String())
}
