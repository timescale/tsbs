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
// single-groupby-1-1-1
// single-groupby-1-1-12
// single-groupby-1-8-1
// single-groupby-5-1-1
// single-groupby-5-1-12
// single-groupby-5-8-1
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
		sql.WriteString(fmt.Sprintf("max(%s)", m))
	}

	sql.WriteString(fmt.Sprintf(" from %s where ", devops.TableName))
	sql.WriteString(hostExpr)

	sql.WriteString(fmt.Sprintf("and time > '%s' and time < '%s' ", interval.Start().Format(time.RFC3339Nano), interval.End().Format(time.RFC3339Nano)))

	sql.WriteString("timeseries 1m order by time")

	humanLabel := fmt.Sprintf("Hyprcubd %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qq, humanLabel, humanDesc, sql.String())
}

// MaxAllCPU satisfies the following query types:
// cpu-max-all-1
// cpu-max-all-8
func (d *Devops) MaxAllCPU(qq query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.MaxAllDuration)
	metrics := devops.GetAllCPUMetrics()
	hostExpr := d.getHostsExpression(nHosts)

	var sql strings.Builder
	sql.WriteString("select time")

	for _, m := range metrics {
		sql.WriteString(", ")
		sql.WriteString(fmt.Sprintf("max(%s)", m))
	}

	sql.WriteString(fmt.Sprintf(" from %s where ", devops.TableName))
	sql.WriteString(hostExpr)

	sql.WriteString(fmt.Sprintf("and time > '%s' and time < '%s' ", interval.Start().Format(time.RFC3339Nano), interval.End().Format(time.RFC3339Nano)))

	sql.WriteString("timeseries 1h order by time")

	humanLabel := devops.GetMaxAllLabel("Hyprcubd", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qq, humanLabel, humanDesc, sql.String())
}

// GroupByTimeAndPrimaryTag satisfies the following query types:
// double-groupby-1
// double-groupby-5
// double-groupby-all
func (d *Devops) GroupByTimeAndPrimaryTag(qq query.Query, numMetrics int) {
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	if err != nil {
		panic(err.Error())
	}
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)

	var sql strings.Builder
	sql.WriteString("select time, hostname")

	for _, m := range metrics {
		sql.WriteString(", ")
		sql.WriteString(fmt.Sprintf("avg(%s)", m))
	}

	sql.WriteString(fmt.Sprintf(" from %s ", devops.TableName))
	sql.WriteString(fmt.Sprintf("where time > '%s' and time < '%s' ", interval.Start().Format(time.RFC3339Nano), interval.End().Format(time.RFC3339Nano)))

	sql.WriteString("group by hostname timeseries 1h order by time")

	humanLabel := devops.GetDoubleGroupByLabel("Hyprcubd", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qq, humanLabel, humanDesc, sql.String())
}
