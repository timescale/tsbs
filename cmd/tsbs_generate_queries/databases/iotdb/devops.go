package iotdb

import (
	"fmt"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/pkg/query"
)

// TODO: Remove the need for this by continuing to bubble up errors
func panicIfErr(err error) {
	if err != nil {
		panic(err.Error())
	}
}

// Devops produces IoTDB-specific queries for all the devops query types.
type Devops struct {
	*BaseGenerator
	*devops.Core
}

// getHostFromString gets multiple random hostnames and creates a FROM SQL statement for these hostnames.
// e.g.  A storage group "root.cpu" has two devices, named "host1" and "host2"
//       Two paths for them are "root.cpu.host1" and "root.cpu.host2"
//       This function returns "root.cpu.host1, root.cpu.host2" (without "FROM")
func (d *Devops) getHostFromString(nHosts int) string {
	hostnames, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)
	var hostnameClauses []string

	for _, hostname := range hostnames {
		hostnameClauses = append(hostnameClauses, fmt.Sprintf("%s.cpu.%s", d.BasicPath, hostname))
	}

	return strings.Join(hostnameClauses, ", ")
}

// getSelectClausesAggMetrics gets clauses for aggregate functions.
func (d *Devops) getSelectClausesAggMetrics(agg string, metrics []string) []string {
	selectClauses := make([]string, len(metrics))
	for i, m := range metrics {
		selectClauses[i] = fmt.Sprintf("%s(%s)", agg, m)
	}

	return selectClauses
}

// GroupByTime selects the MAX for numMetrics metrics under 'cpu',
// per minute for nhosts hosts,
// e.g. in pseudo-SQL:
//
// SELECT minute, max(metric1), ..., max(metricN)
// FROM cpu
// WHERE (hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY minute ORDER BY minute ASC
func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	selectClauses := d.getSelectClausesAggMetrics("MAX_VALUE", metrics)
	fromHosts := d.getHostFromString(nHosts)

	humanLabel := fmt.Sprintf("IoTDB %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	sql := ""
	sql = sql + fmt.Sprintf("SELECT %s", strings.Join(selectClauses, ", "))
	sql = sql + fmt.Sprintf(" FROM %s", fromHosts)
	// sql = sql + fmt.Sprintf(" WHERE time >= %s AND time < %s", interval.StartString(), interval.EndString())
	sql = sql + fmt.Sprintf(" GROUP BY ([%s, %s), 1m)", interval.StartString(), interval.EndString())

	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day,
// e.g. in pseudo-SQL:
//
// SELECT AVG(metric1), ..., AVG(metricN)
// FROM cpu
// WHERE time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour, hostname ORDER BY hour
func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)
	selectClauses := d.getSelectClausesAggMetrics("AVG", metrics)

	humanLabel := devops.GetDoubleGroupByLabel("IoTDB", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	sql := ""
	sql = sql + fmt.Sprintf("SELECT %s", strings.Join(selectClauses, ", "))
	sql = sql + fmt.Sprintf(" FROM %s.cpu.*", d.BasicPath)
	// sql = sql + fmt.Sprintf(" WHERE time >= %s AND time < %s", interval.StartString(), interval.EndString())
	sql = sql + fmt.Sprintf(" GROUP BY ([%s, %s), 1m), LEVEL = %d", interval.StartString(), interval.EndString(), d.BasicPathLevel+2)

	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}
