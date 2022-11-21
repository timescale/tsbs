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

// modifyHostnames makes sure IP address can appear in the path.
// Node names in path can NOT contain "." unless enclosing it within either single quote (') or double quote (").
// In this case, quotes are recognized as part of the node name to avoid ambiguity.
func (d *Devops) modifyHostnames(hostnames []string) []string {
	for i, hostname := range hostnames {
		if strings.Contains(hostname, ".") {
			if !(hostname[:1] == "`" && hostname[len(hostname)-1:] == "`") {
				// not modified yet
				hostnames[i] = "`" + hostnames[i] + "`"
			}

		}
	}
	return hostnames
}

// getHostFromWithHostnames creates FROM SQL statement for multiple hostnames.
// e.g.  A storage group "root.cpu" has two devices.
//       Two hostnames are "host1" and "host2"
//       This function returns "root.cpu.host1, root.cpu.host2" (without "FROM")
func (d *Devops) getHostFromWithHostnames(hostnames []string) string {
	hostnames = d.modifyHostnames(hostnames)
	var hostnameClauses []string
	for _, hostname := range hostnames {
		hostnameClauses = append(hostnameClauses, fmt.Sprintf("%s.cpu.%s", d.BasicPath, hostname))
	}
	return strings.Join(hostnameClauses, ", ")
}

// getHostFromString gets multiple random hostnames and creates a FROM SQL statement for these hostnames.
// e.g.  A storage group "root.cpu" has two devices, named "host1" and "host2"
//       Two paths for them are "root.cpu.host1" and "root.cpu.host2"
//       This function returns "root.cpu.host1, root.cpu.host2" (without "FROM")
func (d *Devops) getHostFromString(nHosts int) string {
	hostnames, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)
	fromClauses := d.getHostFromWithHostnames(hostnames)
	return fromClauses
}

// getSelectClausesAggMetrics gets clauses for aggregate functions.
func (d *Devops) getSelectClausesAggMetrics(agg string, metrics []string) []string {
	selectClauses := make([]string, len(metrics))
	for i, m := range metrics {
		selectClauses[i] = fmt.Sprintf("%s(%s)", agg, m)
	}

	return selectClauses
}

// getSelectClausesAggMetricsString gets a whole select clause for aggregate functions.
func (d *Devops) getSelectClausesAggMetricsString(agg string, metrics []string) string {
	selectClauses := d.getSelectClausesAggMetrics(agg, metrics)
	return strings.Join(selectClauses, ", ")
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
	selectClause := d.getSelectClausesAggMetricsString("MAX_VALUE", metrics)
	fromHosts := d.getHostFromString(nHosts)

	humanLabel := fmt.Sprintf("IoTDB %d cpu metric(s), random %4d hosts, random %s by 5m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	sql := ""
	sql = sql + fmt.Sprintf("SELECT %s", selectClause)
	sql = sql + fmt.Sprintf(" FROM %s", fromHosts)
	sql = sql + fmt.Sprintf(" GROUP BY ([%s, %s), 5m)", interval.Start().Format(iotdbTimeFmt), interval.End().Format(iotdbTimeFmt))

	d.fillInQuery(qi, humanLabel, humanDesc, sql)
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
	selectClause := d.getSelectClausesAggMetricsString("AVG", metrics)

	humanLabel := devops.GetDoubleGroupByLabel("IoTDB", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	sql := ""
	sql = sql + fmt.Sprintf("SELECT %s", selectClause)
	sql = sql + fmt.Sprintf(" FROM %s.cpu.*", d.BasicPath)
	sql = sql + fmt.Sprintf(" GROUP BY ([%s, %s), 1h)", interval.Start().Format(iotdbTimeFmt), interval.End().Format(iotdbTimeFmt))

	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *Devops) LastPointPerHost(qi query.Query) {
	humanLabel := "IoTDB last row per host"
	humanDesc := humanLabel + ": cpu"

	sql := fmt.Sprintf("SELECT LAST * FROM %s.cpu.*", d.BasicPath)
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for nhosts hosts,
// e.g. in pseudo-SQL:
//
// SELECT MAX(metric1), ..., MAX(metricN)
// FROM cpu WHERE (hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour ORDER BY hour
func (d *Devops) MaxAllCPU(qi query.Query, nHosts int, duration time.Duration) {
	interval := d.Interval.MustRandWindow(duration)
	fromHosts := d.getHostFromString(nHosts)
	selectClause := d.getSelectClausesAggMetricsString("MAX_VALUE", devops.GetAllCPUMetrics())

	humanLabel := devops.GetMaxAllLabel("IoTDB", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	sql := ""
	sql = sql + fmt.Sprintf("SELECT %s", selectClause)
	sql = sql + fmt.Sprintf(" FROM %s", fromHosts)
	sql = sql + fmt.Sprintf(" GROUP BY ([%s, %s), 1h)", interval.Start().Format(iotdbTimeFmt), interval.End().Format(iotdbTimeFmt))

	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// GroupByOrderByLimit benchmarks a query that has a time WHERE clause, that groups by a truncated date, orders by that date, and takes a limit:
// SELECT date_trunc('minute', time) AS t, MAX(cpu) FROM cpu
// WHERE time < '$TIME'
// GROUP BY t ORDER BY t DESC
// LIMIT $LIMIT
func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour)
	selectClause := d.getSelectClausesAggMetricsString("MAX_VALUE", []string{"usage_user"})

	sql := ""
	sql = sql + fmt.Sprintf("SELECT %s", selectClause)
	sql = sql + fmt.Sprintf(" FROM %s.cpu.*", d.BasicPath)
	sql = sql + fmt.Sprintf(" GROUP BY ([%s, %s), 1m), LEVEL = %d", interval.Start().Format(iotdbTimeFmt), interval.End().Format(iotdbTimeFmt), d.BasicPathLevel+1)
	sql = sql + " ORDER BY TIME DESC LIMIT 5"

	humanLabel := "IoTDB max cpu over last 5 min-intervals (random end)"
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())

	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// HighCPUForHosts populates a query that gets CPU metrics when the CPU has high
// usage between a time period for a number of hosts (if 0, it will search all hosts),
// e.g. in pseudo-SQL:
//
// SELECT * FROM cpu
// WHERE usage_user > 90.0
// AND time >= '$TIME_START' AND time < '$TIME_END'
// AND (hostname = '$HOST' OR hostname = '$HOST2'...)
func (d *Devops) HighCPUForHosts(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.HighCPUDuration)

	var fromHosts string
	if nHosts <= 0 {
		fromHosts = fmt.Sprintf("%s.cpu.*", d.BasicPath)
	} else {
		fromHosts = d.getHostFromString(nHosts)
	}

	humanLabel, err := devops.GetHighCPULabel("IoTDB", nHosts)
	panicIfErr(err)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())

	sql := "SELECT *"
	sql = sql + fmt.Sprintf(" FROM %s", fromHosts)
	sql = sql + fmt.Sprintf(" WHERE usage_user > 90 AND time >= %s AND time < %s", interval.Start().Format(iotdbTimeFmt), interval.End().Format(iotdbTimeFmt))

	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}
