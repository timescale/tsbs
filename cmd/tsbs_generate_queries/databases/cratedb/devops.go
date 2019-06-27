package cratedb

import (
	"fmt"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/query"
)

// TODO: Remove the need for this by continuing to bubble up errors
func panicIfErr(err error) {
	if err != nil {
		panic(err.Error())
	}
}

// Devops produces CrateDB-specific queries for all the devops query types.
type Devops struct {
	*BaseGenerator
	*devops.Core
}

const hostnameField = "tags['hostname']"

// getSelectAggClauses builds specified aggregate function clauses for
// a set of column idents.
//
// For instance:
//      max(cpu_time) AS max_cpu_time
func (d *Devops) getSelectAggClauses(aggFunc string, idents []string) []string {
	selectAggClauses := make([]string, len(idents))
	for i, ident := range idents {
		selectAggClauses[i] =
			fmt.Sprintf("%[1]s(%[2]s) AS %[1]s_%[2]s", aggFunc, ident)
	}
	return selectAggClauses
}

// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for N random
// hosts
//
// Queries:
// cpu-max-all-1
// cpu-max-all-8
func (d *Devops) MaxAllCPU(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.MaxAllDuration)
	selectClauses := d.getSelectAggClauses("max", devops.GetAllCPUMetrics())
	hosts, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)

	sql := fmt.Sprintf(`
		SELECT
			date_trunc('hour', ts) AS hour,
			%s
		FROM cpu
		WHERE %s IN ('%s')
		  AND ts >= %d
		  AND ts < %d
		GROUP BY hour
		ORDER BY hour`,
		strings.Join(selectClauses, ", "),
		hostnameField,
		strings.Join(hosts, "', '"),
		interval.StartUnixMillis(),
		interval.EndUnixMillis())

	humanLabel := devops.GetMaxAllLabel("CrateDB", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// GroupByTimeAndPrimaryTag selects the AVG of metrics in the group `cpu` per device
// per hour for a day
//
// Queries:
// double-groupby-1
// double-groupby-5
// double-groupby-all
func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)
	selectClauses := d.getSelectAggClauses("mean", metrics)

	sql := fmt.Sprintf(`
		SELECT
			date_trunc('hour', ts) AS hour,
			%s
		FROM cpu
		WHERE ts >= %d
		  AND ts < %d
		GROUP BY hour, %s
		ORDER BY hour`,
		strings.Join(selectClauses, ", "),
		interval.StartUnixMillis(),
		interval.EndUnixMillis(),
		hostnameField)

	humanLabel := devops.GetDoubleGroupByLabel("CrateDB", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// GroupByOrderByLimit populates a query.Query that has a time WHERE clause,
// that groups by a truncated date, orders by that date, and takes a limit:
//
// Queries:
// groupby-orderby-limit
func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour)
	sql := fmt.Sprintf(`
		SELECT
			date_trunc('minute', ts) as minute,
			max(usage_user)
		FROM cpu
		WHERE ts < %d
		GROUP BY minute
		ORDER BY minute DESC
		LIMIT 5`,
		interval.EndUnixMillis())

	humanLabel := "CrateDB max cpu over last 5 min-intervals (random end)"
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *Devops) LastPointPerHost(qi query.Query) {
	sql := fmt.Sprintf(`
		SELECT *
		FROM
		  (
			SELECT %[1]s AS host, max(ts) AS max_ts
			FROM cpu
			GROUP BY %[1]s
		  ) t, cpu c
		WHERE t.max_ts = c.ts
		  AND t.host = c.%[1]s`, hostnameField)

	humanLabel := "CrateDB last row per host"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// HighCPUForHosts populates a query that gets CPU metrics when the CPU has
// high usage between a time period for a number of hosts (if 0, it will
// search all hosts)
//
// Queries:
// high-cpu-1
// high-cpu-all
func (d *Devops) HighCPUForHosts(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.HighCPUDuration)
	hosts, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)

	sql := fmt.Sprintf(`
		SELECT *
		FROM cpu
		WHERE usage_user > 90.0
		  AND ts >= %d
		  AND ts < %d
		  AND %s IN ('%s')`,
		interval.StartUnixMillis(),
		interval.EndUnixMillis(),
		hostnameField,
		strings.Join(hosts, "', '"))

	humanLabel, err := devops.GetHighCPULabel("CrateDB", nHosts)
	panicIfErr(err)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// GroupByTime selects the MAX for metrics under 'cpu', per minute for N random
// hosts
//
// Resultsets:
// single-groupby-1-1-12
// single-groupby-1-1-1
// single-groupby-1-8-1
// single-groupby-5-1-12
// single-groupby-5-1-1
// single-groupby-5-8-1
func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	selectClauses := d.getSelectAggClauses("max", metrics)
	hosts, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)

	sql := fmt.Sprintf(`
		SELECT
			date_trunc('minute', ts) as minute,
			%s
		FROM cpu
		WHERE %s IN ('%s')
		  AND ts >= %d
		  AND ts < %d
		GROUP BY minute
		ORDER BY minute ASC`,
		strings.Join(selectClauses, ", "),
		hostnameField,
		strings.Join(hosts, "', '"),
		interval.StartUnixMillis(),
		interval.EndUnixMillis())

	humanLabel := fmt.Sprintf(
		"CrateDB %d cpu metric(s), random %4d hosts, random %s by 1m",
		numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}
