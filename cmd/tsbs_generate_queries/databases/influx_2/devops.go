package influx_2

import (
	"fmt"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/databases"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/pkg/query"
)

// Devops produces Influx-specific queries for all the devops query types.
type Devops struct {
	*BaseGenerator
	*devops.Core
}

func (d *Devops) getHostFilterClauseWithHostnames(hostnames []string) string {
	hostnameClauses := []string{}
	for _, s := range hostnames {
		hostnameClauses = append(hostnameClauses, fmt.Sprintf("r.hostname == '%s'", s))
	}

	combinedHostnameClause := strings.Join(hostnameClauses, " or ")
	return "(" + combinedHostnameClause + ")"
}

func (d *Devops) getHostFilterClause(nHosts int) string {
	hostnames, err := d.GetRandomHosts(nHosts)
	databases.PanicIfErr(err)
	return d.getHostFilterClauseWithHostnames(hostnames)
}

func (d *Devops) getMetricsFilterClause(metrics []string) string {

	metricClauses := make([]string, len(metrics))
	for i, m := range metrics {
		metricClauses[i] = fmt.Sprintf("r._field == '%s'", m)
	}

	return "(" + strings.Join(metricClauses, " or ") + ")"
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
	databases.PanicIfErr(err)
	metricsFilter := d.getMetricsFilterClause(metrics)
	hostsFilter := d.getHostFilterClause(nHosts)

	humanLabel := fmt.Sprintf("Influx 2.x %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	fluxString := fmt.Sprintf(`
	from(bucket: "benchmark") 
	|> range(start: %s, stop: %s)
	|> filter(fn: (r) => r._measurement == "cpu" and %s and %s)
	|> aggregateWindow(every: 1m, fn: max, createEmpty: false)
	|> yield()
	`, interval.StartString(), interval.EndString(), metricsFilter, hostsFilter)
	d.fillInQuery(qi, humanLabel, humanDesc, fluxString)
}

// GroupByOrderByLimit benchmarks a query that has a time WHERE clause, that groups by a truncated date, orders by that date, and takes a limit:
// SELECT date_trunc('minute', time) AS t, MAX(cpu) FROM cpu
// WHERE time < '$TIME'
// GROUP BY t ORDER BY t DESC
// LIMIT $LIMIT
func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour)

	humanLabel := "Influx 2.x max cpu over last 5 min-intervals (random end)"
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	fluxString := fmt.Sprintf(`
	from(bucket: "benchmark")
	|> range(start: 0, stop: %s)
	|> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_user")
	|> aggregateWindow(every: 1m, fn: max)
	|> sort(columns: ["_time"], desc: true)
	|> limit(n: 5)
	`, interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, fluxString)
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day,
// e.g. in pseudo-SQL:
//
// SELECT AVG(metric1), ..., AVG(metricN)
// FROM cpu
// WHERE time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour, hostname ORDER BY hour, hostname
func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	databases.PanicIfErr(err)
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)
	metricsFilterClause := d.getMetricsFilterClause(metrics)

	humanLabel := devops.GetDoubleGroupByLabel("Influx 2.x", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	fluxString := fmt.Sprintf(`
	from(bucket: "benchmark")
	|> range(start: %s, stop: %s)
	|> filter(fn: (r) => r._measurement == "cpu" and %s)
	|> group(columns: ["_time", "hostname"])
	|> aggregateWindow(every: 1h, fn: mean)
	|> yield(name: "mean")
	`, interval.StartString(), interval.EndString(), metricsFilterClause)
	d.fillInQuery(qi, humanLabel, humanDesc, fluxString)
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
	hostsFilterClause := d.getHostFilterClause(nHosts)
	metricsFilterClause := d.getMetricsFilterClause(devops.GetAllCPUMetrics())

	humanLabel := devops.GetMaxAllLabel("Influx 2.x", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	fluxString := fmt.Sprintf(`
	from(bucket: "benchmark")
	|> range(start: %s, stop: %s)
	|> filter(fn: (r) => r._measurement == "cpu" and %s and %s)
	|> aggregateWindow(every: 1h, fn: max)
	|> yield(name: "max")
	`, interval.StartString(), interval.EndString(), metricsFilterClause, hostsFilterClause)
	d.fillInQuery(qi, humanLabel, humanDesc, fluxString)
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *Devops) LastPointPerHost(qi query.Query) {
	humanLabel := "Influx 2.x last row per host"
	humanDesc := humanLabel + ": cpu"
	fluxString := `
	from(bucket: "benchmark")
	|> range(start: 0)
	|> filter(fn: (r) => r._measurement == "cpu")
	|> group(columns: ["hostname"])
	|> last()
	`
	d.fillInQuery(qi, humanLabel, humanDesc, fluxString)
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

	var hostsFilterClause string
	if nHosts == 0 {
		hostsFilterClause = ""
	} else {
		hostsFilterClause = " and " + d.getHostFilterClause(nHosts)
	}

	humanLabel, err := devops.GetHighCPULabel("Influx 2.x", nHosts)
	databases.PanicIfErr(err)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	fluxString := fmt.Sprintf(`
	from(bucket: "benchmark")
	|> range(start: %s, stop: %s)
	|> filter(fn: (r) => r._measurement == "cpu" and r._field == "usage_user" and r._value > 90.0%s)
	|> yield(name: "high_cpu")
	`, interval.StartString(), interval.EndString(), hostsFilterClause)
	d.fillInQuery(qi, humanLabel, humanDesc, fluxString)
}
