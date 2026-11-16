package zigtsdb

import (
	"fmt"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/pkg/query"
)

// Devops produces ZQL queries for all the devops query types.
//
// zigtsdb stores one float64 value per series, so the TSBS `cpu` measurement
// is loaded as ten metrics `cpu_<field>` (see pkg/targets/zigtsdb). Multi-field
// queries resolve every field-metric in ONE statement via a `__metric__ =~`
// regex and fold per metric with `GROUP BY __metric__` — one round trip, one
// scan. (Double-groupby, which also groups by hostname, still emits N
// statements: multi-label GROUP BY is a follow-up.)
type Devops struct {
	*BaseGenerator
	*devops.Core
}

func panicIfErr(err error) {
	if err != nil {
		panic(err.Error())
	}
}

func metricName(field string) string {
	return "cpu_" + field
}

// hostWhere builds an exact-match OR chain: (hostname = 'host_1' OR ...).
func hostWhere(hosts []string) string {
	parts := make([]string, len(hosts))
	for i, h := range hosts {
		parts[i] = fmt.Sprintf("hostname = '%s'", h)
	}
	return "(" + strings.Join(parts, " OR ") + ")"
}

// metricRegex builds a `__metric__` predicate that resolves exactly the given
// CPU field-metrics in one shot: `__metric__ =~ 'cpu_(usage_user|usage_system|
// ...)'`. ZQL regexes are fully anchored, so no field name matches another as a
// prefix (e.g. usage_guest does not match cpu_usage_guest_nice).
func metricRegex(fields []string) string {
	return fmt.Sprintf("__metric__ =~ 'cpu_(%s)'", strings.Join(fields, "|"))
}

// timeWhere bounds the query to [start, end) in Unix nanoseconds, the unit
// ZQL uses for absolute `time` comparisons.
func timeWhere(startNs, endNs int64) string {
	return fmt.Sprintf("time >= %d AND time < %d", startNs, endNs)
}

// GroupByTime selects the MAX for numMetrics metrics under 'cpu' per minute
// for nHosts hosts. One metric uses the plain windowed path; N>1 metrics fold
// into a single statement:
//
//	SELECT max(value) FROM cpu_usage_user
//	WHERE __metric__ =~ 'cpu_(usage_user|...)' AND (hostname = 'host_1' OR ...)
//	      AND time >= S AND time < E
//	GROUP BY __metric__, time_bucket(time, 1 m)
//
// Resultsets: single-groupby-{1,5}-{1,8}-{1,12}
func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	hosts, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)

	var stmts []string
	if numMetrics == 1 {
		// One metric → the plain windowed aggregation path (no per-metric fold).
		stmts = []string{fmt.Sprintf(
			"SELECT max(value) FROM %s WHERE %s AND %s GROUP BY time_bucket(time, 1 m)",
			metricName(metrics[0]), hostWhere(hosts),
			timeWhere(interval.StartUnixNano(), interval.EndUnixNano()))}
	} else {
		// N metrics → ONE statement: resolve every field-metric via the
		// __metric__ regex and fold per metric with GROUP BY __metric__, instead
		// of N sequential statements (the former 20-100x multi-metric penalty).
		stmts = []string{fmt.Sprintf(
			"SELECT max(value) FROM %s WHERE %s AND %s AND %s GROUP BY __metric__, time_bucket(time, 1 m)",
			metricName(metrics[0]), metricRegex(metrics), hostWhere(hosts),
			timeWhere(interval.StartUnixNano(), interval.EndUnixNano()))}
	}
	humanLabel := fmt.Sprintf("ZigTSDB %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, interval, stmts)
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu'
// per host per hour for a day, one statement per metric:
//
//	SELECT avg(value) FROM cpu_usage_user
//	WHERE time >= S AND time < E
//	GROUP BY hostname, time_bucket(time, 1 h)
//
// Resultsets: double-groupby-{1,5,all}
func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)

	stmts := make([]string, len(metrics))
	for i, m := range metrics {
		stmts[i] = fmt.Sprintf("SELECT avg(value) FROM %s WHERE %s GROUP BY hostname, time_bucket(time, 1 h)",
			metricName(m), timeWhere(interval.StartUnixNano(), interval.EndUnixNano()))
	}
	humanLabel := devops.GetDoubleGroupByLabel("ZigTSDB", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, interval, stmts)
}

// MaxAllCPU selects the MAX of all ten metrics under 'cpu' per hour for nHosts
// hosts in ONE statement, folded per metric:
//
//	SELECT max(value) FROM cpu_usage_user
//	WHERE __metric__ =~ 'cpu_(usage_user|...)' AND (hostname = 'host_1' OR ...)
//	      AND time >= S AND time < E
//	GROUP BY __metric__, time_bucket(time, 1 h)
//
// Resultsets: cpu-max-all-{1,8}
func (d *Devops) MaxAllCPU(qi query.Query, nHosts int, duration time.Duration) {
	interval := d.Interval.MustRandWindow(duration)
	hosts, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)

	metrics := devops.GetAllCPUMetrics()
	// ONE statement folding all ten CPU field-metrics per metric — the long-form
	// of QuestDB's wide `max(usage_user), max(usage_system), ...` row. Replaces
	// the former ten sequential statements (the ~510 ms cpu-max-all rows).
	stmts := []string{fmt.Sprintf(
		"SELECT max(value) FROM %s WHERE %s AND %s AND %s GROUP BY __metric__, time_bucket(time, 1 h)",
		metricName(metrics[0]), metricRegex(metrics), hostWhere(hosts),
		timeWhere(interval.StartUnixNano(), interval.EndUnixNano()))}
	humanLabel := devops.GetMaxAllLabel("ZigTSDB", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, interval, stmts)
}

// GroupByOrderByLimit selects the MAX of usage_user per minute for the last
// five minute-buckets before a random end time:
//
//	SELECT max(value) FROM cpu_usage_user WHERE time < E
//	GROUP BY time_bucket(time, 1 m) ORDER BY time DESC LIMIT 5
//
// Resultsets: groupby-orderby-limit
func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour)
	stmt := fmt.Sprintf("SELECT max(value) FROM %s WHERE time < %d GROUP BY time_bucket(time, 1 m) ORDER BY time DESC LIMIT 5",
		metricName("usage_user"), interval.EndUnixNano())
	humanLabel := "ZigTSDB max cpu over last 5 min-intervals (random end)"
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, interval, []string{stmt})
}

// LastPointPerHost finds the latest usage_user value for every host:
//
//	SELECT last(value) FROM cpu_usage_user GROUP BY hostname
//
// Requires zigtsdb >= 1.2.0 (the `last()` aggregate).
//
// Resultsets: lastpoint
func (d *Devops) LastPointPerHost(qi query.Query) {
	stmt := fmt.Sprintf("SELECT last(value) FROM %s GROUP BY hostname", metricName("usage_user"))
	humanLabel := "ZigTSDB last row per host"
	d.fillInQuery(qi, humanLabel, humanLabel, nil, []string{stmt})
}

// HighCPUForHosts selects the usage_user points above 90 for nHosts hosts
// (all hosts when nHosts is 0) over a 12-hour window:
//
//	SELECT * FROM cpu_usage_user
//	WHERE value > 90.0 [AND (hostname = ...)] AND time >= S AND time < E
//	LIMIT 1000000
//
// Only the usage_user column is returned: ZQL has no cross-metric row
// assembly, so the other nine CPU fields of a qualifying row are not fetched.
//
// Resultsets: high-cpu-{1,all}
func (d *Devops) HighCPUForHosts(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.HighCPUDuration)
	where := "value > 90.0"
	if nHosts > 0 {
		hosts, err := d.GetRandomHosts(nHosts)
		panicIfErr(err)
		where += " AND " + hostWhere(hosts)
	}
	where += " AND " + timeWhere(interval.StartUnixNano(), interval.EndUnixNano())
	stmt := fmt.Sprintf("SELECT * FROM %s WHERE %s LIMIT 1000000", metricName("usage_user"), where)

	humanLabel, err := devops.GetHighCPULabel("ZigTSDB", nHosts)
	panicIfErr(err)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, interval, []string{stmt})
}
