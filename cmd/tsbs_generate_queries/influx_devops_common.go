package main

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// InfluxDevops produces Influx-specific queries for all the devops query types.
type InfluxDevops struct {
	*devopsCore
}

// NewInfluxDevops makes an InfluxDevops object ready to generate Queries.
func newInfluxDevopsCommon(start, end time.Time, scale int) *InfluxDevops {
	return &InfluxDevops{newDevopsCore(start, end, scale)}
}

// GenerateEmptyQuery returns an empty query.HTTP
func (d *InfluxDevops) GenerateEmptyQuery() query.Query {
	return query.NewHTTP()
}

func (d *InfluxDevops) getHostWhereWithHostnames(hostnames []string) string {
	hostnameClauses := []string{}
	for _, s := range hostnames {
		hostnameClauses = append(hostnameClauses, fmt.Sprintf("hostname = '%s'", s))
	}

	combinedHostnameClause := strings.Join(hostnameClauses, " or ")
	return "(" + combinedHostnameClause + ")"
}

func (d *InfluxDevops) getHostWhereString(nHosts int) string {
	hostnames := d.getRandomHosts(nHosts)
	return d.getHostWhereWithHostnames(hostnames)
}

func (d *InfluxDevops) getSelectClausesAggMetrics(agg string, metrics []string) []string {
	selectClauses := make([]string, len(metrics))
	for i, m := range metrics {
		selectClauses[i] = fmt.Sprintf("%s(%s)", agg, m)
	}

	return selectClauses
}

// GroupByTime selects the MAX for numMetrics metrics under 'cpu',
// per minute for nhosts hosts,
// e.g. in psuedo-SQL:
//
// SELECT minute, max(metric1), ..., max(metricN)
// FROM cpu
// WHERE (hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY minute ORDER BY minute ASC
func (d *InfluxDevops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.interval.RandWindow(timeRange)
	metrics := getCPUMetricsSlice(numMetrics)
	selectClauses := d.getSelectClausesAggMetrics("max", metrics)
	whereHosts := d.getHostWhereString(nHosts)

	v := url.Values{}
	v.Set("q", fmt.Sprintf("SELECT %s from cpu where %s and time >= '%s' and time < '%s' group by time(1m)", strings.Join(selectClauses, ", "), whereHosts, interval.StartString(), interval.EndString()))

	humanLabel := fmt.Sprintf("Influx %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	q := qi.(*query.HTTP)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

// GroupByOrderByLimit benchmarks a query that has a time WHERE clause, that groups by a truncated date, orders by that date, and takes a limit:
// SELECT date_trunc('minute', time) AS t, MAX(cpu) FROM cpu
// WHERE time < '$TIME'
// GROUP BY t ORDER BY t DESC
// LIMIT $LIMIT
func (d *InfluxDevops) GroupByOrderByLimit(qi query.Query) {
	interval := d.interval.RandWindow(time.Hour)

	where := fmt.Sprintf("WHERE time < '%s'", interval.EndString())

	v := url.Values{}
	v.Set("q", fmt.Sprintf(`SELECT max(usage_user) from cpu %s group by time(1m) limit 5`, where))

	humanLabel := "Influx max cpu over last 5 min-intervals (rand end)"
	q := qi.(*query.HTTP)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day,
// e.g. in psuedo-SQL:
//
// SELECT AVG(metric1), ..., AVG(metricN)
// FROM cpu
// WHERE time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour, hostname ORDER BY hour, hostname
func (d *InfluxDevops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	metrics := getCPUMetricsSlice(numMetrics)
	interval := d.interval.RandWindow(doubleGroupByDuration)

	selectClauses := make([]string, len(metrics))
	for i, m := range metrics {
		selectClauses[i] = fmt.Sprintf("mean(%s)", m)
	}

	v := url.Values{}
	v.Set("q", fmt.Sprintf("SELECT %s from cpu where time >= '%s' and time < '%s' group by time(1h),hostname", strings.Join(selectClauses, ", "), interval.StartString(), interval.EndString()))

	humanLabel := fmt.Sprintf("Influx mean of %d metrics, all hosts, rand 1day by 1hr", numMetrics)
	q := qi.(*query.HTTP)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for nhosts hosts,
// e.g. in psuedo-SQL:
//
// SELECT MAX(metric1), ..., MAX(metricN)
// FROM cpu WHERE (hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour ORDER BY hour
func (d *InfluxDevops) MaxAllCPU(qi query.Query, nhosts int) {
	interval := d.interval.RandWindow(8 * time.Hour)
	whereHosts := d.getHostWhereString(nhosts)

	v := url.Values{}
	v.Set("q", fmt.Sprintf("SELECT max(usage_user),max(usage_system),max(usage_idle),max(usage_nice),max(usage_iowait),max(usage_irq),max(usage_softirq),max(usage_steal),max(usage_guest),max(usage_guest_nice) from cpu where %s and time >= '%s' and time < '%s' group by time(1m)", whereHosts, interval.StartString(), interval.EndString()))

	humanLabel := fmt.Sprintf("Influx max cpu all fields, rand %4d hosts, rand 12hr by 1m", nhosts)
	q := qi.(*query.HTTP)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *InfluxDevops) LastPointPerHost(qi query.Query) {
	v := url.Values{}
	v.Set("q", "SELECT * from cpu group by \"hostname\" order by time desc limit 1")

	humanLabel := "Influx last row per host"
	q := qi.(*query.HTTP)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: cpu", humanLabel))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

// HighCPUForHosts populates a query that gets CPU metrics when the CPU has high
// usage between a time period for a number of hosts (if 0, it will search all hosts),
// e.g. in psuedo-SQL:
//
// SELECT * FROM cpu
// WHERE usage_user > 90.0
// AND time >= '$TIME_START' AND time < '$TIME_END'
// AND (hostname = '$HOST' OR hostname = '$HOST2'...)
func (d *InfluxDevops) HighCPUForHosts(qi query.Query, nHosts int) {
	interval := d.interval.RandWindow(highCPUDuration)
	var hostWhereClause string
	if nHosts == 0 {
		hostWhereClause = ""
	} else {
		hostWhereClause = fmt.Sprintf("and %s", d.getHostWhereString(nHosts))
	}

	v := url.Values{}
	v.Set("q", fmt.Sprintf("SELECT * from cpu where usage_user > 90.0 %s and time >= '%s' and time < '%s'", hostWhereClause, interval.StartString(), interval.EndString()))

	humanLabel := getHighCPULabel("Influx", nHosts)
	q := qi.(*query.HTTP)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}
