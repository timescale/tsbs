package influx

import (
	"fmt"
	"net/url"
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

// Devops produces Influx-specific queries for all the devops query types.
type Devops struct {
	*devops.Core
}

// NewDevops makes an Devops object ready to generate Queries.
func NewDevops(start, end time.Time, scale int) *Devops {
	core, err := devops.NewCore(start, end, scale)
	panicIfErr(err)
	return &Devops{core}
}

// GenerateEmptyQuery returns an empty query.HTTP
func (d *Devops) GenerateEmptyQuery() query.Query {
	return query.NewHTTP()
}

func (d *Devops) getHostWhereWithHostnames(hostnames []string) string {
	hostnameClauses := []string{}
	for _, s := range hostnames {
		hostnameClauses = append(hostnameClauses, fmt.Sprintf("hostname = '%s'", s))
	}

	combinedHostnameClause := strings.Join(hostnameClauses, " or ")
	return "(" + combinedHostnameClause + ")"
}

func (d *Devops) getHostWhereString(nHosts int) string {
	hostnames, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)
	return d.getHostWhereWithHostnames(hostnames)
}

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
	selectClauses := d.getSelectClausesAggMetrics("max", metrics)
	whereHosts := d.getHostWhereString(nHosts)

	humanLabel := fmt.Sprintf("Influx %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	influxql := fmt.Sprintf("SELECT %s from cpu where %s and time >= '%s' and time < '%s' group by time(1m)", strings.Join(selectClauses, ", "), whereHosts, interval.StartString(), interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

// GroupByOrderByLimit benchmarks a query that has a time WHERE clause, that groups by a truncated date, orders by that date, and takes a limit:
// SELECT date_trunc('minute', time) AS t, MAX(cpu) FROM cpu
// WHERE time < '$TIME'
// GROUP BY t ORDER BY t DESC
// LIMIT $LIMIT
func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour)
	where := fmt.Sprintf("WHERE time < '%s'", interval.EndString())

	humanLabel := "Influx max cpu over last 5 min-intervals (random end)"
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	influxql := fmt.Sprintf(`SELECT max(usage_user) from cpu %s group by time(1m) limit 5`, where)
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
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
	panicIfErr(err)
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)
	selectClauses := d.getSelectClausesAggMetrics("mean", metrics)

	humanLabel := devops.GetDoubleGroupByLabel("Influx", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	influxql := fmt.Sprintf("SELECT %s from cpu where time >= '%s' and time < '%s' group by time(1h),hostname", strings.Join(selectClauses, ", "), interval.StartString(), interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for nhosts hosts,
// e.g. in pseudo-SQL:
//
// SELECT MAX(metric1), ..., MAX(metricN)
// FROM cpu WHERE (hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour ORDER BY hour
func (d *Devops) MaxAllCPU(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.MaxAllDuration)
	whereHosts := d.getHostWhereString(nHosts)
	selectClauses := d.getSelectClausesAggMetrics("max", devops.GetAllCPUMetrics())

	humanLabel := devops.GetMaxAllLabel("Influx", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	influxql := fmt.Sprintf("SELECT %s from cpu where %s and time >= '%s' and time < '%s' group by time(1m)", strings.Join(selectClauses, ","), whereHosts, interval.StartString(), interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *Devops) LastPointPerHost(qi query.Query) {
	humanLabel := "Influx last row per host"
	humanDesc := humanLabel + ": cpu"
	influxql := "SELECT * from cpu group by \"hostname\" order by time desc limit 1"
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
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

	var hostWhereClause string
	if nHosts == 0 {
		hostWhereClause = ""
	} else {
		hostWhereClause = fmt.Sprintf("and %s", d.getHostWhereString(nHosts))
	}

	humanLabel, err := devops.GetHighCPULabel("Influx", nHosts)
	panicIfErr(err)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	influxql := fmt.Sprintf("SELECT * from cpu where usage_user > 90.0 %s and time >= '%s' and time < '%s'", hostWhereClause, interval.StartString(), interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, influxql)
}

func (d *Devops) fillInQuery(qi query.Query, humanLabel, humanDesc, influxql string) {
	v := url.Values{}
	v.Set("q", influxql)
	q := qi.(*query.HTTP)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}
