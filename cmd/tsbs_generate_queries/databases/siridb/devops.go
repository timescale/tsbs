package siridb

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

// Devops produces SiriDB-specific queries for all the devops query types.
type Devops struct {
	*devops.Core
}

// NewDevops makes an Devops object ready to generate Queries.
func NewDevops(start, end time.Time, scale int) *Devops {
	core, err := devops.NewCore(start, end, scale)
	panicIfErr(err)
	return &Devops{core}
}

// GenerateEmptyQuery returns an empty query.SiriDB
func (d *Devops) GenerateEmptyQuery() query.Query {
	return query.NewSiriDB()
}

func (d *Devops) getHostWhereWithHostnames(hostnames []string) string {
	hostnameClauses := []string{}
	for _, s := range hostnames {
		hostnameClauses = append(hostnameClauses, fmt.Sprintf("`%s`", s))
	}
	combinedHostnameClause := strings.Join(hostnameClauses, "|")
	return "(" + combinedHostnameClause + ")"
}

func (d *Devops) getHostWhereString(nhosts int) string {
	hostnames, err := d.GetRandomHosts(nhosts)
	panicIfErr(err)
	return d.getHostWhereWithHostnames(hostnames)
}

func (d *Devops) getMetricWhereString(metrics []string) string {
	metricsClauses := []string{}
	for _, s := range metrics {
		metricsClauses = append(metricsClauses, fmt.Sprintf("`%s`", s))
	}
	combinedMetricsClause := strings.Join(metricsClauses, "|")
	return "(" + combinedMetricsClause + ")"
}

const goTimeFmt = "2006-01-02 15:04:05Z"

// GroupByTime selects the MAX for numMetrics metrics in the group `cpu`,
// per minute for nhosts hosts,
// e.g. in pseudo-SQL:
//
// In case of 1 metric:
// select max(1m) from (`groupHost1` | ...) & `groupMetric1` between 'time1' and 'time2' merge as 'max METRIC for (HOST | ... ) using max(1)
//
// In case of multiple metrics
// NOTE: it is not possible to merge multiple hosts per metric for a list of
// metrics, only for one metric. In this case all series for the provided hosts
// and metrics are returned:
//
// select max(1m) from (`groupHost1` | ...) & (`groupMetric1` | ...) between 'time1' and 'time2'
func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	whereMetrics := d.getMetricWhereString(metrics)
	whereHosts := d.getHostWhereString(nHosts)

	humanLabel := fmt.Sprintf("SiriDB %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())

	var siriql string
	if numMetrics == 1 {
		siriql = fmt.Sprintf("select max(1m) from %s & %s between '%s' and '%s' merge as 'max %s for %s' using max(1)", whereHosts, whereMetrics, interval.StartString(), interval.EndString(), whereMetrics, whereHosts)
	} else {
		siriql = fmt.Sprintf("select max(1m) from %s & %s between '%s' and '%s'", whereHosts, whereMetrics, interval.StartString(), interval.EndString())
	}
	d.fillInQuery(qi, humanLabel, humanDesc, siriql)
}

// GroupByOrderByLimit populates a query.Query that has a time WHERE clause, that groups by a truncated date, orders by that date, and takes a limit:
//
// select max(1m) from `usage_user` between time - 5m and 'roundedTime' merge as 'max usage user of the last 5 aggregate readings' using max(1)
func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour)
	timeStr := interval.End().Format(goTimeFmt)

	timestrRounded := timeStr[:len(timeStr)-4] + ":00Z"
	where := fmt.Sprintf("between '%s' - 5m and '%s'", timeStr, timestrRounded)
	siriql := fmt.Sprintf("select max(1m) from `usage_user` %s merge as 'max usage user of the last 5 aggregate readings' using max(1)", where)

	humanLabel := "SiriDB max cpu over last 5 min-intervals (random end)"
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, siriql)
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics in the group `cpu` per device per hour for a day,
// e.g. in pseudo-SQL:
//
// select mean(1h) from (`groupMetric1` | ...) between 'time1' and 'time2'
func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	whereMetrics := d.getMetricWhereString(metrics)

	humanLabel := devops.GetDoubleGroupByLabel("SiriDB", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	siriql := fmt.Sprintf("select mean(1h) from %s between '%s' and '%s'", whereMetrics, interval.StartString(), interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, siriql)
}

// MaxAllCPU selects the MAX of all metrics in the group `cpu` per hour for nhosts hosts,
// e.g. in pseudo-SQL:
//
// NOTE: it is not possible to merge multiple hosts per metric for a list of
// metrics, only for one metric. In this case all series for the provided
// hosts and all `cpu` metrics are returned):
//
// select max(1h) from (`groupHost1` | ...) & `cpu` between 'time1' and 'time2'
func (d *Devops) MaxAllCPU(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.MaxAllDuration)

	whereMetrics := "`cpu`"
	whereHosts := d.getHostWhereString(nHosts)

	humanLabel := devops.GetMaxAllLabel("SiriDB", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	siriql := fmt.Sprintf("select max(1h) from %s & %s between '%s' and '%s'", whereHosts, whereMetrics, interval.StartString(), interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, siriql)
}

// LastPointPerHost finds the last value of every time serie within the CPU group.
//
// select last() from `cpu`
func (d *Devops) LastPointPerHost(qi query.Query) {
	siriql := "select last() from `cpu`"
	humanLabel := "SiriDB last row per host"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, siriql)
}

// HighCPUForHosts populates a query that gets CPU metrics when the CPU has high
// usage between a time period for a number of hosts (if 0, it will search all hosts),
// e.g. in pseudo-SQL:
//
// NOTE: It is not possible to return the other cpu metrics when e.g. usage_user
// has reached a threshold. Here only the metric "usage_user" is returned.
//
// nHosts=0:
// select filter(> 90) from `usage_user` between 'time1' and 'time2'
// nHosts>0:
// select filter(> 90) from `usage_user` & (`groupHost1` | ...) between 'time1' and 'time2'
func (d *Devops) HighCPUForHosts(qi query.Query, nHosts int) {
	var whereHosts string
	if nHosts == 0 {
		whereHosts = ""
	} else {
		whereHosts = "& " + d.getHostWhereString(nHosts)
	}
	interval := d.Interval.MustRandWindow(devops.HighCPUDuration)

	humanLabel, err := devops.GetHighCPULabel("SiriDB", nHosts)
	panicIfErr(err)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	siriql := fmt.Sprintf("select filter(> 90) from `usage_user` %s between '%s' and '%s'", whereHosts, interval.StartString(), interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, siriql)
}

func (d *Devops) fillInQuery(qi query.Query, humanLabel, humanDesc, sql string) {
	q := qi.(*query.SiriDB)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.SqlQuery = []byte(sql)
}
