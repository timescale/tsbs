package tdengine

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

// Devops produces TimescaleDB-specific queries for all the devops query types.
type Devops struct {
	*BaseGenerator
	*devops.Core
}

// getHostWhereWithHostnames creates WHERE SQL statement for multiple hostnames.
// NOTE 'WHERE' itself is not included, just hostname filter clauses, ready to concatenate to 'WHERE' string
func (d *Devops) getHostWhereWithHostnames(hostnames []string) string {
	var hostnameClauses []string
	for _, s := range hostnames {
		hostnameClauses = append(hostnameClauses, fmt.Sprintf("'%s'", s))
	}
	return fmt.Sprintf("tbname IN (%s)", strings.Join(hostnameClauses, ","))
}

// getHostWhereString gets multiple random hostnames and creates a WHERE SQL statement for these hostnames.
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

func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	selectClauses := d.getSelectClausesAggMetrics("max", metrics)
	if len(selectClauses) < 1 {
		panic(fmt.Sprintf("invalid number of select clauses: got %d", len(selectClauses)))
	}

	//SELECT _wstart as ts,max(usage_user) FROM cpu WHERE tbname IN ('host_249') AND ts >= 1451618560000 AND ts < 1451622160000 INTERVAL(1m) ;
	//SELECT _wstart as ts,max(usage_user) FROM host_249 WHERE ts >= 1451618560000 AND ts < 1451622160000 INTERVAL(1m) ;
	sql := ""
	if nHosts == 1 {
		hostnames, err := d.GetRandomHosts(nHosts)
		panicIfErr(err)
		sql = fmt.Sprintf(`SELECT  _wstart as ts,%s FROM %s WHERE ts >= %d AND ts < %d INTERVAL(1m)`,
			strings.Join(selectClauses, ", "),
			hostnames[0],
			interval.StartUnixMillis(),
			interval.EndUnixMillis())

	} else {
		sql = fmt.Sprintf(`SELECT  _wstart as ts,%s FROM cpu WHERE %s AND ts >= %d AND ts < %d INTERVAL(1m)`,
			strings.Join(selectClauses, ", "),
			d.getHostWhereString(nHosts),
			interval.StartUnixMillis(),
			interval.EndUnixMillis())
	}

	humanLabel := fmt.Sprintf("TDengine %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour)
	//SELECT _wstart as ts,max(usage_user) FROM cpu WHERE ts < 1451618228646 INTERVAL(1m) LIMIT 5;
	sql := fmt.Sprintf(`SELECT _wstart as ts,max(usage_user) FROM cpu WHERE ts < %d INTERVAL(1m) LIMIT 5`,
		interval.EndUnixMillis())

	humanLabel := "TDengine max cpu over last 5 min-intervals (random end)"
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day,
func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)

	selectClauses := d.getSelectClausesAggMetrics("avg", metrics)
	//SELECT _wstart as ts,tbname, avg(usage_user) from cpu where ts >= 1451733760646 and ts < 1451776960646 partition by tbname interval(1h) order by tbname,ts asc;
	//SELECT _wstart as ts,tbname, avg(usage_user), avg(usage_system), avg(usage_idle), avg(usage_nice), avg(usage_iowait), avg(usage_irq), avg(usage_softirq), avg(usage_steal), avg(usage_guest), avg(usage_guest_nice) from cpu where ts >= 1451733760646 and ts < 1451776960646 partition by tbname interval(1h) order by tbname,ts asc;
	sql := fmt.Sprintf("SELECT _wstart as ts,tbname,%s from cpu where ts >= %d and ts < %d partition by tbname interval(1h) order by tbname,ts asc", strings.Join(selectClauses, ", "), interval.StartUnixMillis(), interval.EndUnixMillis())

	humanLabel := devops.GetDoubleGroupByLabel("TDengine", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

func (d *Devops) MaxAllCPU(qi query.Query, nHosts int, duration time.Duration) {
	interval := d.Interval.MustRandWindow(duration)

	metrics := devops.GetAllCPUMetrics()
	selectClauses := d.getSelectClausesAggMetrics("max", metrics)
	//SELECT _wstart as ts,max(usage_user), max(usage_system), max(usage_idle), max(usage_nice), max(usage_iowait), max(usage_irq), max(usage_softirq), max(usage_steal), max(usage_guest), max(usage_guest_nice) FROM host_249 WHERE ts >= 1451648911646 AND ts < 1451677711646 interval(1h);
	//SELECT_wstart as ts, max(usage_user), max(usage_system), max(usage_idle), max(usage_nice), max(usage_iowait), max(usage_irq), max(usage_softirq), max(usage_steal), max(usage_guest), max(usage_guest_nice) FROM cpu WHERE tbname IN ('host_249','host_403','host_435','host_39','host_139','host_75','host_315','host_121') AND ts >= 1451648911646 AND ts < 1451677711646 interval(1h)

	sql := ""
	if nHosts == 1 {
		hostnames, err := d.GetRandomHosts(nHosts)
		panicIfErr(err)
		sql = fmt.Sprintf(`SELECT _wstart as ts,%s FROM %s  WHERE ts >= %d AND ts < %d interval(1h)`,
			strings.Join(selectClauses, ", "),
			hostnames[0],
			interval.StartUnixMillis(),
			interval.EndUnixMillis())
	} else {
		sql = fmt.Sprintf(`SELECT _wstart as ts,%s FROM cpu WHERE %s AND ts >= %d AND ts < %d interval(1h)`,
			strings.Join(selectClauses, ", "),
			d.getHostWhereString(nHosts),
			interval.StartUnixMillis(),
			interval.EndUnixMillis())
	}
	humanLabel := devops.GetMaxAllLabel("TDengine", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

func (d *Devops) LastPointPerHost(qi query.Query) {
	//SELECT last_row(*),tbname from cpu group by tbname;
	sql := "SELECT last_row(*),tbname from cpu group by tbname"
	humanLabel := "TDengine last row per host"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

func (d *Devops) HighCPUForHosts(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.HighCPUDuration)
	var hostWhereClause string
	if nHosts == 0 {
		hostWhereClause = ""
	} else {
		hostWhereClause = fmt.Sprintf("AND %s", d.getHostWhereString(nHosts))
	}
	//SELECT ts,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice FROM cpu WHERE usage_user > 90.0 and ts >= 1451777731138 AND ts < 1451820931138 AND tbname IN ('host_35')
	//modify:SELECT * FROM host_35 WHERE usage_user  > 90.0 and ts >= 1451777731138 AND ts < 1451820931138

	sql := ""
	if nHosts == 1 {
		hostnames, err := d.GetRandomHosts(nHosts)
		panicIfErr(err)
		sql = fmt.Sprintf(`SELECT * FROM %s WHERE usage_user > 90.0 and ts >= %d AND ts < %d `,
			hostnames[0], interval.StartUnixMillis(), interval.EndUnixMillis())
	} else {
		sql = fmt.Sprintf(`SELECT ts,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice FROM cpu WHERE usage_user > 90.0 and ts >= %d AND ts < %d %s`,
			interval.StartUnixMillis(), interval.EndUnixMillis(), hostWhereClause)
	}
	humanLabel, err := devops.GetHighCPULabel("TDengine", nHosts)
	panicIfErr(err)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}
