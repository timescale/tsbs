package timestream

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

const (
	oneMinute = 60
	oneHour   = oneMinute * 60

	timeBucketFmt = "bin(time, %ds)"
)

// Devops produces Timestream-specific queries for all the devops query types.
type Devops struct {
	*BaseGenerator
	*devops.Core
}

// getHostWhereWithHostnames creates WHERE SQL statement for multiple hostnames.
// NOTE 'WHERE' itself is not included, just hostname filter clauses, ready to concatenate to 'WHERE' string
func (d *Devops) getHostWhereWithHostnames(hostnames []string) string {
	var hostnameClauses []string

	for _, s := range hostnames {
		hostnameClauses = append(hostnameClauses, fmt.Sprintf("hostname = '%s'", s))
	}
	combinedHostnameClause := strings.Join(hostnameClauses, " OR ")

	return "(" + combinedHostnameClause + ")"
}

// getHostWhereString gets multiple random hostnames and creates a WHERE SQL statement for these hostnames.
func (d *Devops) getHostWhereString(nHosts int) string {
	hostnames, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)
	return d.getHostWhereWithHostnames(hostnames)
}

// getMeasureNameWhereString returns a WHERE SQL statement for the given measure names
// [a,b] => (measure_name = 'a' OR measure_name = 'b')
func (d *Devops) getMeasureNameWhereString(measureNames []string) string {
	var measureClauses []string

	for _, s := range measureNames {
		measureClauses = append(measureClauses, fmt.Sprintf("measure_name = '%s'", s))
	}
	combinedMeasureClause := strings.Join(measureClauses, " OR ")

	return "(" + combinedMeasureClause + ")"
}
func (d *Devops) getTimeBucket(seconds int) string {
	return fmt.Sprintf(timeBucketFmt, seconds)
}

func (d *Devops) getSelectClausesAggMetrics(agg string, metrics []string) []string {
	selectClauses := make([]string, len(metrics))
	for i, m := range metrics {
		selectClauses[i] = fmt.Sprintf("%[1]s(case when measure_name = '%[2]s' THEN measure_value::double ELSE NULL END) as %[1]s_%[2]s", agg, m)
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
	if len(selectClauses) < 1 {
		panic(fmt.Sprintf("invalid number of select clauses: got %d", len(selectClauses)))
	}

	sql := fmt.Sprintf(`SELECT %s AS minute,
        %s
        FROM "%s"."cpu"
        WHERE %s AND %s AND time >= '%s' AND time < '%s'
        GROUP BY 1 ORDER BY 1 ASC`,
		d.getTimeBucket(oneMinute),
		strings.Join(selectClauses, ",\n"),
		d.DBName,
		d.getMeasureNameWhereString(metrics),
		d.getHostWhereString(nHosts),
		interval.Start().Format(goTimeFmt),
		interval.End().Format(goTimeFmt))

	humanLabel := fmt.Sprintf("Timestream %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

// GroupByOrderByLimit populates a query.Query that has a time WHERE clause, that groups by a truncated date, orders by that date, and takes a limit:
// SELECT time_bucket('1 minute', time) AS t, MAX(cpu) FROM cpu
// WHERE time < '$TIME'
// GROUP BY t ORDER BY t DESC
// LIMIT $LIMIT
func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour)
	sql := fmt.Sprintf(`SELECT %s AS minute, max(measure_value::double) as max_usage_user
        FROM "%s"."cpu"
        WHERE time < '%s' AND measure_name = 'usage_user'
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 5`,
		d.getTimeBucket(oneMinute),
		d.DBName,
		interval.End().Format(goTimeFmt))

	humanLabel := "Timestream max cpu over last 5 min-intervals (random end)"
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.EndString())
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

	selectClauses := make([]string, numMetrics)
	meanClauses := make([]string, numMetrics)
	for i, m := range metrics {
		meanClauses[i] = "mean_" + m
		selectClauses[i] = fmt.Sprintf("avg (case when measure_name = '%[1]s' THEN measure_value::double ELSE NULL END) as %[2]s", m, meanClauses[i])
	}

	sql := fmt.Sprintf(`
        SELECT %s as hour, 
			hostname,
			%s
		FROM "%s"."cpu"
		WHERE time >= '%s' AND time < '%s'
		GROUP BY 1, 2`,
		d.getTimeBucket(oneHour),
		strings.Join(selectClauses, ",\n\t\t\t"),
		d.DBName,
		interval.Start().Format(goTimeFmt),
		interval.End().Format(goTimeFmt))
	humanLabel := devops.GetDoubleGroupByLabel("Timestream", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
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

	metrics := devops.GetAllCPUMetrics()
	selectClauses := d.getSelectClausesAggMetrics("max", metrics)

	sql := fmt.Sprintf(`SELECT %s AS hour,
			%s
		FROM "%s"."cpu"
		WHERE %s AND time >= '%s' AND time < '%s'
		GROUP BY 1 ORDER BY 1`,
		d.getTimeBucket(oneHour),
		strings.Join(selectClauses, ",\n\t\t\t"),
		d.DBName,
		d.getHostWhereString(nHosts),
		interval.Start().Format(goTimeFmt),
		interval.End().Format(goTimeFmt))

	humanLabel := devops.GetMaxAllLabel("Timestream", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *Devops) LastPointPerHost(qi query.Query) {
	var sql string
	sql = fmt.Sprintf(`
	WITH latest_recorded_time AS (
		SELECT 
			hostname,
			measure_name,
			max(time) as latest_time
		FROM "%[1]s"."cpu"
		GROUP BY 1, 2
	)
	SELECT b.hostname, 
		b.measure_name, 
		b.measure_value::double, 
		b.time
	FROM latest_recorded_time a
	JOIN "%[1]s"."cpu" b
	ON a.hostname = b.hostname AND a.latest_time = b.time AND a.measure_name = b.measure_name
	ORDER BY hostname, measure_name`, d.DBName)
	humanLabel := "Timestream last row per host"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
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
	var hostWhereClause string
	if nHosts == 0 {
		hostWhereClause = ""
	} else {
		hostWhereClause = fmt.Sprintf("AND %s", d.getHostWhereString(nHosts))
	}
	interval := d.Interval.MustRandWindow(devops.HighCPUDuration)

	sql := fmt.Sprintf(`
		WITH usage_over_ninety AS (
			SELECT time, 
				hostname
			FROM "%s"."cpu"
			WHERE measure_name = 'usage_user' AND measure_value::double > 90
				AND time >= '%s' AND time < '%s'
				%s
		)
		SELECT * 
		FROM "%s"."cpu" a
		JOIN usage_over_ninety b ON a.hostname = b.hostname AND a.time = b.time`,
		d.DBName,
		interval.Start().Format(goTimeFmt),
		interval.End().Format(goTimeFmt),
		hostWhereClause,
		d.DBName,
	)

	humanLabel, err := devops.GetHighCPULabel("Timestream", nHosts)
	panicIfErr(err)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}
