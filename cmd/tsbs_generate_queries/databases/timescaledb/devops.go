package timescaledb

import (
	"fmt"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/query"
)

const (
	errMoreItemsThanScale = "cannot get random permutation with more items than scale"
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

	timeBucketFmt    = "time_bucket('%d seconds', time)"
	nonTimeBucketFmt = "to_timestamp(((extract(epoch from time)::int)/%d)*%d)"
)

// Devops produces TimescaleDB-specific queries for all the devops query types.
type Devops struct {
	*BaseGenerator
	*devops.Core
}

// getHostWhereWithHostnames creates WHERE SQL statement for multiple hostnames.
// NOTE 'WHERE' itself is not included, just hostname filter clauses, ready to concatenate to 'WHERE' string
func (d *Devops) getHostWhereWithHostnames(hostnames []string) string {
	hostnameClauses := []string{}
	if d.UseJSON {
		for _, s := range hostnames {
			hostnameClauses = append(hostnameClauses, fmt.Sprintf("tagset @> '{\"hostname\": \"%s\"}'", s))
		}
		return fmt.Sprintf("tags_id IN (SELECT id FROM tags WHERE %s)", strings.Join(hostnameClauses, " OR "))
	} else if d.UseTags {
		for _, s := range hostnames {
			hostnameClauses = append(hostnameClauses, fmt.Sprintf("'%s'", s))
		}
		return fmt.Sprintf("tags_id IN (SELECT id FROM tags WHERE hostname IN (%s))", strings.Join(hostnameClauses, ","))
	} else {
		for _, s := range hostnames {
			hostnameClauses = append(hostnameClauses, fmt.Sprintf("hostname = '%s'", s))
		}
		combinedHostnameClause := strings.Join(hostnameClauses, " OR ")

		return "(" + combinedHostnameClause + ")"
	}
}

// getHostWhereString gets multiple random hostnames and creates a WHERE SQL statement for these hostnames.
func (d *Devops) getHostWhereString(nHosts int) string {
	hostnames, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)
	return d.getHostWhereWithHostnames(hostnames)
}

func (d *Devops) getTimeBucket(seconds int) string {
	if d.UseTimeBucket {
		return fmt.Sprintf(timeBucketFmt, seconds)
	}
	return fmt.Sprintf(nonTimeBucketFmt, seconds, seconds)
}

func (d *Devops) getSelectClausesAggMetrics(agg string, metrics []string) []string {
	selectClauses := make([]string, len(metrics))
	for i, m := range metrics {
		selectClauses[i] = fmt.Sprintf("%[1]s(%[2]s) as %[1]s_%[2]s", agg, m)
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
        FROM cpu
        WHERE %s AND time >= '%s' AND time < '%s'
        GROUP BY minute ORDER BY minute ASC`,
		d.getTimeBucket(oneMinute),
		strings.Join(selectClauses, ", "),
		d.getHostWhereString(nHosts),
		interval.Start().Format(goTimeFmt),
		interval.End().Format(goTimeFmt))

	humanLabel := fmt.Sprintf("TimescaleDB %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
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
	sql := fmt.Sprintf(`SELECT %s AS minute, max(usage_user)
        FROM cpu
        WHERE time < '%s'
        GROUP BY minute
        ORDER BY minute DESC
        LIMIT 5`,
		d.getTimeBucket(oneMinute),
		interval.End().Format(goTimeFmt))

	humanLabel := "TimescaleDB max cpu over last 5 min-intervals (random end)"
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
		selectClauses[i] = fmt.Sprintf("avg(%s) as %s", m, meanClauses[i])
	}

	hostnameField := "hostname"
	joinStr := ""
	if d.UseJSON || d.UseTags {
		if d.UseJSON {
			hostnameField = "tags->>'hostname'"
		} else if d.UseTags {
			hostnameField = "tags.hostname"
		}
		joinStr = "JOIN tags ON cpu_avg.tags_id = tags.id"
	}

	sql := fmt.Sprintf(`
        WITH cpu_avg AS (
          SELECT %s as hour, tags_id,
          %s
          FROM cpu
          WHERE time >= '%s' AND time < '%s'
          GROUP BY hour, tags_id
        )
        SELECT hour, %s, %s
        FROM cpu_avg
        %s
        ORDER BY hour, %s`,
		d.getTimeBucket(oneHour),
		strings.Join(selectClauses, ", "),
		interval.Start().Format(goTimeFmt),
		interval.End().Format(goTimeFmt),
		hostnameField, strings.Join(meanClauses, ", "),
		joinStr, hostnameField)
	humanLabel := devops.GetDoubleGroupByLabel("TimescaleDB", numMetrics)
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
        FROM cpu
        WHERE %s AND time >= '%s' AND time < '%s'
        GROUP BY hour ORDER BY hour`,
		d.getTimeBucket(oneHour),
		strings.Join(selectClauses, ", "),
		d.getHostWhereString(nHosts),
		interval.Start().Format(goTimeFmt),
		interval.End().Format(goTimeFmt))

	humanLabel := devops.GetMaxAllLabel("TimescaleDB", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *Devops) LastPointPerHost(qi query.Query) {
	var sql string
	if d.UseTags {
		sql = fmt.Sprintf("SELECT DISTINCT ON (t.hostname) * FROM tags t INNER JOIN LATERAL(SELECT * FROM cpu c WHERE c.tags_id = t.id ORDER BY time DESC LIMIT 1) AS b ON true ORDER BY t.hostname, b.time DESC")
	} else if d.UseJSON {
		sql = fmt.Sprintf("SELECT DISTINCT ON (t.tagset->>'hostname') * FROM tags t INNER JOIN LATERAL(SELECT * FROM cpu c WHERE c.tags_id = t.id ORDER BY time DESC LIMIT 1) AS b ON true ORDER BY t.tagset->>'hostname', b.time DESC")
	} else {
		sql = fmt.Sprintf(`SELECT DISTINCT ON (hostname) * FROM cpu ORDER BY hostname, time DESC`)
	}

	humanLabel := "TimescaleDB last row per host"
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

	sql := fmt.Sprintf(`SELECT * FROM cpu WHERE usage_user > 90.0 and time >= '%s' AND time < '%s' %s`,
		interval.Start().Format(goTimeFmt), interval.End().Format(goTimeFmt), hostWhereClause)

	humanLabel, err := devops.GetHighCPULabel("TimescaleDB", nHosts)
	panicIfErr(err)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}
