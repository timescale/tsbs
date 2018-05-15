package main

import (
	"fmt"
	"strings"
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// TimescaleDBDevops produces TimescaleDB-specific queries for all the devops query types.
type TimescaleDBDevops struct {
	*devopsCore
}

// NewTimescaleDBDevops makes an TimescaleDBDevops object ready to generate Queries.
func newTimescaleDBDevopsCommon(start, end time.Time, scale int) *TimescaleDBDevops {
	return &TimescaleDBDevops{newDevopsCore(start, end, scale)}
}

func (d *TimescaleDBDevops) getHostWhereWithHostnames(hostnames []string) string {
	if timescaleUseJSON {
		hostnameClauses := []string{}
		for _, s := range hostnames {
			hostnameClauses = append(hostnameClauses, fmt.Sprintf("tagset @> '{\"hostname\": \"%s\"}'", s))
		}
		return fmt.Sprintf("tags_id IN (SELECT id FROM tags WHERE %s)", strings.Join(hostnameClauses, " OR "))
	} else if timescaleUseTags {
		hostnameClauses := []string{}
		for _, s := range hostnames {
			hostnameClauses = append(hostnameClauses, fmt.Sprintf("'%s'", s))
		}
		return fmt.Sprintf("tags_id IN (SELECT id FROM tags WHERE hostname IN (%s))", strings.Join(hostnameClauses, " OR "))
	} else {
		hostnameClauses := []string{}
		for _, s := range hostnames {
			hostnameClauses = append(hostnameClauses, fmt.Sprintf("hostname = '%s'", s))
		}

		combinedHostnameClause := strings.Join(hostnameClauses, " OR ")

		return "(" + combinedHostnameClause + ")"
	}
}

func (d *TimescaleDBDevops) getHostWhereString(nhosts int) string {
	hostnames := d.getRandomHosts(nhosts)
	return d.getHostWhereWithHostnames(hostnames)
}

func (d *TimescaleDBDevops) getSelectClausesAggMetrics(agg string, metrics []string) []string {
	selectClauses := make([]string, len(metrics))
	for i, m := range metrics {
		selectClauses[i] = fmt.Sprintf("%s(%s) as mean_%s", agg, m, m)
	}

	return selectClauses
}

const goTimeFmt = "2006-01-02 15:04:05.999999 -0700"

// MaxCPUMetricsByMinute selects the MAX for numMetrics metrics under 'cpu',
// per minute for nhosts hosts,
// e.g. in psuedo-SQL:
//
// SELECT minute, max(metric1), ..., max(metricN)
// FROM cpu
// WHERE (hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY minute ORDER BY minute ASC
func (d *TimescaleDBDevops) MaxCPUMetricsByMinute(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.interval.RandWindow(timeRange)
	metrics := getCPUMetricsSlice(numMetrics)
	selectClauses := d.getSelectClausesAggMetrics("max", metrics)

	sqlQuery := fmt.Sprintf(`SELECT date_trunc('minute', time) AS minute,
    %s
    FROM cpu
    WHERE %s AND time >= '%s' AND time < '%s'
    GROUP BY minute ORDER BY minute ASC`,
		strings.Join(selectClauses, ", "),
		d.getHostWhereString(nHosts),
		interval.Start.Format(goTimeFmt),
		interval.End.Format(goTimeFmt))

	humanLabel := fmt.Sprintf("TimescaleDB %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	q := qi.(*query.TimescaleDB)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.Hypertable = []byte("cpu")
	q.SqlQuery = []byte(sqlQuery)
}

// GroupByOrderByLimit populates a query.Query that has a time WHERE clause, that groups by a truncated date, orders by that date, and takes a limit:
// SELECT date_trunc('minute', time) AS t, MAX(cpu) FROM cpu
// WHERE time < '$TIME'
// GROUP BY t ORDER BY t DESC
// LIMIT $LIMIT
func (d *TimescaleDBDevops) GroupByOrderByLimit(qi query.Query) {
	interval := d.interval.RandWindow(time.Hour)
	timeStr := interval.End.Format(goTimeFmt)

	where := fmt.Sprintf("WHERE time < '%s'", timeStr)

	sqlQuery := fmt.Sprintf(`SELECT date_trunc('minute', time) AS minute, max(usage_user) FROM cpu %s GROUP BY minute ORDER BY minute DESC LIMIT 5`, where)

	humanLabel := "TimescaleDB max cpu over last 5 min-intervals (rand end)"
	q := qi.(*query.TimescaleDB)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.EndString()))
	q.Hypertable = []byte("cpu")
	q.SqlQuery = []byte(sqlQuery)
}

// MeanCPUMetricsDayByHourAllHostsGroupbyHost selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day,
// e.g. in psuedo-SQL:
//
// SELECT AVG(metric1), ..., AVG(metricN)
// FROM cpu
// WHERE time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour, hostname ORDER BY hour
func (d *TimescaleDBDevops) MeanCPUMetricsDayByHourAllHostsGroupbyHost(qi query.Query, numMetrics int) {
	metrics := getCPUMetricsSlice(numMetrics)
	interval := d.interval.RandWindow(24 * time.Hour)

	selectClauses := make([]string, numMetrics)
	meanClauses := make([]string, numMetrics)
	for i, m := range metrics {
		meanClauses[i] = "mean_" + m
		selectClauses[i] = fmt.Sprintf("avg(%s) as %s", m, meanClauses[i])
	}

	hostnameField := "hostname"
	joinStr := ""
	if timescaleUseJSON || timescaleUseTags {
		if timescaleUseJSON {
			hostnameField = "tags->>'hostname'"
		} else if timescaleUseTags {
			hostnameField = "tags.hostname"
		}
		joinStr = "JOIN tags ON cpu_avg.tags_id = tags.id"
	}

	sqlQuery := fmt.Sprintf(`
        WITH cpu_avg AS (
          SELECT date_trunc('hour', time) as hour, tags_id,
          %s
          FROM cpu
          WHERE time >= '%s' AND time < '%s'
          GROUP BY hour, tags_id
        )
        SELECT hour, %s, %s
        FROM cpu_avg
        %s
        ORDER BY hour, %s`,
		strings.Join(selectClauses, ", "),
		interval.Start.Format(goTimeFmt), interval.End.Format(goTimeFmt),
		hostnameField, strings.Join(meanClauses, ", "),
		joinStr, hostnameField)
	humanLabel := fmt.Sprintf("TimescaleDB mean of %d metrics, all hosts, rand 1day by 1hr", numMetrics)
	q := qi.(*query.TimescaleDB)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.Hypertable = []byte("cpu")
	q.SqlQuery = []byte(sqlQuery)
}

// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for nhosts hosts,
// e.g. in psuedo-SQL:
//
// SELECT MAX(metric1), ..., MAX(metricN)
// FROM cpu WHERE (hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour ORDER BY hour
func (d *TimescaleDBDevops) MaxAllCPU(qi query.Query, nhosts int) {
	interval := d.interval.RandWindow(8 * time.Hour)
	metrics := getCPUMetricsSlice(len(cpuMetrics))
	selectClauses := d.getSelectClausesAggMetrics("max", metrics)

	sqlQuery := fmt.Sprintf(`SELECT date_trunc('hour', time) AS hour,
    %s
    FROM cpu
	WHERE %s AND time >= '%s' AND time < '%s'
	GROUP BY hour ORDER BY hour`,
		strings.Join(selectClauses, ", "),
		d.getHostWhereString(nhosts),
		interval.Start.Format(goTimeFmt), interval.End.Format(goTimeFmt))

	humanLabel := fmt.Sprintf("TimescaleDB max cpu all fields, rand %4d hosts, rand 8hr by 1h", nhosts)
	q := qi.(*query.TimescaleDB)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.Hypertable = []byte("cpu")
	q.SqlQuery = []byte(sqlQuery)
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *TimescaleDBDevops) LastPointPerHost(qi query.Query) {
	var sqlQuery string
	if timescaleUseTags {
		sqlQuery = fmt.Sprintf("SELECT DISTINCT ON (t.hostname) * FROM tags t INNER JOIN LATERAL(SELECT * FROM cpu c WHERE c.tags_id = t.id ORDER BY time DESC LIMIT 1) AS b ON true ORDER BY t.hostname, b.time DESC")
	} else if timescaleUseJSON {
		sqlQuery = fmt.Sprintf("SELECT DISTINCT ON (t.tagset->>'hostname') * FROM tags t INNER JOIN LATERAL(SELECT * FROM cpu c WHERE c.tags_id = t.id ORDER BY time DESC LIMIT 1) AS b ON true ORDER BY t.tagset->>'hostname', b.time DESC")
	} else {
		sqlQuery = fmt.Sprintf(`SELECT DISTINCT ON (hostname) * FROM cpu ORDER BY hostname, time DESC`)
	}

	humanLabel := "TimescaleDB last row per host"
	q := qi.(*query.TimescaleDB)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s", humanLabel))
	q.Hypertable = []byte("cpu")
	q.SqlQuery = []byte(sqlQuery)
}

// HighCPUForHosts populates a query that gets CPU metrics when the CPU has high
// usage between a time period for a number of hosts (if 0, it will search all hosts),
// e.g. in psuedo-SQL:
//
// SELECT * FROM cpu
// WHERE usage_user > 90.0
// AND time >= '$TIME_START' AND time < '$TIME_END'
// AND (hostname = '$HOST' OR hostname = '$HOST2'...)
func (d *TimescaleDBDevops) HighCPUForHosts(qi query.Query, nhosts int) {
	var hostWhereClause string
	if nhosts == 0 {
		hostWhereClause = ""
	} else {
		hostWhereClause = fmt.Sprintf("AND %s", d.getHostWhereString(nhosts))
	}
	interval := d.interval.RandWindow(24 * time.Hour)

	sqlQuery := fmt.Sprintf(`SELECT * FROM cpu WHERE usage_user > 90.0 and time >= '%s' AND time < '%s' %s`,
		interval.Start.Format(goTimeFmt), interval.End.Format(goTimeFmt), hostWhereClause)

	humanLabel := "TimescaleDB CPU over threshold, "
	if nhosts > 0 {
		humanLabel += fmt.Sprintf("%d host(s)", nhosts)
	} else {
		humanLabel += "all hosts"
	}
	q := qi.(*query.TimescaleDB)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.Hypertable = []byte("cpu")
	q.SqlQuery = []byte(sqlQuery)
}
