package main

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"
)

// TimescaleDBDevops produces Influx-specific queries for all the devops query types.
type TimescaleDBDevops struct {
	AllInterval TimeInterval
}

// NewTimescaleDBDevops makes an InfluxDevops object ready to generate Queries.
func newTimescaleDBDevopsCommon(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	if !start.Before(end) {
		panic("bad time order")
	}

	return &TimescaleDBDevops{
		AllInterval: NewTimeInterval(start, end),
	}
}

func getHostWhereWithHostnames(hostnames []string) string {
	if timescaleUseJSON {
		hostnameClauses := []string{}
		for _, s := range hostnames {
			hostnameClauses = append(hostnameClauses, fmt.Sprintf("tagset @> '{\"hostname\": \"%s\"}'", s))
		}
		return fmt.Sprintf("tags_id IN (SELECT id FROM tags WHERE %s)", strings.Join(hostnameClauses, " OR "))
	} else if timescaleUseTags {
		hostnameClauses := []string{}
		for _, s := range hostnames {
			hostnameClauses = append(hostnameClauses, fmt.Sprintf("hostname = '%s'", s))
		}
		return fmt.Sprintf("tags_id IN (SELECT id FROM tags WHERE %s)", strings.Join(hostnameClauses, " OR "))
	} else {
		hostnameClauses := []string{}
		for _, s := range hostnames {
			hostnameClauses = append(hostnameClauses, fmt.Sprintf("hostname = '%s'", s))
		}

		combinedHostnameClause := strings.Join(hostnameClauses, " OR ")

		return "(" + combinedHostnameClause + ")"
	}
}

func getHostWhereString(scaleVar int, nhosts int) string {
	if nhosts > scaleVar {
		log.Fatal("nhosts > scaleVar")
	}

	nn := rand.Perm(scaleVar)[:nhosts]

	hostnames := []string{}
	for _, n := range nn {
		hostnames = append(hostnames, fmt.Sprintf("host_%d", n))
	}

	return getHostWhereWithHostnames(hostnames)
}

// Dispatch fulfills the QueryGenerator interface.
func (d *TimescaleDBDevops) Dispatch(i, scaleVar int) Query {
	q := NewTimescaleDBQuery() // from pool
	devopsDispatchAll(d, i, q, scaleVar)
	return q
}

// MaxCPUUsageHourByMinuteOneHost populates a Query for max(cpu) per minute over 1 hour on 1 host
func (d *TimescaleDBDevops) MaxCPUUsageHourByMinuteOneHost(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 1, time.Hour)
}

// MaxCPUUsageHourByMinuteTwoHosts populates a Query for max(cpu) per minute over 1 hour on 2 hosts
func (d *TimescaleDBDevops) MaxCPUUsageHourByMinuteTwoHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 2, time.Hour)
}

// MaxCPUUsageHourByMinuteFourHosts populates a Query for max(cpu) per minute over 1 hour on 4 hosts
func (d *TimescaleDBDevops) MaxCPUUsageHourByMinuteFourHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 4, time.Hour)
}

// MaxCPUUsageHourByMinuteEightHosts populates a Query for max(cpu) per minute over 1 hour on 8 hosts
func (d *TimescaleDBDevops) MaxCPUUsageHourByMinuteEightHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 8, time.Hour)
}

// MaxCPUUsageHourByMinuteSixteenHosts populates a Query for max(cpu) per minute over 1 hour on 16 hosts
func (d *TimescaleDBDevops) MaxCPUUsageHourByMinuteSixteenHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 16, time.Hour)
}

// MaxCPUUsageHourByMinuteThirtyTwoHosts populates a Query for max(cpu) per minute over 1 hour on 32 hosts
func (d *TimescaleDBDevops) MaxCPUUsageHourByMinuteThirtyTwoHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 32, time.Hour)
}

// MaxCPUUsage12HoursByMinuteOneHost populates a Query for max(cpu) per minute over 12 hours on 1 host
func (d *TimescaleDBDevops) MaxCPUUsage12HoursByMinuteOneHost(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 1, 12*time.Hour)
}

const goTimeFmt = "2006-01-02 15:04:05.999999 -7:00"

// SELECT minute, MAX(usage_user) FROM cpu
// WHERE (hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY minute ORDER BY minute ASC
func (d *TimescaleDBDevops) maxCPUUsageHourByMinuteNHosts(qi Query, scaleVar, nhosts int, timeRange time.Duration) {
	interval := d.AllInterval.RandWindow(timeRange)

	sqlQuery := fmt.Sprintf(`SELECT date_trunc('minute', time) AS minute, max(usage_user) FROM cpu where %s AND time >= '%s' AND time < '%s' GROUP BY minute ORDER BY minute ASC`, getHostWhereString(scaleVar, nhosts), interval.Start.Format(goTimeFmt), interval.End.Format(goTimeFmt))

	humanLabel := fmt.Sprintf("TimescaleDB max cpu, rand %4d hosts, rand %s by 1m", nhosts, timeRange)
	q := qi.(*TimescaleDBQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.NamespaceName = []byte("cpu")
	q.FieldName = []byte("usage_user")
	q.SqlQuery = []byte(sqlQuery)
}

// CPU5Metrics selects the MAX of 5 metrics under 'cpu' per minute for nhosts hosts,
// e.g. in psuedo-SQL:
//
// SELECT minute, max(metric1), ..., max(metric5)
// FROM cpu
// WHERE (hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY minute ORDER BY minute ASC
func (d *TimescaleDBDevops) CPU5Metrics(qi Query, scaleVar, nhosts int, timeRange time.Duration) {
	interval := d.AllInterval.RandWindow(timeRange)

	sqlQuery := fmt.Sprintf(`SELECT date_trunc('minute', time) AS minute,
    max(usage_user) AS max_usage_user,
    max(usage_system) AS max_usage_system,
    max(usage_idle) AS max_usage_idle,
    max(usage_nice) AS max_usage_nice,
    max(usage_guest) AS max_usage_guest
    FROM cpu
    WHERE %s AND time >= '%s' AND time < '%s'
    GROUP BY minute ORDER BY minute ASC`, getHostWhereString(scaleVar, nhosts), interval.Start.Format(goTimeFmt), interval.End.Format(goTimeFmt))

	humanLabel := fmt.Sprintf("TimescaleDB 5 cpu metrics, rand %4d hosts, rand %s by 1m", nhosts, timeRange)
	q := qi.(*TimescaleDBQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.NamespaceName = []byte("cpu")
	q.FieldName = []byte("*")
	q.SqlQuery = []byte(sqlQuery)
}

// GroupByOrderByLimit populates a Query that has a time WHERE clause, that groups by a truncated date, orders by that date, and takes a limit:
// SELECT date_trunc('minute', time) AS t, MAX(cpu) FROM cpu
// WHERE time < '$TIME'
// GROUP BY t ORDER BY t DESC
// LIMIT $LIMIT
func (d *TimescaleDBDevops) GroupByOrderByLimit(qi Query, _ int) {
	interval := d.AllInterval.RandWindow(12 * time.Hour)
	timeStr := interval.End.Format(goTimeFmt)

	where := fmt.Sprintf("WHERE time < '%s'", timeStr)

	sqlQuery := fmt.Sprintf(`SELECT date_trunc('minute', time) AS minute, max(usage_user) FROM cpu %s GROUP BY minute ORDER BY minute DESC LIMIT 5`, where)

	humanLabel := "TimescaleDB max cpu over last 5 min-intervals (rand end)"
	q := qi.(*TimescaleDBQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.NamespaceName = []byte("cpu")
	q.FieldName = []byte("usage_user")
	q.SqlQuery = []byte(sqlQuery)
}

// MeanCPUUsageDayByHourAllHostsGroupbyHost populates a Query with a query that looks like:
// SELECT mean(usage_user) from cpu where time >= '$DAY_START' and time < '$DAY_END' group by time(1h),hostname
func (d *TimescaleDBDevops) MeanCPUUsageDayByHourAllHostsGroupbyHost(qi Query, _ int) {
	humanLabel := "TimescaleDB mean cpu usage, all hosts, rand 1day by 1hour"
	d.meanCPUDayByHourAllHostsGroupbyHost(qi, []string{"usage_user"}, humanLabel)
}

// MeanCPUMetricsDayByHourAllHostsGroupbyHost populates a Query with a query that looks like:
// SELECT mean(usage_user) from cpu where time >= '$DAY_START' and time < '$DAY_END' group by time(1h),hostname
func (d *TimescaleDBDevops) MeanCPUMetricsDayByHourAllHostsGroupbyHost(qi Query, _ int) {
	metrics := []string{
		"usage_user",
		"usage_system",
		"usage_idle",
		"usage_nice",
		"usage_iowait",
		"usage_irq",
		"usage_softirq",
		"usage_steal",
		"usage_guest",
		"usage_guest_nice",
	}
	humanLabel := "TimescaleDB mean all cpu metrics, all hosts, rand 1day by 1hour"
	d.meanCPUDayByHourAllHostsGroupbyHost(qi, metrics, humanLabel)
}

func (d *TimescaleDBDevops) meanCPUDayByHourAllHostsGroupbyHost(qi Query, metrics []string, label string) {
	if len(metrics) < 0 {
		panic("no metrics given")
	}
	interval := d.AllInterval.RandWindow(24 * time.Hour)

	selectClauses := make([]string, len(metrics))
	for i, m := range metrics {
		selectClauses[i] = fmt.Sprintf("avg(%s) as mean_%s", m, m)
	}

	sqlQuery := fmt.Sprintf(`SELECT date_trunc('hour', time) as hour, hostname,
    %s
    FROM cpu WHERE time >= '%s' AND time < '%s'
    GROUP BY hour, hostname ORDER BY hour`, strings.Join(selectClauses, ","), interval.Start.Format(goTimeFmt), interval.End.Format(goTimeFmt))

	humanLabel := label
	q := qi.(*TimescaleDBQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.NamespaceName = []byte("cpu")
	q.FieldName = []byte("*")
	q.SqlQuery = []byte(sqlQuery)
}

// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for nhosts hosts,
// e.g. in psuedo-SQL:
//
// SELECT MAX(metric1), ..., MAX(metricN)
// FROM cpu WHERE (hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour ORDER BY hour
func (d *TimescaleDBDevops) MaxAllCPU(qi Query, scaleVar, nhosts int) {
	interval := d.AllInterval.RandWindow(12 * time.Hour)

	sqlQuery := fmt.Sprintf(`SELECT date_trunc('hour', time) AS hour,
    max(usage_user) AS max_usage_user,
    max(usage_system) AS max_usage_system,
    max(usage_idle) AS max_usage_idle,
    max(usage_nice) AS max_usage_nice,
    max(usage_iowait) AS max_usage_iowait,
    max(usage_irq) AS max_usage_irq,
    max(usage_softirq) AS max_usage_softirq,
    max(usage_steal) AS max_usage_steal,
    max(usage_guest) AS max_usage_guest,
    max(usage_guest_nice) AS max_usage_guest_nice
    FROM cpu where %s AND time >= '%s' AND time < '%s' GROUP BY hour ORDER BY hour`, getHostWhereString(scaleVar, nhosts), interval.Start.Format(goTimeFmt), interval.End.Format(goTimeFmt))

	humanLabel := fmt.Sprintf("TimescaleDB max cpu all fields, rand %4d hosts, rand 12hr by 1h", nhosts)
	q := qi.(*TimescaleDBQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.NamespaceName = []byte("cpu")
	q.FieldName = []byte("*")
	q.SqlQuery = []byte(sqlQuery)
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *TimescaleDBDevops) LastPointPerHost(qi Query, _ int) {
	measure := measurements[rand.Intn(len(measurements))]

	var sqlQuery string
	if timescaleUseTags {
		sqlQuery = fmt.Sprintf("SELECT DISTINCT ON (t.hostname) * FROM tags t INNER JOIN LATERAL(SELECT * FROM cpu c WHERE c.tags_id = t.id ORDER BY time DESC LIMIT 1) AS b ON true ORDER BY t.hostname, b.time DESC")
	} else if timescaleUseJSON {
		sqlQuery = fmt.Sprintf("SELECT DISTINCT ON (t.tagset->>'hostname') * FROM tags t INNER JOIN LATERAL(SELECT * FROM cpu c WHERE c.tags_id = t.id ORDER BY time DESC LIMIT 1) AS b ON true ORDER BY t.tagset->>hostname, b.time DESC")
	} else {
		sqlQuery = fmt.Sprintf(`SELECT DISTINCT ON (hostname) * FROM cpu ORDER BY hostname, time DESC`)
	}

	humanLabel := "TimescaleDB last row per host"
	q := qi.(*TimescaleDBQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, measure))
	q.NamespaceName = []byte(measure)
	q.FieldName = []byte("*")
	q.SqlQuery = []byte(sqlQuery)
}

// HighCPU populates a query that gets CPU metrics when the CPU has high
// usage between a time period across all hosts
// i.e. SELECT * FROM cpu WHERE usage_user > 90.0 AND time >= '$TIME_START' AND time < '$TIME_END'
func (d *TimescaleDBDevops) HighCPU(qi Query, _ int) {
	d.highCPUForHost(qi, "")
}

// HighCPUAndField populates a query that gets CPU metrics when the CPU has high
// usage between a time period for a particular host
// i.e. SELECT * FROM cpu WHERE usage_user > 90.0 AND time >= '$TIME_START' AND time < '$TIME_END' AND hostname = '$HOST'
func (d *TimescaleDBDevops) HighCPUAndField(qi Query, scaleVar int) {
	d.highCPUForHost(qi, fmt.Sprintf("AND (%s)", getHostWhereString(scaleVar, 1)))
}

func (d *TimescaleDBDevops) highCPUForHost(qi Query, hostWhereClause string) {
	interval := d.AllInterval.RandWindow(24 * time.Hour)

	sqlQuery := fmt.Sprintf(`SELECT * FROM cpu WHERE usage_user > 90.0 and time >= '%s' AND time < '%s' %s`,
		interval.Start.Format(goTimeFmt), interval.End.Format(goTimeFmt), hostWhereClause)

	humanLabel := "TimescaleDB CPU over threshold, "
	if len(hostWhereClause) > 0 {
		humanLabel += "one host"
	} else {
		humanLabel += "all hosts"
	}
	q := qi.(*TimescaleDBQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.NamespaceName = []byte("cpu")
	q.FieldName = []byte("*")
	q.SqlQuery = []byte(sqlQuery)
}

// "SELECT * from mem where used_percent > 98.0 or used > 10000 or used_percent < 5.0 and time >= '%s' and time < '%s' ", interval.StartString(), interval.EndString()))

func (d *TimescaleDBDevops) MultipleMemOrs(qi Query, hosts int) {
	interval := d.AllInterval.RandWindow(24 * time.Hour)

	sqlQuery := fmt.Sprintf(`SELECT * FROM mem WHERE (used_percent > 98.0 OR used > 10000 OR used_percent < 5.0) AND (time >= '%s' AND time < '%s')`, interval.Start.Format(goTimeFmt), interval.End.Format(goTimeFmt))

	humanLabel := "TimescaleDB mem fields with or, all hosts"
	q := qi.(*TimescaleDBQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.NamespaceName = []byte("mem")
	q.FieldName = []byte("*")
	q.SqlQuery = []byte(sqlQuery)
}

func (d *TimescaleDBDevops) MultipleMemOrsByHost(qi Query, hosts int) {
	interval := d.AllInterval.RandWindow(24 * time.Hour)

	sqlQuery := fmt.Sprintf(`SELECT date_trunc('hour', time) AS hour, MAX(used_percent) from mem where (used < 1000 OR used_percent > 98.0 OR used_percent < 10.0) and (time >= '%s' and time < '%s') GROUP BY hour,hostname`, interval.Start.Format(goTimeFmt), interval.End.Format(goTimeFmt))

	humanLabel := "TimescaleDB mem fields with or, all hosts"
	q := qi.(*TimescaleDBQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.NamespaceName = []byte("mem")
	q.FieldName = []byte("*")

	q.SqlQuery = []byte(sqlQuery)
}

// SELECT * where CPU > threshold OR battery < 5% OR free_memory < threshold and <some time period>
// "SELECT * from cpu,mem,disk where cpu > 90.0 and free < 10.0 and used_percent < 90.0 and time >= '%s' and time < '%s' GROUP BY 'host'", interval.StartString(), interval.EndString()))

// SELECT device_id, COUNT() where CPU > threshold OR battery < 5% OR free_memory < threshold and <some time period> GROUP BY device_id
// SELECT avg(cpu) where <some time period> GROUP BY customer_id, location_id
