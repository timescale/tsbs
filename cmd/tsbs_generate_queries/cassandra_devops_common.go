package main

import (
	"fmt"
	"strings"
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// CassandraDevops produces Cassandra-specific queries for all the devops query types.
type CassandraDevops struct {
	*devopsCore
}

// NewCassandraDevops makes an CassandraDevops object ready to generate Queries.
func newCassandraDevopsCommon(start, end time.Time, scale int) *CassandraDevops {
	return &CassandraDevops{newDevopsCore(start, end, scale)}
}

func (d *CassandraDevops) getHostWhere(nhosts int) []string {
	hostnames := d.getRandomHosts(nhosts)

	tagSet := []string{}
	for _, hostname := range hostnames {
		tag := fmt.Sprintf("hostname=%s", hostname)
		tagSet = append(tagSet, tag)
	}

	return tagSet
}

// MaxCPUMetricsByMinute selects the MAX for numMetrics metrics under 'cpu',
// per minute for nhosts hosts,
// e.g. in psuedo-SQL:
//
// SELECT minute, max(metric1), ..., max(metricN)
// FROM cpu
// WHERE (hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY minute ORDER BY minute ASC
func (d *CassandraDevops) MaxCPUMetricsByMinute(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.interval.RandWindow(timeRange)
	metrics := getCPUMetricsSlice(numMetrics)
	tagSet := d.getHostWhere(nHosts)

	tagSets := [][]string{}
	tagSets = append(tagSets, tagSet)

	humanLabel := fmt.Sprintf("Cassandra %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	q := qi.(*query.Cassandra)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))

	q.AggregationType = []byte("max")
	q.MeasurementName = []byte("cpu")
	q.FieldName = []byte(strings.Join(metrics, ","))

	q.TimeStart = interval.Start
	q.TimeEnd = interval.End
	q.GroupByDuration = time.Minute

	q.TagSets = tagSets
}

// GroupByOrderByLimit populates a query.Query that has a time WHERE clause, that groups by a truncated date, orders by that date, and takes a limit:
// SELECT date_trunc('minute', time) AS t, MAX(cpu) FROM cpu
// WHERE time < '$TIME'
// GROUP BY t ORDER BY t DESC
// LIMIT $LIMIT
func (d *CassandraDevops) GroupByOrderByLimit(qi query.Query) {
	interval := d.interval.RandWindow(time.Hour)

	humanLabel := "Cassandra max cpu over last 5 min-intervals (rand end)"
	q := qi.(*query.Cassandra)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, d.interval.StartString()))

	q.AggregationType = []byte("max")
	q.MeasurementName = []byte("cpu")
	q.FieldName = []byte("usage_user")

	q.TimeStart = d.interval.Start
	q.TimeEnd = interval.End
	q.GroupByDuration = time.Minute
	q.OrderBy = []byte("timestamp_ns DESC")
	q.Limit = 5
}

// MeanCPUMetricsDayByHourAllHostsGroupbyHost selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day,
// e.g. in psuedo-SQL:
//
// SELECT AVG(metric1), ..., AVG(metricN)
// FROM cpu
// WHERE time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour, hostname ORDER BY hour
func (d *CassandraDevops) MeanCPUMetricsDayByHourAllHostsGroupbyHost(qi query.Query, numMetrics int) {
	interval := d.interval.RandWindow(24 * time.Hour)
	metrics := cpuMetrics[:numMetrics]

	humanLabel := fmt.Sprintf("Cassandra mean of %d metrics, all hosts, rand 1day by 1hr", numMetrics)
	q := qi.(*query.Cassandra)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))

	q.AggregationType = []byte("avg")
	q.MeasurementName = []byte("cpu")
	q.FieldName = []byte(strings.Join(metrics, ","))

	q.TimeStart = interval.Start
	q.TimeEnd = interval.End
	q.GroupByDuration = time.Hour
}

// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for nhosts hosts,
// e.g. in psuedo-SQL:
//
// SELECT MAX(metric1), ..., MAX(metricN)
// FROM cpu WHERE (hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour ORDER BY hour
func (d *CassandraDevops) MaxAllCPU(qi query.Query, nhosts int) {
	interval := d.interval.RandWindow(8 * time.Hour)
	tagSet := d.getHostWhere(nhosts)

	tagSets := [][]string{}
	tagSets = append(tagSets, tagSet)

	humanLabel := fmt.Sprintf("Cassandra max cpu all fields, rand %4d hosts, rand 12hr by 1h", nhosts)
	q := qi.(*query.Cassandra)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))

	q.AggregationType = []byte("max")
	q.MeasurementName = []byte("cpu")
	q.FieldName = []byte("usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice")

	q.TimeStart = interval.Start
	q.TimeEnd = interval.End
	q.GroupByDuration = time.Hour

	q.TagSets = tagSets
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *CassandraDevops) LastPointPerHost(qi query.Query) {
	humanLabel := "Cassandra last row per host"
	q := qi.(*query.Cassandra)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, d.interval.StartString()))

	q.MeasurementName = []byte("cpu")
	q.FieldName = []byte("usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice")

	q.TimeStart = d.interval.Start
	q.TimeEnd = d.interval.End

	q.ForEveryN = []byte("hostname,1")
}

// HighCPUForHosts populates a query that gets CPU metrics when the CPU has high
// usage between a time period for a number of hosts (if 0, it will search all hosts),
// e.g. in psuedo-SQL:
//
// SELECT * FROM cpu
// WHERE usage_user > 90.0
// AND time >= '$TIME_START' AND time < '$TIME_END'
// AND (hostname = '$HOST' OR hostname = '$HOST2'...)
func (d *CassandraDevops) HighCPUForHosts(qi query.Query, nhosts int) {
	interval := d.interval.RandWindow(24 * time.Hour)
	tagSet := d.getHostWhere(nhosts)

	tagSets := [][]string{}
	if len(tagSet) > 0 {
		tagSets = append(tagSets, tagSet)
	}

	humanLabel := "Cassandra CPU over threshold, "
	if len(tagSet) > 0 {
		humanLabel += "one host"
	} else {
		humanLabel += "all hosts"
	}

	q := qi.(*query.Cassandra)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))

	q.AggregationType = []byte("")
	q.MeasurementName = []byte("cpu")
	q.FieldName = []byte("usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice")

	q.TimeStart = interval.Start
	q.TimeEnd = interval.End
	q.GroupByDuration = time.Hour
	q.WhereClause = []byte("usage_user,>,90.0")

	q.TagSets = tagSets
}
