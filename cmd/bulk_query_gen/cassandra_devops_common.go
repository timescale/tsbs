package main

import (
	"fmt"
	"math/rand"
	"time"
)

// CassandraDevops produces Cassandra-specific queries for all the devops query types.
type CassandraDevops struct {
	KeyspaceName string
	AllInterval  TimeInterval
}

// NewCassandraDevops makes an CassandraDevops object ready to generate Queries.
func newCassandraDevopsCommon(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	if !start.Before(end) {
		panic("bad time order")
	}

	return &CassandraDevops{
		KeyspaceName: dbConfig["database-name"],
		AllInterval:  NewTimeInterval(start, end),
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (d *CassandraDevops) Dispatch(i, scaleVar int) Query {
	q := NewCassandraQuery() // from pool
	devopsDispatchAll(d, i, q, scaleVar)
	return q
}

func (d *CassandraDevops) MaxCPUUsageHourByMinuteOneHost(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*CassandraQuery), scaleVar, 1, time.Hour)
}

func (d *CassandraDevops) MaxCPUUsageHourByMinuteTwoHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*CassandraQuery), scaleVar, 2, time.Hour)
}

func (d *CassandraDevops) MaxCPUUsageHourByMinuteFourHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*CassandraQuery), scaleVar, 4, time.Hour)
}

func (d *CassandraDevops) MaxCPUUsageHourByMinuteEightHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*CassandraQuery), scaleVar, 8, time.Hour)
}

func (d *CassandraDevops) MaxCPUUsageHourByMinuteSixteenHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*CassandraQuery), scaleVar, 16, time.Hour)
}

func (d *CassandraDevops) MaxCPUUsageHourByMinuteThirtyTwoHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*CassandraQuery), scaleVar, 32, time.Hour)
}

func (d *CassandraDevops) MaxCPUUsage12HoursByMinuteOneHost(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*CassandraQuery), scaleVar, 1, 12*time.Hour)
}

// MaxCPUUsageHourByMinuteThirtyTwoHosts populates a Query with a query that looks like:
// SELECT max(usage_user) from cpu where (hostname = '$HOSTNAME_1' or ... or hostname = '$HOSTNAME_N') and time >= '$HOUR_START' and time < '$HOUR_END' group by time(1m)
func (d *CassandraDevops) maxCPUUsageHourByMinuteNHosts(qi Query, scaleVar, nhosts int, timeRange time.Duration) {
	interval := d.AllInterval.RandWindow(timeRange)
	nn := rand.Perm(scaleVar)[:nhosts]

	tagSets := [][]string{}
	tagSet := []string{}
	for _, n := range nn {
		hostname := fmt.Sprintf("host_%d", n)
		tag := fmt.Sprintf("hostname=%s", hostname)
		tagSet = append(tagSet, tag)
	}
	tagSets = append(tagSets, tagSet)

	humanLabel := fmt.Sprintf("Cassandra max cpu, rand %4d hosts, rand %s by 1m", nhosts, timeRange)
	q := qi.(*CassandraQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))

	q.AggregationType = []byte("max")
	q.MeasurementName = []byte("cpu")
	q.FieldName = []byte("usage_user")

	q.TimeStart = interval.Start
	q.TimeEnd = interval.End
	q.GroupByDuration = time.Minute

	q.TagSets = tagSets
}

// CPU5MetricsHourByMinuteOneHost populates a Query for 5 CPU metrics per minute over 1 hour on 1 host
func (d *CassandraDevops) CPU5MetricsHourByMinuteOneHost(q Query, scaleVar int) {
	d.cpu5MetricsHourByMinuteNHosts(q, scaleVar, 1, time.Hour)
}

// CPU5Metrics12HoursByMinuteOneHost populates a Query for 5 CPU metrics per minute over 12 hours on 1 host
func (d *CassandraDevops) CPU5Metrics12HoursByMinuteOneHost(q Query, scaleVar int) {
	d.cpu5MetricsHourByMinuteNHosts(q, scaleVar, 1, 12*time.Hour)
}

// CPU5MetricsHourByMinuteEightHosts populates a Query for 5 CPU metrics per minute over 1 hour on 8 hosts
func (d *CassandraDevops) CPU5MetricsHourByMinuteEightHosts(q Query, scaleVar int) {
	d.cpu5MetricsHourByMinuteNHosts(q, scaleVar, 8, time.Hour)
}

// SELECT minute, metric1, metric2, metric3, metric4, metric5
// FROM cpu
// WHERE (hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY minute ORDER BY minute ASC
func (d *CassandraDevops) cpu5MetricsHourByMinuteNHosts(qi Query, scaleVar, nhosts int, timeRange time.Duration) {
	interval := d.AllInterval.RandWindow(timeRange)
	nn := rand.Perm(scaleVar)[:nhosts]

	tagSets := [][]string{}
	tagSet := []string{}
	for _, n := range nn {
		hostname := fmt.Sprintf("host_%d", n)
		tag := fmt.Sprintf("hostname=%s", hostname)
		tagSet = append(tagSet, tag)
	}
	tagSets = append(tagSets, tagSet)

	humanLabel := fmt.Sprintf("Cassandra 5 cpu metrics, rand %4d hosts, rand %s by 1m", nhosts, timeRange)
	q := qi.(*CassandraQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))

	q.AggregationType = []byte("max")
	q.MeasurementName = []byte("cpu")
	q.FieldName = []byte("usage_user,usage_system,usage_idle,usage_nice,usage_guest")

	q.TimeStart = interval.Start
	q.TimeEnd = interval.End
	q.GroupByDuration = time.Minute

	q.TagSets = tagSets
}

// MeanCPUUsageDayByHourAllHosts populates a Query with a query that looks like:
// SELECT mean(usage_user) from cpu where time >= '$DAY_START' and time < '$DAY_END' group by time(1h),hostname
func (d *CassandraDevops) MeanCPUUsageDayByHourAllHostsGroupbyHost(qi Query, _ int) {
	interval := d.AllInterval.RandWindow(24 * time.Hour)

	humanLabel := "Cassandra mean cpu, all hosts, rand 1day by 1hour"
	q := qi.(*CassandraQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))

	q.AggregationType = []byte("avg")
	q.MeasurementName = []byte("cpu")
	q.FieldName = []byte("usage_user")

	q.TimeStart = interval.Start
	q.TimeEnd = interval.End
	q.GroupByDuration = time.Hour
}

// MaxAllCPUOneHost populates a Query to get the max of all CPU metrics per hour over 12 hours on 1 host
func (d *CassandraDevops) MaxAllCPUOneHost(q Query, scaleVar int) {
	d.maxAllCPUHostsN(q, scaleVar, 1)
}

// MaxAllCPUEightHosts populates a Query to get the max of all CPU metrics per hour over 12 hours on 8 hosts
func (d *CassandraDevops) MaxAllCPUEightHosts(q Query, scaleVar int) {
	d.maxAllCPUHostsN(q, scaleVar, 8)
}

// SELECT MAX(metric1), ..., MAX(metricN)
// FROM cpu WHERE (hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// AND time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour ORDER BY hour
func (d *CassandraDevops) maxAllCPUHostsN(qi Query, scaleVar, nhosts int) {
	interval := d.AllInterval.RandWindow(12 * time.Hour)
	nn := rand.Perm(scaleVar)[:nhosts]

	tagSets := [][]string{}
	tagSet := []string{}
	for _, n := range nn {
		hostname := fmt.Sprintf("host_%d", n)
		tag := fmt.Sprintf("hostname=%s", hostname)
		tagSet = append(tagSet, tag)
	}
	tagSets = append(tagSets, tagSet)

	humanLabel := fmt.Sprintf("Cassandra max cpu all fields, rand %4d hosts, rand 12hr by 1h", nhosts)
	q := qi.(*CassandraQuery)
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

// HighCPU populates a query that gets CPU metrics when the CPU has high
// usage between a time period across all hosts
// i.e. SELECT * FROM cpu WHERE usage_user > 90.0 AND time >= '$TIME_START' AND time < '$TIME_END'
func (d *CassandraDevops) HighCPU(qi Query, _ int) {
	d.highCPUForHost(qi, []string{})
}

// HighCPUAndField populates a query that gets CPU metrics when the CPU has high
// usage between a time period for a particular host
// i.e. SELECT * FROM cpu WHERE usage_user > 90.0 AND time >= '$TIME_START' AND time < '$TIME_END' AND hostname = '$HOST'
func (d *CassandraDevops) HighCPUAndField(qi Query, scaleVar int) {
	nn := rand.Perm(scaleVar)[:1]
	tagSet := []string{}
	for _, n := range nn {
		hostname := fmt.Sprintf("host_%d", n)
		tag := fmt.Sprintf("hostname=%s", hostname)
		tagSet = append(tagSet, tag)
	}
	d.highCPUForHost(qi, tagSet)
}

func (d *CassandraDevops) highCPUForHost(qi Query, tagSet []string) {
	interval := d.AllInterval.RandWindow(24 * time.Hour)

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

	q := qi.(*CassandraQuery)
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

//func (d *CassandraDevops) MeanCPUUsageDayByHourAllHostsGroupbyHost(qi Query, _ int) {
//	interval := d.AllInterval.RandWindow(24*time.Hour)
//
//	v := url.Values{}
//	v.Set("db", d.KeyspaceName)
//	v.Set("q", fmt.Sprintf("SELECT count(usage_user) from cpu where time >= '%s' and time < '%s' group by time(1h)", interval.StartString(), interval.EndString()))
//
//	humanLabel := "Cassandra mean cpu, all hosts, rand 1day by 1hour"
//	q := qi.(*CassandraQuery)
//	q.HumanLabel = []byte(humanLabel)
//	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
//	q.Method = []byte("GET")
//	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
//	q.Body = nil
//}
