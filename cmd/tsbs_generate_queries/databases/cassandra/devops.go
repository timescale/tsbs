package cassandra

import (
	"fmt"
	"strings"
	"time"

	"bitbucket.org/440-labs/influxdb-comparisons/cmd/tsbs_generate_queries/uses/devops"
	"bitbucket.org/440-labs/influxdb-comparisons/query"
)

// Devops produces Cassandra-specific queries for all the devops query types.
type Devops struct {
	*devops.Core
}

// NewDevops makes an Devops object ready to generate Queries.
func NewDevops(start, end time.Time, scale int) *Devops {
	return &Devops{devops.NewCore(start, end, scale)}
}

// GenerateEmptyQuery returns an empty query.Cassandra
func (d *Devops) GenerateEmptyQuery() query.Query {
	return query.NewCassandra()
}

func (d *Devops) getHostWhere(nHosts int) []string {
	hostnames := d.GetRandomHosts(nHosts)

	tagSet := []string{}
	for _, hostname := range hostnames {
		tag := fmt.Sprintf("hostname=%s", hostname)
		tagSet = append(tagSet, tag)
	}

	return tagSet
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
func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.RandWindow(timeRange)
	metrics := devops.GetCPUMetricsSlice(numMetrics)
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
func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	interval := d.Interval.RandWindow(time.Hour)

	humanLabel := "Cassandra max cpu over last 5 min-intervals (random end)"
	q := qi.(*query.Cassandra)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, d.Interval.StartString()))

	q.AggregationType = []byte("max")
	q.MeasurementName = []byte("cpu")
	q.FieldName = []byte("usage_user")

	q.TimeStart = d.Interval.Start
	q.TimeEnd = interval.End
	q.GroupByDuration = time.Minute
	q.OrderBy = []byte("timestamp_ns DESC")
	q.Limit = 5
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day,
// e.g. in psuedo-SQL:
//
// SELECT AVG(metric1), ..., AVG(metricN)
// FROM cpu
// WHERE time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour, hostname ORDER BY hour
func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	interval := d.Interval.RandWindow(devops.DoubleGroupByDuration)
	metrics := devops.GetCPUMetricsSlice(numMetrics)

	humanLabel := devops.GetDoubleGroupByLabel("Cassandra", numMetrics)
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
func (d *Devops) MaxAllCPU(qi query.Query, nHosts int) {
	interval := d.Interval.RandWindow(devops.MaxAllDuration)
	tagSet := d.getHostWhere(nHosts)

	tagSets := [][]string{}
	tagSets = append(tagSets, tagSet)

	humanLabel := devops.GetMaxAllLabel("Cassandra", nHosts)
	q := qi.(*query.Cassandra)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))

	q.AggregationType = []byte("max")
	q.MeasurementName = []byte("cpu")
	q.FieldName = []byte(strings.Join(devops.GetAllCPUMetrics(), ","))

	q.TimeStart = interval.Start
	q.TimeEnd = interval.End
	q.GroupByDuration = time.Hour

	q.TagSets = tagSets
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *Devops) LastPointPerHost(qi query.Query) {
	humanLabel := "Cassandra last row per host"
	q := qi.(*query.Cassandra)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, d.Interval.StartString()))

	q.MeasurementName = []byte("cpu")
	q.FieldName = []byte(strings.Join(devops.GetAllCPUMetrics(), ","))

	q.TimeStart = d.Interval.Start
	q.TimeEnd = d.Interval.End

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
func (d *Devops) HighCPUForHosts(qi query.Query, nHosts int) {
	interval := d.Interval.RandWindow(devops.HighCPUDuration)
	tagSet := d.getHostWhere(nHosts)

	tagSets := [][]string{}
	if len(tagSet) > 0 {
		tagSets = append(tagSets, tagSet)
	}

	humanLabel := devops.GetHighCPULabel("Cassandra", nHosts)
	q := qi.(*query.Cassandra)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))

	q.AggregationType = []byte("")
	q.MeasurementName = []byte("cpu")
	q.FieldName = []byte(strings.Join(devops.GetAllCPUMetrics(), ","))

	q.TimeStart = interval.Start
	q.TimeEnd = interval.End
	q.GroupByDuration = time.Hour
	q.WhereClause = []byte("usage_user,>,90.0")

	q.TagSets = tagSets
}
