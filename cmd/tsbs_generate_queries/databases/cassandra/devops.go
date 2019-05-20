package cassandra

import (
	"fmt"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/query"
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

func (d *Devops) getHostWhereWithHostnames(hostnames []string) []string {
	tagSet := []string{}
	for _, hostname := range hostnames {
		tag := "hostname=" + hostname
		tagSet = append(tagSet, tag)
	}

	return tagSet
}

func (d *Devops) getHostWhere(nHosts int) []string {
	hostnames := d.GetRandomHosts(nHosts)
	return d.getHostWhereWithHostnames(hostnames)
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

	metrics := devops.GetCPUMetricsSlice(numMetrics)
	tagSet := d.getHostWhere(nHosts)

	tagSets := [][]string{}
	tagSets = append(tagSets, tagSet)

	humanLabel := fmt.Sprintf("Cassandra %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, "max", metrics, interval, tagSets)
	q := qi.(*query.Cassandra)
	q.GroupByDuration = time.Minute
}

// GroupByOrderByLimit populates a query.Query that has a time WHERE clause, that groups by a truncated date, orders by that date, and takes a limit:
// SELECT date_trunc('minute', time) AS t, MAX(cpu) FROM cpu
// WHERE time < '$TIME'
// GROUP BY t ORDER BY t DESC
// LIMIT $LIMIT
func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour)

	interval, err := utils.NewTimeInterval(d.Interval.Start(), interval.End())
	if err != nil {
		panic(err.Error())
	}

	humanLabel := "Cassandra max cpu over last 5 min-intervals (random end)"
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, d.Interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, "max", []string{"usage_user"}, interval, nil)
	q := qi.(*query.Cassandra)
	q.GroupByDuration = time.Minute
	q.OrderBy = []byte("timestamp_ns DESC")
	q.Limit = 5
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day,
// e.g. in pseudo-SQL:
//
// SELECT AVG(metric1), ..., AVG(metricN)
// FROM cpu
// WHERE time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour, hostname ORDER BY hour
func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)

	metrics := devops.GetCPUMetricsSlice(numMetrics)

	humanLabel := devops.GetDoubleGroupByLabel("Cassandra", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, "avg", metrics, interval, nil)
	q := qi.(*query.Cassandra)
	q.GroupByDuration = time.Hour
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

	tagSet := d.getHostWhere(nHosts)

	tagSets := [][]string{}
	tagSets = append(tagSets, tagSet)

	humanLabel := devops.GetMaxAllLabel("Cassandra", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, "max", devops.GetAllCPUMetrics(), interval, tagSets)
	q := qi.(*query.Cassandra)
	q.GroupByDuration = time.Hour
	q.TagSets = tagSets
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *Devops) LastPointPerHost(qi query.Query) {
	humanLabel := "Cassandra last row per host"
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, d.Interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, "", devops.GetAllCPUMetrics(), d.Interval, nil)
	q := qi.(*query.Cassandra)
	q.ForEveryN = []byte("hostname,1")
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

	tagSet := d.getHostWhere(nHosts)

	tagSets := [][]string{}
	if len(tagSet) > 0 {
		tagSets = append(tagSets, tagSet)
	}

	humanLabel := devops.GetHighCPULabel("Cassandra", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, "", devops.GetAllCPUMetrics(), interval, tagSets)
	q := qi.(*query.Cassandra)
	q.GroupByDuration = time.Hour
	q.WhereClause = []byte("usage_user,>,90.0")
}

func (d *Devops) fillInQuery(qi query.Query, humanLabel, humanDesc, aggType string, fields []string, interval *utils.TimeInterval, tagSets [][]string) {
	q := qi.(*query.Cassandra)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)

	q.AggregationType = []byte(aggType)
	q.MeasurementName = []byte("cpu")
	q.FieldName = []byte(strings.Join(fields, ","))

	q.TimeStart = interval.Start()
	q.TimeEnd = interval.End()

	q.TagSets = tagSets
}
