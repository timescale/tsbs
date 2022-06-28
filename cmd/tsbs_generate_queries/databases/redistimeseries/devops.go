package redistimeseries

import (
	"fmt"
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
	oneMinuteMillis = 60 * 1000
	oneHourMillis   = oneMinuteMillis * 60
)

// Devops produces RedisTimeSeries-specific queries for all the devops query types.
type Devops struct {
	*BaseGenerator
	*devops.Core
}

// GenerateEmptyQuery returns an empty query.RedisTimeSeries
func (d *Devops) GenerateEmptyQuery() query.Query {
	return query.NewRedisTimeSeries()
}

// GroupByTime fetches the MAX for numMetrics metrics under 'cpu', per minute for nhosts hosts,
// every 5 mins for 1 hour
func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	redisQuery := [][]byte{
		//[]byte("TS.MRANGE"), Just to help understanding
		[]byte(fmt.Sprintf("%d", interval.StartUnixMillis())),
		[]byte(fmt.Sprintf("%d", interval.EndUnixMillis())),
		[]byte("AGGREGATION"),
		[]byte("MAX"),
		[]byte(fmt.Sprintf("%d", oneMinuteMillis)),
		[]byte("FILTER"),
		[]byte("measurement=cpu"),
	}

	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)

	// we only need to filter if we we dont want all of them
	if numMetrics != devops.GetCPUMetricsLen() {
		redisArg := "fieldname="
		if numMetrics > 1 {
			redisArg += "("
		}
		for idx, value := range metrics {
			redisArg += value
			if idx != (numMetrics - 1) {
				redisArg += ","
			}
		}
		if numMetrics > 1 {
			redisArg += ")"
		}
		redisQuery = append(redisQuery, []byte(redisArg))
	}

	hostnames, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)

	// add specific fieldname if needed.
	redisArg := "hostname="
	if nHosts > 1 {
		redisArg += "("
	}
	for idx, value := range hostnames {
		redisArg += value
		if idx != (nHosts - 1) {
			redisArg += ","
		}
	}
	if nHosts > 1 {
		redisArg += ")"
	}
	redisQuery = append(redisQuery, []byte(redisArg))

	if nHosts > 1 && numMetrics == 1 {
		redisQuery = append(redisQuery, []byte("GROUPBY"), []byte("hostname"), []byte("REDUCE"), []byte("max"))
	}
	if numMetrics > 1 {
		redisQuery = append(redisQuery, []byte("GROUPBY"), []byte("fieldname"), []byte("REDUCE"), []byte("max"))
	}

	humanLabel := devops.GetSingleGroupByLabel("RedisTimeSeries", numMetrics, nHosts, string(timeRange))
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQueryStrings(qi, humanLabel, humanDesc)
	d.AddQuery(qi, redisQuery, []byte("TS.MRANGE"))

}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day
func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)
	redisQuery := [][]byte{
		//[]byte("TS.MRANGE"), Just to help understanding
		[]byte(fmt.Sprintf("%d", interval.StartUnixMillis())),
		[]byte(fmt.Sprintf("%d", interval.EndUnixMillis())),
		[]byte("AGGREGATION"),
		[]byte("AVG"),
		[]byte(fmt.Sprintf("%d", oneHourMillis)),
		[]byte("FILTER"),
		[]byte("measurement=cpu"),
	}

	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)

	// add specific fieldname if needed.
	if numMetrics != devops.GetCPUMetricsLen() {
		redisArg := "fieldname="
		if numMetrics > 1 {
			redisArg += "("
		}
		for idx, value := range metrics {
			redisArg += value
			if idx != (numMetrics - 1) {
				redisArg += ","
			}
		}
		if numMetrics > 1 {
			redisArg += ")"
		}
		redisQuery = append(redisQuery, []byte(redisArg))
	}
	if numMetrics > 1 {
		redisQuery = append(redisQuery, []byte("GROUPBY"), []byte("hostname"), []byte("REDUCE"), []byte("max"))
	}

	humanLabel := devops.GetDoubleGroupByLabel("RedisTimeSeries", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQueryStrings(qi, humanLabel, humanDesc)
	d.AddQuery(qi, redisQuery, []byte("TS.MRANGE"))
	functorName := query.GetFunctionName(query.GroupByTimeAndTagHostname)
	d.SetApplyFunctor(qi, true, functorName)
}

// MaxAllCPU fetches the aggregate across all CPU metrics per hour over 1 hour for a single host.
// Currently only one host is supported
func (d *Devops) MaxAllCPU(qi query.Query, nHosts int, duration time.Duration) {
	interval := d.Interval.MustRandWindow(duration)
	hostnames, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)
	redisQuery := [][]byte{
		//[]byte("TS.MRANGE"), Just to help understanding
		[]byte(fmt.Sprintf("%d", interval.StartUnixMillis())),
		[]byte(fmt.Sprintf("%d", interval.EndUnixMillis())),
		[]byte("AGGREGATION"),
		[]byte("MAX"),
		[]byte(fmt.Sprintf("%d", oneHourMillis)),
		[]byte("FILTER"),
		[]byte("measurement=cpu"),
	}

	redisArg := "hostname="
	if nHosts > 1 {
		redisArg += "("
	}
	for idx, value := range hostnames {
		redisArg += value
		if idx != (nHosts - 1) {
			redisArg += ","
		}
	}
	if nHosts > 1 {
		redisArg += ")"
	}
	redisQuery = append(redisQuery, []byte(redisArg))
	if nHosts > 1 {
		redisQuery = append(redisQuery, []byte("GROUPBY"), []byte("fieldname"), []byte("REDUCE"), []byte("max"))
	}
	humanLabel := devops.GetMaxAllLabel("RedisTimeSeries", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQueryStrings(qi, humanLabel, humanDesc)
	d.AddQuery(qi, redisQuery, []byte("TS.MRANGE"))
}

// LastPointPerHost finds the last row for every host in the dataset
func (d *Devops) LastPointPerHost(qi query.Query) {
	redisQuery := [][]byte{
		[]byte("SELECTED_LABELS"),
		[]byte("hostname"),
		[]byte("fieldname"),
		[]byte("FILTER"),
		[]byte("measurement=cpu"),
		[]byte("hostname!="),
	}

	humanLabel := "RedisTimeSeries last row per host"
	humanDesc := fmt.Sprintf("%s", humanLabel)
	d.fillInQueryStrings(qi, humanLabel, humanDesc)
	d.AddQuery(qi, redisQuery, []byte("TS.MGET"))
}

// HighCPUForHosts populates a query that gets CPU metrics when the CPU has high
// usage between a time period for a number of hosts (if 0, it will search all hosts),
// e.g. in pseudo-SQL:
//
// SELECT * FROM cpu
// WHERE usage_user > 90.0
// AND time >= '$TIME_START' AND time < '$TIME_END'
// AND (hostname = '$HOST' OR hostname = '$HOST2'...)
//
// Resultsets:
// high-cpu-1
// high-cpu-all
func (d *Devops) HighCPUForHosts(qi query.Query, nHosts int) {
	hostnames, err := d.GetRandomHosts(nHosts)
	interval := d.Interval.MustRandWindow(devops.HighCPUDuration)
	redisQuery := [][]byte{
		//[]byte("TS.MRANGE"), Just to help understanding
		[]byte(fmt.Sprintf("%d", interval.StartUnixMillis())),
		[]byte(fmt.Sprintf("%d", interval.EndUnixMillis())),
		[]byte("FILTER_BY_VALUE"), []byte("90.0"), []byte("1000"),
		[]byte("FILTER"),
		[]byte("fieldname=usage_user"),
		[]byte("measurement=cpu"),
	}
	if nHosts > 0 {
		redisArg := "hostname="
		if nHosts > 1 {
			redisArg += "("
		}
		for idx, value := range hostnames {
			redisArg += value
			if idx != (nHosts - 1) {
				redisArg += ","
			}
		}
		if nHosts > 1 {
			redisArg += ")"
		}
		redisQuery = append(redisQuery, []byte(redisArg))
	}
	redisQuery = append(redisQuery, []byte("GROUPBY"), []byte("fieldname"), []byte("REDUCE"), []byte("max"))

	humanLabel, err := devops.GetHighCPULabel("RedisTimeSeries", nHosts)
	panicIfErr(err)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQueryStrings(qi, humanLabel, humanDesc)
	d.AddQuery(qi, redisQuery, []byte("TS.MRANGE"))
	d.SetApplyFunctor(qi, true, "FILTER_BY_TS")
}

// GroupByOrderByLimit populates a query.Query that has a time WHERE clause, that groups by a truncated date, orders by that date, and takes a limit:
func (d *Devops) GroupByOrderByLimit(qi query.Query) {

	interval := d.Interval.MustRandWindow(time.Hour)
	redisQuery := [][]byte{
		//[]byte("TS.MREVRANGE"), Just to help understanding
		[]byte("-"),
		[]byte(fmt.Sprintf("%d", interval.EndUnixMillis())),
		[]byte("COUNT"),
		[]byte("5"),
		[]byte("AGGREGATION"),
		[]byte("MAX"),
		[]byte(fmt.Sprintf("%d", oneMinuteMillis)),
		[]byte("FILTER"),
		[]byte("measurement=cpu"),
		[]byte("fieldname=usage_user"),
	}

	humanLabel := devops.GetGroupByOrderByLimitLabel("RedisTimeSeries")
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.EndString())

	d.fillInQueryStrings(qi, humanLabel, humanDesc)
	d.AddQuery(qi, redisQuery, []byte("TS.MREVRANGE"))

}
