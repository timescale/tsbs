package main

import (
	"fmt"
	"math/rand"
	"net/url"
	"time"
)

// InfluxDevops produces Influx-specific queries for the devops use case.
type InfluxDevops struct {
	DatabaseName string
	AllInterval  TimeInterval
}

// NewInfluxDevops makes an InfluxDevops object ready to generate Queries.
func NewInfluxDevops(databaseName string, start, end time.Time) *InfluxDevops {
	if !start.Before(end) {
		panic("bad time order")
	}
	return &InfluxDevops{
		DatabaseName: databaseName,
		AllInterval:  NewTimeInterval(start, end),
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (d *InfluxDevops) Dispatch(i int, q *Query, scaleVar int) {
	DevopsDispatch(d, i, q, scaleVar)
}

// AvgCPUUsageDayByHour populates a Query with a query that looks like:
// SELECT mean(usage_user) from cpu where time >= '$DAY_START' and time < '$DAY_END' group by time(1h)
func (d *InfluxDevops) AvgCPUUsageDayByHour(q *Query) {
	interval := d.AllInterval.RandWindow(24 * time.Hour)

	v := url.Values{}
	v.Set("db", d.DatabaseName)
	v.Set("q", fmt.Sprintf("SELECT mean(usage_user) from cpu where time >= '%s' and time < '%s' group by time(1h)", interval.StartString(), interval.EndString()))

	humanLabel := []byte("Influx avg cpu, all hosts, rand 1d by 1h")
	q.HumanLabel = humanLabel
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

// AvgCPUUsageWeekByHour populates a Query with a query that looks like:
// SELECT mean(usage_user) from cpu where time >= '$WEEK_START' and time < '$WEEK_END' group by time(1h)
func (d *InfluxDevops) AvgCPUUsageWeekByHour(q *Query) {
	interval := d.AllInterval.RandWindow(7 * 24 * time.Hour)

	v := url.Values{}
	v.Set("db", d.DatabaseName)
	v.Set("q", fmt.Sprintf("SELECT mean(usage_user) from cpu where time >= '%s' and time < '%s' group by time(1h)", interval.StartString(), interval.EndString()))

	humanLabel := []byte("Influx avg cpu, all hosts, rand 7d by 1h")
	q.HumanLabel = humanLabel
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

// AvgCPUUsageMonthByDay populates a Query with a query that looks like:
// SELECT mean(usage_user) from cpu where time >= '$MONTH_START' and time < '$MONTH_END' group by time(1d)
func (d *InfluxDevops) AvgCPUUsageMonthByDay(q *Query) {
	interval := d.AllInterval.RandWindow(28 * 24 * time.Hour)

	v := url.Values{}
	v.Set("db", d.DatabaseName)
	v.Set("q", fmt.Sprintf("SELECT mean(usage_user) from cpu where time >= '%s' and time < '%s' group by time(1d)", interval.StartString(), interval.EndString()))

	humanLabel := []byte("Influx avg cpu, all hosts, rand 28d by 1d")
	q.HumanLabel = humanLabel
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

// AvgMemAvailableDayByHour populates a Query with a query that looks like:
// SELECT mean(available) from mem where time >= '$DAY_START' and time < '$DAY_END' group by time(1h)
func (d *InfluxDevops) AvgMemAvailableDayByHour(q *Query) {
	interval := d.AllInterval.RandWindow(24 * time.Hour)

	v := url.Values{}
	v.Set("db", d.DatabaseName)
	v.Set("q", fmt.Sprintf("SELECT mean(available) from mem where time >= '%s' and time < '%s' group by time(1h)", interval.StartString(), interval.EndString()))

	humanLabel := []byte("Influx avg mem, all hosts, rand 1d by 1h")
	q.HumanLabel = humanLabel
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

// AvgMemAvailableWeekByHour populates a Query with a query that looks like:
// SELECT mean(available) from mem where time >= '$WEEK_START' and time < '$WEEK_END' group by time(1h)
func (d *InfluxDevops) AvgMemAvailableWeekByHour(q *Query) {
	interval := d.AllInterval.RandWindow(7 * 24 * time.Hour)

	v := url.Values{}
	v.Set("db", d.DatabaseName)
	v.Set("q", fmt.Sprintf("SELECT mean(available) from mem where time >= '%s' and time < '%s' group by time(1h)", interval.StartString(), interval.EndString()))

	humanLabel := []byte("Influx avg mem, all hosts, rand 7d by 1h")
	q.HumanLabel = humanLabel
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

// AvgMemAvailableMonthByDay populates a Query with a query that looks like:
// SELECT mean(available) from mem where time >= '$MONTH_START' and time < '$MONTH_END' group by time(1d)
func (d *InfluxDevops) AvgMemAvailableMonthByDay(q *Query) {
	interval := d.AllInterval.RandWindow(28 * 24 * time.Hour)

	v := url.Values{}
	v.Set("db", d.DatabaseName)
	v.Set("q", fmt.Sprintf("SELECT mean(available) from mem where time >= '%s' and time < '%s' group by time(1d)", interval.StartString(), interval.EndString()))

	humanLabel := []byte("Influx avg mem, all hosts, rand 28d by 1d")
	q.HumanLabel = humanLabel
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

// MaxCPUUsageHourByMinuteOneHost populates a Query with a query that looks like:
// SELECT max(usage_user) from cpu where hostname = '$HOSTNAME' and time >= '$HOUR_START' and time < '$HOUR_END' group by time(1m)
func (d *InfluxDevops) MaxCPUUsageHourByMinuteOneHost(q *Query, scaleVar int) {
	interval := d.AllInterval.RandWindow(time.Hour)
	hostname := fmt.Sprintf("host_%d", rand.Intn(scaleVar))

	v := url.Values{}
	v.Set("db", d.DatabaseName)
	v.Set("q", fmt.Sprintf("SELECT max(usage_user) from cpu where hostname = '%s' and time >= '%s' and time < '%s' group by time(1m)", hostname, interval.StartString(), interval.EndString()))

	humanLabel := []byte("Influx max cpu, rand 1 host, rand 1hr by 1m")
	q.HumanLabel = humanLabel
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}
