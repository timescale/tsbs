package main

import (
	"fmt"
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
func (d *InfluxDevops) Dispatch(i int, q *Query) {
	DevopsDispatch(d, i, q)
}

// AvgCPUUsageDayByHour populates a Query with a query that looks like:
// SELECT mean(usage_user) from cpu where time >= '$DAY_START' and time < '$DAY_END' group by time(1h)
func (d *InfluxDevops) AvgCPUUsageDayByHour(q *Query) {
	interval := d.AllInterval.RandWindow(24 * time.Hour)

	v := url.Values{}
	v.Set("db", d.DatabaseName)
	v.Set("q", fmt.Sprintf("SELECT mean(usage_user) from cpu where time >= '%s' and time < '%s' group by time(1h)", interval.StartString(), interval.EndString()))

	q.HumanLabel = []byte("Influx CPU day   by 1h")
	q.HumanDescription = []byte(fmt.Sprintf("Influx CPU day   by 1h: %s", interval.StartString()))
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

	q.HumanLabel = []byte("Influx CPU week  by 1h")
	q.HumanDescription = []byte(fmt.Sprintf("Influx CPU week  by 1h: %s", interval.StartString()))
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

	q.HumanLabel = []byte("Influx CPU month by 1d")
	q.HumanDescription = []byte(fmt.Sprintf("Influx CPU month by 1d: %s", interval.StartString()))
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

	q.HumanLabel = []byte("Influx mem day   by 1h")
	q.HumanDescription = []byte(fmt.Sprintf("Influx mem day   by 1h: %s", interval.StartString()))
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

	q.HumanLabel = []byte("Influx mem week  by 1h")
	q.HumanDescription = []byte(fmt.Sprintf("Influx mem week  by 1h: %s", interval.StartString()))
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

	q.HumanLabel = []byte("Influx mem month by 1d")
	q.HumanDescription = []byte(fmt.Sprintf("Influx mem month by 1d: %s", interval.StartString()))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}
