package main

import (
	"fmt"
	"net/url"
	"time"
)

// ElasticSearchDevops produces ES-specific queries for the devops use case.
type ElasticSearchDevops struct {
	DayIntervals   TimeIntervals
	WeekIntervals  TimeIntervals
	MonthIntervals TimeIntervals
}

// NewElasticSearchDevops makes an ElasticSearchDevops object ready to generate Queries.
func NewElasticSearchDevops(start, end time.Time) *ElasticSearchDevops {
	if !start.Before(end) {
		panic("bad time order")
	}
	return &ElasticSearchDevops{
		DayIntervals:   NewTimeIntervals(start, end, 24*time.Hour),
		WeekIntervals:  NewTimeIntervals(start, end, 7*24*time.Hour),
		MonthIntervals: NewTimeIntervals(start, end, 31*24*time.Hour),
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (d *ElasticSearchDevops) Dispatch(i int, q *Query) {
	DevopsDispatch(d, i, q)
}

// AvgCPUUsageDayByHour populates a Query with a query that looks like:
// SELECT mean(usage_user) from cpu where time >= '$DAY_START' and time < '$DAY_END' group by time(1h)
func (d *ElasticSearchDevops) AvgCPUUsageDayByHour(q *Query) {
	interval := d.DayIntervals.RandChoice()

	v := url.Values{}
	v.Set("q", fmt.Sprintf("SELECT mean(usage_user) from cpu where time >= '%s' and time < '%s' group by time(1h)", interval.StartString(), interval.EndString()))

	q.HumanLabel = []byte("CPU day   by 1h")
	q.HumanDescription = []byte(fmt.Sprintf("CPU day   by 1h: %s", interval.StartString()))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

// AvgCPUUsageWeekByHour populates a Query with a query that looks like:
// SELECT mean(usage_user) from cpu where time >= '$WEEK_START' and time < '$WEEK_END' group by time(1h)
func (d *ElasticSearchDevops) AvgCPUUsageWeekByHour(q *Query) {
	interval := d.WeekIntervals.RandChoice()

	v := url.Values{}
	v.Set("q", fmt.Sprintf("SELECT mean(usage_user) from cpu where time >= '%s' and time < '%s' group by time(1h)", interval.StartString(), interval.EndString()))

	q.HumanLabel = []byte("CPU week  by 1h")
	q.HumanDescription = []byte(fmt.Sprintf("CPU week  by 1h: %s", interval.StartString()))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

// AvgCPUUsageMonthByDay populates a Query with a query that looks like:
// SELECT mean(usage_user) from cpu where time >= '$MONTH_START' and time < '$MONTH_END' group by time(1d)
func (d *ElasticSearchDevops) AvgCPUUsageMonthByDay(q *Query) {
	interval := d.MonthIntervals.RandChoice()

	v := url.Values{}
	v.Set("q", fmt.Sprintf("SELECT mean(usage_user) from cpu where time >= '%s' and time < '%s' group by time(1d)", interval.StartString(), interval.EndString()))

	q.HumanLabel = []byte("CPU month by 1d")
	q.HumanDescription = []byte(fmt.Sprintf("CPU month by 1d: %s", interval.StartString()))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

// AvgMemAvailableDayByHour populates a Query with a query that looks like:
// SELECT mean(available) from mem where time >= '$DAY_START' and time < '$DAY_END' group by time(1h)
func (d *ElasticSearchDevops) AvgMemAvailableDayByHour(q *Query) {
	interval := d.DayIntervals.RandChoice()

	v := url.Values{}
	v.Set("q", fmt.Sprintf("SELECT mean(available) from mem where time >= '%s' and time < '%s' group by time(1h)", interval.StartString(), interval.EndString()))

	q.HumanLabel = []byte("mem day   by 1h")
	q.HumanDescription = []byte(fmt.Sprintf("mem day   by 1h: %s", interval.StartString()))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

// AvgMemAvailableWeekByHour populates a Query with a query that looks like:
// SELECT mean(available) from mem where time >= '$WEEK_START' and time < '$WEEK_END' group by time(1h)
func (d *ElasticSearchDevops) AvgMemAvailableWeekByHour(q *Query) {
	interval := d.WeekIntervals.RandChoice()

	v := url.Values{}
	v.Set("q", fmt.Sprintf("SELECT mean(available) from mem where time >= '%s' and time < '%s' group by time(1h)", interval.StartString(), interval.EndString()))

	q.HumanLabel = []byte("mem week  by 1h")
	q.HumanDescription = []byte(fmt.Sprintf("mem week  by 1h: %s", interval.StartString()))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

// AvgMemAvailableMonthByDay populates a Query with a query that looks like:
// SELECT mean(available) from mem where time >= '$MONTH_START' and time < '$MONTH_END' group by time(1d)
func (d *ElasticSearchDevops) AvgMemAvailableMonthByDay(q *Query) {
	interval := d.MonthIntervals.RandChoice()

	v := url.Values{}
	v.Set("q", fmt.Sprintf("SELECT mean(available) from mem where time >= '%s' and time < '%s' group by time(1d)", interval.StartString(), interval.EndString()))

	q.HumanLabel = []byte("mem month by 1d")
	q.HumanDescription = []byte(fmt.Sprintf("mem month by 1d: %s", interval.StartString()))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}
