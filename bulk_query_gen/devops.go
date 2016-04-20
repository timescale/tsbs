package main

import (
	"fmt"
	"math/rand"
	"net/url"
	"time"
)

type Devops interface {
	AvgCPUUsageDayByHour(*Request)
	AvgCPUUsageMonthByDay(*Request)
}

type InfluxDevops struct {
	DatabaseName string
	DayIntervals TimeIntervals
	MonthIntervals TimeIntervals
}

type TimeInterval struct {
	Start, End time.Time
}

func (ti *TimeInterval) StartString() string {
	return ti.Start.Format(time.RFC3339)
}

func (ti *TimeInterval) EndString() string {
	return ti.End.Format(time.RFC3339)
}

type TimeIntervals []TimeInterval

func (tis TimeIntervals) RandChoice() *TimeInterval {
	return &tis[rand.Intn(len(tis))]
}

func NewTimeIntervals(start, end time.Time, window time.Duration) TimeIntervals {
	xs := TimeIntervals{}
	for start.Add(window).Before(end) || start.Add(window).Equal(end) {
		x := TimeInterval {
			Start: start,
			End: start.Add(window),
		}
		xs = append(xs, x)

		start = start.Add(window)
	}

	return xs
}

func NewInfluxDevops(databaseName string, start, end time.Time) *InfluxDevops {
	if !start.Before(end) {
		panic("bad time order")
	}
	return &InfluxDevops {
		DatabaseName: databaseName,
		DayIntervals: NewTimeIntervals(start, end, 24*time.Hour),
		MonthIntervals: NewTimeIntervals(start, end, 31*24*time.Hour),
	}
}

func (d *InfluxDevops) AvgCPUUsageDayByHour(req *Request) {
	interval := d.DayIntervals.RandChoice()

	v := url.Values{}
	v.Set("db", d.DatabaseName)
	v.Set("q", fmt.Sprintf("SELECT mean(usage_user) from cpu where time >= '%s' and time < '%s' group by time(1h)", interval.StartString(), interval.EndString()))

	req.Method = "GET"
	req.Path = "/query"
	req.QueryArguments = v
	req.Body = ""
}

func (d *InfluxDevops) AvgCPUUsageMonthByDay(req *Request) {
	interval := d.MonthIntervals.RandChoice()

	v := url.Values{}
	v.Set("db", d.DatabaseName)
	v.Set("q", fmt.Sprintf("SELECT mean(usage_user) from cpu where time >= '%s' and time < '%s' group by time(1d)", interval.StartString(), interval.EndString()))

	req.Method = "GET"
	req.Path = "/query"
	req.QueryArguments = v
	req.Body = ""
}
