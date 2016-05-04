package main

import (
	"fmt"
	"math/rand"
	"net/url"
	"strings"
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

func (d *InfluxDevops) MaxCPUUsageHourByMinuteOneHost(q *Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 1)
}

func (d *InfluxDevops) MaxCPUUsageHourByMinuteTwoHosts(q *Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 2)
}

func (d *InfluxDevops) MaxCPUUsageHourByMinuteFourHosts(q *Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 4)
}

func (d *InfluxDevops) MaxCPUUsageHourByMinuteEightHosts(q *Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 8)
}

func (d *InfluxDevops) MaxCPUUsageHourByMinuteSixteenHosts(q *Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 16)
}

func (d *InfluxDevops) MaxCPUUsageHourByMinuteThirtyTwoHosts(q *Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 32)
}

// MaxCPUUsageHourByMinuteThirtyTwoHosts populates a Query with a query that looks like:
// SELECT max(usage_user) from cpu where (hostname = '$HOSTNAME_1' or ... or hostname = '$HOSTNAME_N') and time >= '$HOUR_START' and time < '$HOUR_END' group by time(1m)
func (d *InfluxDevops) maxCPUUsageHourByMinuteNHosts(q *Query, scaleVar, nhosts int) {
	interval := d.AllInterval.RandWindow(time.Hour)
	nn := rand.Perm(scaleVar)[:nhosts]

	hostnames := []string{}
	for _, n := range nn {
		hostnames = append(hostnames, fmt.Sprintf("host_%d", n))
	}

	hostnameClauses := []string{}
	for _, s := range hostnames {
		hostnameClauses = append(hostnameClauses, fmt.Sprintf("hostname = '%s'", s))
	}

	combinedHostnameClause := strings.Join(hostnameClauses, " or ")

	v := url.Values{}
	v.Set("db", d.DatabaseName)
	v.Set("q", fmt.Sprintf("SELECT max(usage_user) from cpu where (%s) and time >= '%s' and time < '%s' group by time(1m)", combinedHostnameClause, interval.StartString(), interval.EndString()))

	humanLabel := fmt.Sprintf("Influx max cpu, rand %4d hosts, rand 1hr by 1m", nhosts)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}
