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

type InfluxDevopsSingleHost struct {
	InfluxDevops
}

// NewInfluxDevops makes an InfluxDevops object ready to generate Queries.
func NewInfluxDevops(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	if !start.Before(end) {
		panic("bad time order")
	}
	if _, ok := dbConfig["database-name"]; !ok {
		panic("need influx database name")
	}

	return &InfluxDevops{
		DatabaseName: dbConfig["database-name"],
		AllInterval:  NewTimeInterval(start, end),
	}
}

func NewInfluxDevopsSingleHost(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	underlying := NewInfluxDevops(dbConfig, start, end).(*InfluxDevops)
	return &InfluxDevopsSingleHost{
		InfluxDevops: *underlying,
	}

}

func (d *InfluxDevopsSingleHost) Dispatch(i int, q *Query, scaleVar int) {
	d.MaxCPUUsageHourByMinuteOneHost(q, scaleVar)
}

// Dispatch fulfills the QueryGenerator interface.
func (d *InfluxDevops) Dispatch(i int, q *Query, scaleVar int) {
	devopsDispatchAll(d, i, q, scaleVar)
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
