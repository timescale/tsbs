package main

import (
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"time"
)

// CassandraDevops produces Cassandra-specific queries for all the devops query types.
type CassandraDevops struct {
	AllInterval  TimeInterval
}

// NewCassandraDevops makes an CassandraDevops object ready to generate Queries.
func newCassandraDevopsCommon(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	if !start.Before(end) {
		panic("bad time order")
	}

	return &CassandraDevops{
		AllInterval:  NewTimeInterval(start, end),
	}
}

// Dispatch fulfills the QueryGenerator interface.
func (d *CassandraDevops) Dispatch(i, scaleVar int) Query {
	q := NewHTTPQuery() // from pool
	devopsDispatchAll(d, i, q, scaleVar)
	return q
}

func (d *CassandraDevops) MaxCPUUsageHourByMinuteOneHost(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*HTTPQuery), scaleVar, 1)
}

func (d *CassandraDevops) MaxCPUUsageHourByMinuteTwoHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*HTTPQuery), scaleVar, 2)
}

func (d *CassandraDevops) MaxCPUUsageHourByMinuteFourHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*HTTPQuery), scaleVar, 4)
}

func (d *CassandraDevops) MaxCPUUsageHourByMinuteEightHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*HTTPQuery), scaleVar, 8)
}

func (d *CassandraDevops) MaxCPUUsageHourByMinuteSixteenHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*HTTPQuery), scaleVar, 16)
}

func (d *CassandraDevops) MaxCPUUsageHourByMinuteThirtyTwoHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*HTTPQuery), scaleVar, 32)
}

// MaxCPUUsageHourByMinuteThirtyTwoHosts populates a Query with a query that looks like:
// SELECT max(usage_user) from cpu where (hostname = '$HOSTNAME_1' or ... or hostname = '$HOSTNAME_N') and time >= '$HOUR_START' and time < '$HOUR_END' group by time(1m)
func (d *CassandraDevops) maxCPUUsageHourByMinuteNHosts(qi Query, scaleVar, nhosts int) {
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

	humanLabel := fmt.Sprintf("Cassandra max cpu, rand %4d hosts, rand 1hr by 1m", nhosts)
	q := qi.(*HTTPQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

// MeanCPUUsageDayByHourAllHosts populates a Query with a query that looks like:
// SELECT mean(usage_user) from cpu where time >= '$DAY_START' and time < '$DAY_END' group by time(1h),hostname
func (d *CassandraDevops) MeanCPUUsageDayByHourAllHostsGroupbyHost(qi Query, _ int) {
	interval := d.AllInterval.RandWindow(24*time.Hour)

	v := url.Values{}
	v.Set("db", d.DatabaseName)
	v.Set("q", fmt.Sprintf("SELECT mean(usage_user) from cpu where time >= '%s' and time < '%s' group by time(1h),hostname", interval.StartString(), interval.EndString()))

	humanLabel := "Cassandra mean cpu, all hosts, rand 1day by 1hour"
	q := qi.(*HTTPQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

//func (d *CassandraDevops) MeanCPUUsageDayByHourAllHostsGroupbyHost(qi Query, _ int) {
//	interval := d.AllInterval.RandWindow(24*time.Hour)
//
//	v := url.Values{}
//	v.Set("db", d.DatabaseName)
//	v.Set("q", fmt.Sprintf("SELECT count(usage_user) from cpu where time >= '%s' and time < '%s' group by time(1h)", interval.StartString(), interval.EndString()))
//
//	humanLabel := "Cassandra mean cpu, all hosts, rand 1day by 1hour"
//	q := qi.(*HTTPQuery)
//	q.HumanLabel = []byte(humanLabel)
//	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
//	q.Method = []byte("GET")
//	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
//	q.Body = nil
//}
