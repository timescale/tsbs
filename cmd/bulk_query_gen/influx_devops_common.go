package main

import (
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"time"
)

// InfluxDevops produces Influx-specific queries for all the devops query types.
type InfluxDevops struct {
	DatabaseName string
	AllInterval  TimeInterval
}

// NewInfluxDevops makes an InfluxDevops object ready to generate Queries.
func newInfluxDevopsCommon(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
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

// Dispatch fulfills the QueryGenerator interface.
func (d *InfluxDevops) Dispatch(i, scaleVar int) Query {
	q := NewHTTPQuery() // from pool
	devopsDispatchAll(d, i, q, scaleVar)
	return q
}

func (d *InfluxDevops) MaxCPUUsageHourByMinuteOneHost(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*HTTPQuery), scaleVar, 1, time.Hour)
}

func (d *InfluxDevops) MaxCPUUsageHourByMinuteTwoHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*HTTPQuery), scaleVar, 2, time.Hour)
}

func (d *InfluxDevops) MaxCPUUsageHourByMinuteFourHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*HTTPQuery), scaleVar, 4, time.Hour)
}

func (d *InfluxDevops) MaxCPUUsageHourByMinuteEightHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*HTTPQuery), scaleVar, 8, time.Hour)
}

func (d *InfluxDevops) MaxCPUUsageHourByMinuteSixteenHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*HTTPQuery), scaleVar, 16, time.Hour)
}

func (d *InfluxDevops) MaxCPUUsageHourByMinuteThirtyTwoHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*HTTPQuery), scaleVar, 32, time.Hour)
}

func (d *InfluxDevops) MaxCPUUsage12HoursByMinuteOneHost(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q.(*HTTPQuery), scaleVar, 1, 12*time.Hour)
}

func (d *InfluxDevops) MaxAllCPUHourByMinuteOneHost(q Query, scaleVar int) {
	d.maxAllCPUHourByMinuteNHosts(q.(*HTTPQuery), scaleVar, 1)
}

func (d *InfluxDevops) MaxAllCPUHourByMinuteEightHosts(q Query, scaleVar int) {
	d.maxAllCPUHourByMinuteNHosts(q.(*HTTPQuery), scaleVar, 8)
}

// MaxCPUUsageHourByMinuteThirtyTwoHosts populates a Query with a query that looks like:
// SELECT max(usage_user) from cpu where (hostname = '$HOSTNAME_1' or ... or hostname = '$HOSTNAME_N') and time >= '$HOUR_START' and time < '$HOUR_END' group by time(1m)
func (d *InfluxDevops) maxCPUUsageHourByMinuteNHosts(qi Query, scaleVar, nhosts int, timeRange time.Duration) {
	interval := d.AllInterval.RandWindow(timeRange)
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

	humanLabel := fmt.Sprintf("Influx max cpu, rand %4d hosts, rand %s by 1m", nhosts, timeRange)
	q := qi.(*HTTPQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

// MeanCPUUsageDayByHourAllHosts populates a Query with a query that looks like:
// SELECT mean(usage_user) from cpu where time >= '$DAY_START' and time < '$DAY_END' group by time(1h),hostname
func (d *InfluxDevops) MeanCPUUsageDayByHourAllHostsGroupbyHost(qi Query, _ int) {
	interval := d.AllInterval.RandWindow(24 * time.Hour)

	v := url.Values{}
	v.Set("db", d.DatabaseName)
	v.Set("q", fmt.Sprintf("SELECT mean(usage_user) from cpu where time >= '%s' and time < '%s' group by time(1h),hostname", interval.StartString(), interval.EndString()))

	humanLabel := "Influx mean cpu, all hosts, rand 1day by 1hour"
	q := qi.(*HTTPQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

func (d *InfluxDevops) maxAllCPUHourByMinuteNHosts(qi Query, scaleVar, nhosts int) {
	interval := d.AllInterval.RandWindow(12 * time.Hour)
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
	v.Set("q", fmt.Sprintf("SELECT max(usage_user),max(usage_system),max(usage_idle),max(usage_nice),max(usage_iowait),max(usage_irq),max(usage_softirq),max(usage_steal),max(usage_guest),max(usage_guest_nice) from cpu where (%s) and time >= '%s' and time < '%s' group by time(1m)", combinedHostnameClause, interval.StartString(), interval.EndString()))

	humanLabel := fmt.Sprintf("Influx max cpu all fields, rand %4d hosts, rand 12hr by 1m", nhosts)
	q := qi.(*HTTPQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

//func (d *InfluxDevops) MeanCPUUsageDayByHourAllHostsGroupbyHost(qi Query, _ int) {
//	interval := d.AllInterval.RandWindow(24*time.Hour)
//
//	v := url.Values{}
//	v.Set("db", d.DatabaseName)
//	v.Set("q", fmt.Sprintf("SELECT count(usage_user) from cpu where time >= '%s' and time < '%s' group by time(1h)", interval.StartString(), interval.EndString()))
//
//	humanLabel := "Influx mean cpu, all hosts, rand 1day by 1hour"
//	q := qi.(*HTTPQuery)
//	q.HumanLabel = []byte(humanLabel)
//	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
//	q.Method = []byte("GET")
//	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
//	q.Body = nil
//}

func (d *InfluxDevops) LastPointPerHost(qi Query, _ int) {

	v := url.Values{}
	v.Set("db", d.DatabaseName)
	v.Set("q", "SELECT * from cpu group by \"hostname\" order by time desc limit 1")

	humanLabel := "Influx last row per host"
	q := qi.(*HTTPQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: cpu", humanLabel))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

// SELECT * where CPU > threshold and <some time period>
func (d *InfluxDevops) HighCPU(qi Query, _ int) {
	interval := d.AllInterval.RandWindow(24 * time.Hour)

	v := url.Values{}
	v.Set("db", d.DatabaseName)
	v.Set("q", fmt.Sprintf("SELECT * from cpu where usage_user > 90.0 and time >= '%s' and time < '%s'", interval.StartString(), interval.EndString()))

	humanLabel := "Influx cpu over threshold"
	q := qi.(*HTTPQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

// SELECT * where CPU > threshold and device_type = FOO and <some time period>
func (d *InfluxDevops) HighCPUAndField(qi Query, hosts int) {
	interval := d.AllInterval.RandWindow(24 * time.Hour)

	v := url.Values{}
	v.Set("db", d.DatabaseName)
	v.Set("q", fmt.Sprintf("SELECT * from cpu where usage_user > 90.0 and host == 'host_%d' and time >= '%s' and time < '%s'", rand.Intn(hosts), interval.StartString(), interval.EndString()))

	humanLabel := "Influx cpu over threshold with field"
	q := qi.(*HTTPQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

func (d *InfluxDevops) MultipleMemFieldsOrs(qi Query, _ int) {
	interval := d.AllInterval.RandWindow(24 * time.Hour)
	v := url.Values{}
	v.Set("db", d.DatabaseName)
	v.Set("q", fmt.Sprintf("SELECT * from mem where used < 1000 or used_percent > 98.0 or used_percent < 10.0 and time >= '%s' and time < '%s' ", interval.StartString(), interval.EndString()))

	humanLabel := "Influx mem fields with or"
	q := qi.(*HTTPQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

func (d *InfluxDevops) MultipleMemFieldsOrsGroupedByHost(qi Query, _ int) {
	interval := d.AllInterval.RandWindow(24 * time.Hour)
	v := url.Values{}
	v.Set("db", d.DatabaseName)
	v.Set("q", fmt.Sprintf("SELECT MAX(used_percent) from mem where used < 1000 or used_percent > 98.0 or used_percent < 10.0 and time >= '%s' and time < '%s' GROUP BY time(1h),hostname", interval.StartString(), interval.EndString()))

	humanLabel := "Influx mem fields with or by host"
	q := qi.(*HTTPQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval))
	q.Method = []byte("GET")
	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
	q.Body = nil
}

// SELECT * where CPU > threshold and <some time period>
// "SELECT * from cpu where cpu > 90.0 and time >= '%s' and time < '%s'", interval.StartString(), interval.EndString()))

// SELECT * where CPU > threshold and device_type = FOO and <some time period>
// "SELECT * from cpu where cpu > 90.0 and host == 'host0' and time >= '%s' and time < '%s'", interval.StartString(), interval.EndString()))

// SELECT * where CPU > threshold OR battery < 5% OR free_memory < threshold and <some time period>
// "SELECT * from cpu,mem,disk where cpu > 90.0 and free < 10.0 and used_percent < 90.0 and time >= '%s' and time < '%s' GROUP BY 'host'", interval.StartString(), interval.EndString()))

// SELECT device_id, COUNT() where CPU > threshold OR battery < 5% OR free_memory < threshold and <some time period> GROUP BY device_id
// SELECT avg(cpu) where <some time period> GROUP BY customer_id, location_id
