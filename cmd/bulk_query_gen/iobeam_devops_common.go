package main

import (
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"
)

// IobeamDevops produces Influx-specific queries for all the devops query types.
type IobeamDevops struct {
	AllInterval TimeInterval
}

// NewIobeamDevops makes an InfluxDevops object ready to generate Queries.
func newIobeamDevopsCommon(dbConfig DatabaseConfig, start, end time.Time) QueryGenerator {
	if !start.Before(end) {
		panic("bad time order")
	}

	return &IobeamDevops{
		AllInterval: NewTimeInterval(start, end),
	}
}

func (d *IobeamDevops) getHostWhereString(scaleVar int, nhosts int) string {

	if nhosts > scaleVar {
		log.Fatal("nhosts > scaleVar")
	}

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

	return combinedHostnameClause
}

// Dispatch fulfills the QueryGenerator interface.
func (d *IobeamDevops) Dispatch(i, scaleVar int) Query {
	q := NewIobeamQuery() // from pool
	devopsDispatchAll(d, i, q, scaleVar)
	return q
}

func (d *IobeamDevops) MaxCPUUsageHourByMinuteOneHost(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 1, time.Hour)
}

func (d *IobeamDevops) MaxCPUUsageHourByMinuteTwoHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 2, time.Hour)
}

func (d *IobeamDevops) MaxCPUUsageHourByMinuteFourHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 4, time.Hour)
}

func (d *IobeamDevops) MaxCPUUsageHourByMinuteEightHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 8, time.Hour)
}

func (d *IobeamDevops) MaxCPUUsageHourByMinuteSixteenHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 16, time.Hour)
}

func (d *IobeamDevops) MaxCPUUsageHourByMinuteThirtyTwoHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 32, time.Hour)
}

func (d *IobeamDevops) MaxCPUUsage12HoursByMinuteOneHost(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 1, 12*time.Hour)
}

func (d *IobeamDevops) MaxAllCPUHourByMinuteOneHost(q Query, scaleVar int) {
	d.maxAllCPUHourByMinuteNHosts(q, scaleVar, 1)
}

func (d *IobeamDevops) MaxAllCPUHourByMinuteEightHosts(q Query, scaleVar int) {
	d.maxAllCPUHourByMinuteNHosts(q, scaleVar, 8)
}

// MaxCPUUsageHourByMinuteThirtyTwoHosts populates a Query with a query that looks like:
// SELECT max(usage_user) from cpu where (hostname = '$HOSTNAME_1' or ... or hostname = '$HOSTNAME_N') and time >= '$HOUR_START' and time < '$HOUR_END' group by time(1m)
func (d *IobeamDevops) maxCPUUsageHourByMinuteNHosts(qi Query, scaleVar, nhosts int, timeRange time.Duration) {
	interval := d.AllInterval.RandWindow(timeRange)

	sqlQuery := fmt.Sprintf(`SELECT date_trunc('minute'::text, io2ts(time)) AS minute, max(usage_user) FROM cpu where %s AND time >= %d AND time < %d GROUP BY minute ORDER BY minute ASC`, d.getHostWhereString(scaleVar, nhosts), interval.Start.UnixNano(), interval.End.UnixNano())

	humanLabel := fmt.Sprintf("Iobeam max cpu, rand %4d hosts, rand %s by 1m", nhosts, timeRange)
	q := qi.(*IobeamQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.NamespaceName = []byte("cpu")
	q.FieldName = []byte("usage_user")
	q.SqlQuery = []byte(sqlQuery)
}

// MeanCPUUsageDayByHourAllHosts populates a Query with a query that looks like:
// SELECT mean(usage_user) from cpu where time >= '$DAY_START' and time < '$DAY_END' group by time(1h),hostname
func (d *IobeamDevops) MeanCPUUsageDayByHourAllHostsGroupbyHost(qi Query, _ int) {
	interval := d.AllInterval.RandWindow(24 * time.Hour)

	sqlQuery := fmt.Sprintf(`SELECT date_trunc('hour'::text, io2ts(time)) as hour, hostname, avg(usage_user) as mean_usage_user FROM cpu WHERE time >= %d AND time < %d GROUP BY hour, hostname ORDER BY hour`, interval.Start.UnixNano(), interval.End.UnixNano())

	humanLabel := "Iobeam mean cpu, all hosts, rand 1day by 1hour"
	q := qi.(*IobeamQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.NamespaceName = []byte("cpu")
	q.FieldName = []byte("usage_user")
	q.SqlQuery = []byte(sqlQuery)

}

// MaxCPUUsageHourByMinuteThirtyTwoHosts populates a Query with a query that looks like:
// SELECT max(usage_user) from cpu where (hostname = '$HOSTNAME_1' or ... or hostname = '$HOSTNAME_N') and time >= '$HOUR_START' and time < '$HOUR_END' group by time(1m)
func (d *IobeamDevops) maxAllCPUHourByMinuteNHosts(qi Query, scaleVar, nhosts int) {
	interval := d.AllInterval.RandWindow(12 * time.Hour)

	sqlQuery := fmt.Sprintf(`SELECT date_trunc('hour'::text, io2ts(time)) AS hour, max(usage_user) as max_usage_user FROM cpu where %s AND time >= %d AND time < %d GROUP BY hour ORDER BY hour`, d.getHostWhereString(scaleVar, nhosts), interval.Start.UnixNano(), interval.End.UnixNano())

	humanLabel := fmt.Sprintf("Iobeam max cpu all fields, rand %4d hosts, rand 12hr by 1m", nhosts)
	q := qi.(*IobeamQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.NamespaceName = []byte("cpu")
	q.FieldName = []byte("usage_user")
	q.SqlQuery = []byte(sqlQuery)
}

func (d *IobeamDevops) LastPointPerHost(qi Query, _ int) {
	measure := measurements[rand.Intn(len(measurements))]

	sqlQuery := fmt.Sprintf(`SELECT DISTINCT ON (hostname) * FROM cpu ORDER BY hostname, time DESC`)

	humanLabel := "Iobeam last row per host"
	q := qi.(*IobeamQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, measure))
	q.NamespaceName = []byte(measure)
	q.FieldName = []byte("*")
	q.SqlQuery = []byte(sqlQuery)
}

//func (d *IobeamDevops) MeanCPUUsageDayByHourAllHostsGroupbyHost(qi Query, _ int) {
//	interval := d.AllInterval.RandWindow(24*time.Hour)
//
//	v := url.Values{}
//	v.Set("db", d.DatabaseName)
//	v.Set("q", fmt.Sprintf("SELECT count(usage_user) from cpu where time >= '%s' and time < '%s' group by time(1h)", interval.StartString(), interval.EndString()))
//
//	humanLabel := "Iobeam mean cpu, all hosts, rand 1day by 1hour"
//	q := qi.(*HTTPQuery)
//	q.HumanLabel = []byte(humanLabel)
//	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
//	q.Method = []byte("GET")
//	q.Path = []byte(fmt.Sprintf("/query?%s", v.Encode()))
//	q.Body = nil
//}

// SELECT * where CPU > threshold and <some time period>
// "SELECT * from cpu where cpu > 90.0 and time >= '%s' and time < '%s'", interval.StartString(), interval.EndString()))
func (d *IobeamDevops) HighCPU(qi Query, _ int) {
	interval := d.AllInterval.RandWindow(24 * time.Hour)

	sqlQuery := fmt.Sprintf(`SELECT * FROM cpu WHERE usage_user > 90.0 AND time >= %d AND time < %d`, interval.Start.UnixNano(), interval.End.UnixNano())

	humanLabel := "Iobeam cpu over threshold, all hosts"
	q := qi.(*IobeamQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.NamespaceName = []byte("cpu")
	q.FieldName = []byte("*")
	q.SqlQuery = []byte(sqlQuery)

}

func (d *IobeamDevops) HighCPUAndField(qi Query, hosts int) {
	interval := d.AllInterval.RandWindow(24 * time.Hour)
	hostName := fmt.Sprintf("host_%d", rand.Intn(hosts))

	sqlQuery := fmt.Sprintf(`SELECT * FROM cpu WHERE usage_user > 90.0 and time >= %d AND time < %d and hostname = '%s'`, interval.Start.UnixNano(), interval.End.UnixNano(), hostName)

	humanLabel := "Iobeam cpu over threshold, all hosts"
	q := qi.(*IobeamQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.NamespaceName = []byte("cpu")
	q.FieldName = []byte("*")
	q.SqlQuery = []byte(sqlQuery)
}

// "SELECT * from mem where used_percent > 98.0 or used > 10000 or used_percent < 5.0 and time >= '%s' and time < '%s' ", interval.StartString(), interval.EndString()))

func (d *IobeamDevops) MultipleMemOrs(qi Query, hosts int) {
	interval := d.AllInterval.RandWindow(24 * time.Hour)

	sqlQuery := fmt.Sprintf(`SELECT * FROM mem WHERE used_percent > 98.0 OR used > 10000 OR used_percent < 5.0 AND time >= %d AND time < %d`, interval.Start.UnixNano(), interval.End.UnixNano())

	humanLabel := "Iobeam mem fields with or, all hosts"
	q := qi.(*IobeamQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.NamespaceName = []byte("mem")
	q.FieldName = []byte("*")
	q.SqlQuery = []byte(sqlQuery)
}

func (d *IobeamDevops) MultipleMemOrsByHost(qi Query, hosts int) {
	interval := d.AllInterval.RandWindow(24 * time.Hour)

	sqlQuery := fmt.Sprintf(`SELECT date_trunc('hour'::text, io2ts(time)) AS hour, MAX(used_percent) from mem where used < 1000 or used_percent > 98.0 or used_percent < 10.0 and time >= %d and time < %d GROUP BY hour,hostname`, interval.Start.UnixNano(), interval.End.UnixNano())

	humanLabel := "Iobeam mem fields with or, all hosts"
	q := qi.(*IobeamQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.NamespaceName = []byte("mem")
	q.FieldName = []byte("*")

	q.SqlQuery = []byte(sqlQuery)
}

// SELECT * where CPU > threshold OR battery < 5% OR free_memory < threshold and <some time period>
// "SELECT * from cpu,mem,disk where cpu > 90.0 and free < 10.0 and used_percent < 90.0 and time >= '%s' and time < '%s' GROUP BY 'host'", interval.StartString(), interval.EndString()))

// SELECT device_id, COUNT() where CPU > threshold OR battery < 5% OR free_memory < threshold and <some time period> GROUP BY device_id
// SELECT avg(cpu) where <some time period> GROUP BY customer_id, location_id
