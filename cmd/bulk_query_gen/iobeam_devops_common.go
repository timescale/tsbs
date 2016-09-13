package main

import (
	"fmt"
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

// Dispatch fulfills the QueryGenerator interface.
func (d *IobeamDevops) Dispatch(i, scaleVar int) Query {
	q := NewIobeamQuery() // from pool
	devopsDispatchAll(d, i, q, scaleVar)
	return q
}

func (d *IobeamDevops) MaxCPUUsageHourByMinuteOneHost(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 1)
}

func (d *IobeamDevops) MaxCPUUsageHourByMinuteTwoHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 2)
}

func (d *IobeamDevops) MaxCPUUsageHourByMinuteFourHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 4)
}

func (d *IobeamDevops) MaxCPUUsageHourByMinuteEightHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 8)
}

func (d *IobeamDevops) MaxCPUUsageHourByMinuteSixteenHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 16)
}

func (d *IobeamDevops) MaxCPUUsageHourByMinuteThirtyTwoHosts(q Query, scaleVar int) {
	d.maxCPUUsageHourByMinuteNHosts(q, scaleVar, 32)
}

// MaxCPUUsageHourByMinuteThirtyTwoHosts populates a Query with a query that looks like:
// SELECT max(usage_user) from cpu where (hostname = '$HOSTNAME_1' or ... or hostname = '$HOSTNAME_N') and time >= '$HOUR_START' and time < '$HOUR_END' group by time(1m)
func (d *IobeamDevops) maxCPUUsageHourByMinuteNHosts(qi Query, scaleVar, nhosts int) {
	interval := d.AllInterval.RandWindow(12 * time.Hour)
	nn := rand.Perm(scaleVar)[:nhosts]

	hostnames := []string{}
	for _, n := range nn {
		hostnames = append(hostnames, fmt.Sprintf("host_%d", n))
	}

	hostnameClauses := []string{}
	for _, s := range hostnames {
		hostnameClauses = append(hostnameClauses, fmt.Sprintf("new_field_predicate('hostname', '=', '%s'::text)", s))
	}

	combinedHostnameClause := strings.Join(hostnameClauses, ",")

	sqlQuery := fmt.Sprintf(`SELECT * FROM ioql_exec_query(new_ioql_query(
	project_id => 1::bigint, 
	namespace_name => 'cpu', 
	select_field => ARRAY[new_select_item('usage_user'::text, 'MAX')], 
	aggregate => new_aggregate(60000000000, 'hostname'),
	time_condition => new_time_condition(%d, %d),
	field_condition=> new_field_condition('OR', ARRAY[%s]),
	limit_rows => NULL,
	limit_time_periods => NULL,
	limit_by_field => NULL,
	total_partitions => 1
))`, interval.Start.UnixNano(), interval.End.UnixNano(), combinedHostnameClause)

	humanLabel := fmt.Sprintf("Iobeam max cpu, rand %4d hosts, rand 12hr by 1m", nhosts)
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

	sqlQuery := fmt.Sprintf(`SELECT * FROM ioql_exec_query(new_ioql_query(
	project_id => 1::bigint, 
	namespace_name => 'cpu', 
	select_field => ARRAY[new_select_item('usage_user'::text, 'MAX')], 
	aggregate => new_aggregate(3600000000000, 'hostname'),
	time_condition => new_time_condition(%d, %d),
	field_condition=> NULL,
	limit_rows => NULL,
	limit_time_periods => NULL,
	limit_by_field => NULL,
	total_partitions => 1
))`, interval.Start.UnixNano(), interval.End.UnixNano())

	humanLabel := "Iobeam mean cpu, all hosts, rand 1day by 1hour"
	q := qi.(*IobeamQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(fmt.Sprintf("%s: %s", humanLabel, interval.StartString()))
	q.NamespaceName = []byte("cpu")
	q.FieldName = []byte("usage_user")
	q.SqlQuery = []byte(sqlQuery)

}

func (d *IobeamDevops) LastPointPerHost(qi Query, _ int) {
	measure := measurements[rand.Intn(len(measurements))]

	sqlQuery := fmt.Sprintf(`SELECT * FROM ioql_exec_query(new_ioql_query(
	project_id => 1::bigint, 
	namespace_name => '%s', 
	select_field => NULL, 
	aggregate => NULL,
	time_condition => NULL,
	field_condition=> NULL,
	limit_rows => NULL,
	limit_time_periods => NULL,
	limit_by_field => new_limit_by_field('hostname', 1),
	total_partitions => 1
))`, measure)

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

	sqlQuery := fmt.Sprintf(`SELECT * FROM ioql_exec_query(new_ioql_query(
	project_id => 1::bigint, 
	namespace_name => 'cpu', 
	select_field =>  ARRAY[new_select_item('usage_user'::text)], 
	time_condition => new_time_condition(%d, %d),
	field_condition=> new_field_condition('AND', ARRAY[ new_field_predicate('cpu', '>', '90.0'::double) ]),
	limit_rows => NULL,
	limit_time_periods => NULL,
	limit_by_field => NULL
	total_partitions => 1,
))`, interval.Start.UnixNano(), interval.End.UnixNano())

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

	sqlQuery := fmt.Sprintf(`SELECT * FROM ioql_exec_query(new_ioql_query(
	project_id => 1::bigint, 
	namespace_name => 'cpu', 
	select_field =>  ARRAY[new_select_item('usage_user'::text)], 
	time_condition => new_time_condition(%d, %d),
	field_condition=> new_field_condition('AND', ARRAY[ new_field_predicate('cpu', '>', '90.0'::double) , new_field_predicate('hostname', '==', '%s'::text) ]),
	limit_rows => NULL,
	limit_time_periods => NULL,
	limit_by_field => NULL
	total_partitions => 1,
))`, interval.Start.UnixNano(), interval.End.UnixNano(), hostName)

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

	sqlQuery := fmt.Sprintf(`SELECT * FROM ioql_exec_query(new_ioql_query(
	project_id => 1::bigint, 
	namespace_name => 'mem', 
	time_condition => new_time_condition(%d, %d),
	field_condition=> new_field_condition('OR', ARRAY[ new_field_predicate('user_percentage', '>', '98.0'::double) , new_field_predicate('used', '<', '1000'::long) , new_field_predicate('used_percentage', '<', '99.0'::double) ]),
	limit_rows => NULL,
	limit_time_periods => NULL,
	limit_by_field => NULL
	total_partitions => 1,
))`, interval.Start.UnixNano(), interval.End.UnixNano())

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

	sqlQuery := fmt.Sprintf(`SELECT * FROM ioql_exec_query(new_ioql_query(
	project_id => 1::bigint, 
	namespace_name => 'mem', 
	time_condition => new_time_condition(%d, %d),
	field_condition=> new_field_condition('OR', ARRAY[ new_field_predicate('user_percentage', '>', '98.0'::double) , new_field_predicate('used', '<', '1000'::long) , new_field_predicate('used_percentage', '<', '99.0'::double) ]),
	limit_rows => NULL,
	limit_time_periods => NULL,
	limit_by_field => NULL
	total_partitions => 1,
	group_by => 'hostname',
))`, interval.Start.UnixNano(), interval.End.UnixNano())

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
