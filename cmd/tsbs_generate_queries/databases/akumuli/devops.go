package akumuli

import (
	"bytes"
	"fmt"
	"html/template"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/query"
)

// Devops produces Influx-specific queries for all the devops query types.
type Devops struct {
	*devops.Core
}

// NewDevops makes an Devops object ready to generate Queries.
func NewDevops(start, end time.Time, scale int) *Devops {
	return &Devops{devops.NewCore(start, end, scale)}
}

// GenerateEmptyQuery returns an empty query.HTTP
func (d *Devops) GenerateEmptyQuery() query.Query {
	return query.NewHTTP()
}

// GroupByTime selects the MAX for a single metric under 'cpu',
// per minute for nhosts hosts,
// e.g. in pseudo-SQL:
//
// SELECT minute, max(metric)
// FROM cpu
// WHERE
// 		(hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// 	AND time >= '$HOUR_START'
// 	AND time < '$HOUR_END'
// GROUP BY minute
// ORDER BY minute ASC
//
// Resultsets:
// single-groupby-1-1-12
// single-groupby-1-1-1
// single-groupby-1-8-1
// single-groupby-5-1-12
// single-groupby-5-1-1
// single-groupby-5-8-1
func (d *Devops) GroupByTime(qi query.Query, nhosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.RandWindow(timeRange)
	metrics := devops.GetCPUMetricsSlice(numMetrics)
	hostnames := d.GetRandomHosts(nhosts)
	combinedHostnameClause := "\"" + strings.Join(hostnames, "\",\"") + "\""

	if len(metrics) != 1 {
		return
	}

	startTimestamp := interval.StartUnixNano()
	endTimestamp := interval.EndUnixNano()

	const tmplString = `
	{
		"group-aggregate": {
			"metric": "cpu.{{.MetricName}}",
			"func": [ "max" ],
			"step": "1m"
		},
		"range": {
			"from": {{.StartTimestamp}},
			"to": {{.EndTimestamp}}
		},
		"where": {
			"hostname": [ {{.CombinedHostnameClause}} ]
		},
		"output": {
			"format": "csv"
		}
	}
	`

	tmpl := template.Must(template.New("tmpl").Parse(tmplString))
	bodyWriter := new(bytes.Buffer)

	arg := struct {
		StartTimestamp, EndTimestamp int64
		CombinedHostnameClause       string
		MetricName                   string
	}{
		startTimestamp,
		endTimestamp,
		combinedHostnameClause,
		metrics[0], // TODO: loop
	}
	err := tmpl.Execute(bodyWriter, arg)
	if err != nil {
		panic("logic error")
	}

	humanLabel := fmt.Sprintf("Akumuli max cpu, rand %4d hosts, rand %s by 1m", nhosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, string(bodyWriter.Bytes()), interval.StartUnixNano(), interval.EndUnixNano())
}

// HighCPUForHosts populates a query that gets CPU metrics when the CPU has high
// usage between a time period for a number of hosts (if 0, it will search all hosts),
// e.g. in pseudo-SQL:
//
// SELECT * FROM cpu
// WHERE usage_user > 90.0
// AND time >= '$TIME_START' AND time < '$TIME_END'
// AND (hostname = '$HOST' OR hostname = '$HOST2'...)
//
// Resultsets:
// high-cpu-1
// high-cpu-all
func (d *Devops) HighCPUForHosts(qi query.Query, nHosts int) {
	interval := d.Interval.RandWindow(devops.HighCPUDuration)

	var hostWhereClause string
	if nHosts == 0 {
		hostWhereClause = ""
	} else {
		hostnames := d.GetRandomHosts(nHosts)
		combinedHostnameClause := "\"" + strings.Join(hostnames, "\",\"") + "\""
		hostWhereClause = fmt.Sprintf(`
		"where": {
			"hostname": [ %s ]
		},`,
			combinedHostnameClause)
	}

	startTimestamp := interval.StartUnixNano()
	endTimestamp := interval.EndUnixNano()

	const tmplString = `
	{
		"select": "cpu.usage_user",
		"range": {
			"from": {{.StartTimestamp}},
			"to": {{.EndTimestamp}}
		},
		"filter": {
			"cpu": { "gt": 90.0 }
		},
		"output": {
			"format": "csv"
		}
		{{.WhereClause}}
	}
	`

	tmpl := template.Must(template.New("tmpl").Parse(tmplString))
	bodyWriter := new(bytes.Buffer)

	arg := struct {
		StartTimestamp, EndTimestamp int64
		WhereClause                  string
	}{
		startTimestamp,
		endTimestamp,
		hostWhereClause,
	}
	err := tmpl.Execute(bodyWriter, arg)
	if err != nil {
		panic("logic error")
	}

	humanLabel := fmt.Sprintf("Akumuli high CPU, rand %4d hosts, rand %s", nHosts, interval)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, string(bodyWriter.Bytes()), interval.StartUnixNano(), interval.EndUnixNano())
}

func (d *Devops) fillInQuery(qi query.Query, humanLabel, humanDesc, body string, begin, end int64) {
	q := qi.(*query.HTTP)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.Method = []byte("POST")
	q.Path = []byte("/api/query")
	q.Body = []byte(body)
	q.StartTimestamp = begin
	q.EndTimestamp = end
}
