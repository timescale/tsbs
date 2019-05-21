package clickhouse

import (
	"fmt"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/query"
)

// TODO: Remove the need for this by continuing to bubble up errors
func panicIfErr(err error) {
	if err != nil {
		panic(err.Error())
	}
}

// Devops produces ClickHouse-specific queries for all the devops query types.
type Devops struct {
	*devops.Core
	UseTags bool
}

// NewDevops makes an Devops object ready to generate Queries.
func NewDevops(start, end time.Time, scale int) *Devops {
	core, err := devops.NewCore(start, end, scale)
	panicIfErr(err)
	return &Devops{core, false}
}

// GenerateEmptyQuery returns an empty query.ClickHouse
func (d *Devops) GenerateEmptyQuery() query.Query {
	return query.NewClickHouse()
}

// getHostWhereWithHostnames creates WHERE SQL statement for multiple hostnames.
// NOTE: 'WHERE' itself is not included, just hostname filter clauses, ready to concatenate to 'WHERE' string
func (d *Devops) getHostWhereWithHostnames(hostnames []string) string {
	hostnameSelectionClauses := []string{}

	if d.UseTags {
		// Use separated table for Tags
		// Need to prepare WHERE with `tags` table
		// WHERE tags_id IN (SELECT those tag.id FROM separated tags table WHERE )
		for _, s := range hostnames {
			hostnameSelectionClauses = append(hostnameSelectionClauses, fmt.Sprintf("'%s'", s))
		}
		return fmt.Sprintf("tags_id IN (SELECT id FROM tags WHERE hostname IN (%s))", strings.Join(hostnameSelectionClauses, ","))
	}

	// Here we DO NOT use tags as a separate table
	// So hostname is embedded into processed table itself and we can build direct WHERE statement as
	// ((hostname = 'H1') OR (hostname = 'H2') ...)

	// All tags are included into one table
	// Need to prepare WHERE (hostname = 'host1' OR hostname = 'host2') clause
	for _, s := range hostnames {
		hostnameSelectionClauses = append(hostnameSelectionClauses, fmt.Sprintf("hostname = '%s'", s))
	}
	// (host=h1 OR host=h2)
	return "(" + strings.Join(hostnameSelectionClauses, " OR ") + ")"
}

// getHostWhereString gets multiple random hostnames and create WHERE SQL statement for these hostnames.
func (d *Devops) getHostWhereString(nhosts int) string {
	hostnames, err := d.GetRandomHosts(nhosts)
	panicIfErr(err)
	return d.getHostWhereWithHostnames(hostnames)
}

// getSelectClausesAggMetrics gets specified aggregate function clause for multiple memtrics
// Ex.: max(cpu_time) AS max_cpu_time
func (d *Devops) getSelectClausesAggMetrics(aggregateFunction string, metrics []string) []string {
	selectAggregateClauses := make([]string, len(metrics))
	for i, metric := range metrics {
		selectAggregateClauses[i] = fmt.Sprintf("%[1]s(%[2]s) AS %[1]s_%[2]s", aggregateFunction, metric)
	}
	return selectAggregateClauses
}

// ClickHouse understands and can compare time presented as strings of this format
const clickhouseTimeStringFormat = "2006-01-02 15:04:05"

// MaxAllCPU selects the MAX of all metrics under 'cpu' per hour for nhosts hosts,
// e.g. in pseudo-SQL:
//
// SELECT MAX(metric1), ..., MAX(metricN)
// FROM cpu
// WHERE
// 		(hostname = '$HOSTNAME_1' OR ... OR hostname = '$HOSTNAME_N')
// 		AND time >= '$HOUR_START'
// 		AND time < '$HOUR_END'
// GROUP BY hour
// ORDER BY hour
//
// Resultsets:
// cpu-max-all-1
// cpu-max-all-8
func (d *Devops) MaxAllCPU(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.MaxAllDuration)
	metrics := devops.GetAllCPUMetrics()
	selectClauses := d.getSelectClausesAggMetrics("max", metrics)

	sql := fmt.Sprintf(`
        SELECT
            toStartOfHour(created_at) AS hour,
            %s
        FROM cpu
        WHERE %s AND (created_at >= '%s') AND (created_at < '%s')
        GROUP BY hour
        ORDER BY hour
        `,
		strings.Join(selectClauses, ", "),
		d.getHostWhereString(nHosts),
		interval.Start().Format(clickhouseTimeStringFormat),
		interval.End().Format(clickhouseTimeStringFormat))

	humanLabel := devops.GetMaxAllLabel("ClickHouse", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day,
// e.g. in pseudo-SQL:
//
// SELECT AVG(metric1), ..., AVG(metricN)
// FROM cpu
// WHERE time >= '$HOUR_START' AND time < '$HOUR_END'
// GROUP BY hour, hostname
// ORDER BY hour
//
// Resultsets:
// double-groupby-1
// double-groupby-5
// double-groupby-all
func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)

	selectClauses := make([]string, numMetrics)
	meanClauses := make([]string, numMetrics)
	for i, m := range metrics {
		meanClauses[i] = "mean_" + m
		selectClauses[i] = fmt.Sprintf("avg(%s) AS %s", m, meanClauses[i])
	}

	hostnameField := "hostname"
	joinClause := ""
	if d.UseTags {
		joinClause = "ANY INNER JOIN tags USING (id)"
	}

	sql := fmt.Sprintf(`
        SELECT
            hour,
            %s,
            %s
        FROM
        (
            SELECT
                toStartOfHour(created_at) AS hour,
                tags_id AS id,
                %s
            FROM cpu
            WHERE (created_at >= '%s') AND (created_at < '%s')
            GROUP BY
                hour,
                id
        ) AS cpu_avg
        %s
        ORDER BY
            hour ASC,
            %s
        `,
		hostnameField,                                       // main SELECT %s,
		strings.Join(meanClauses, ", "),                     // main SELECT %s
		strings.Join(selectClauses, ", "),                   // cpu_avg SELECT %s
		interval.Start().Format(clickhouseTimeStringFormat), // cpu_avg time >= '%s'
		interval.End().Format(clickhouseTimeStringFormat),   // cpu_avg time < '%s'
		joinClause,    // JOIN clause
		hostnameField) // ORDER BY %s

	humanLabel := devops.GetDoubleGroupByLabel("ClickHouse", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// GroupByOrderByLimit populates a query.Query that has a time WHERE clause, that groups by a truncated date, orders by that date, and takes a limit:
// SELECT time_bucket('1 minute', time) AS t, MAX(cpu)
// FROM cpu
// WHERE time < '$TIME'
// GROUP BY t
// ORDER BY t DESC
// LIMIT $LIMIT
//
// Resultsets:
// groupby-orderby-limit
func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour)

	sql := fmt.Sprintf(`
        SELECT
            toStartOfMinute(created_at) AS minute,
            max(usage_user)
        FROM cpu
        WHERE created_at < '%s'
        GROUP BY minute
        ORDER BY minute DESC
        LIMIT 5
        `,
		interval.End().Format(clickhouseTimeStringFormat))

	humanLabel := "ClickHouse max cpu over last 5 min-intervals (random end)"
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
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
	var hostWhereClause string
	if nHosts == 0 {
		hostWhereClause = ""
	} else {
		hostWhereClause = fmt.Sprintf("AND (%s)", d.getHostWhereString(nHosts))
	}
	interval := d.Interval.MustRandWindow(devops.HighCPUDuration)

	sql := fmt.Sprintf(`
        SELECT *
        FROM cpu
        PREWHERE (usage_user > 90.0) AND (created_at >= '%s') AND (created_at <  '%s') %s
        `,
		interval.Start().Format(clickhouseTimeStringFormat),
		interval.End().Format(clickhouseTimeStringFormat),
		hostWhereClause)

	humanLabel, err := devops.GetHighCPULabel("ClickHouse", nHosts)
	panicIfErr(err)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// LastPointPerHost finds the last row for every host in the dataset
//
// Resultsets:
// lastpoint
func (d *Devops) LastPointPerHost(qi query.Query) {
	var sql string
	if d.UseTags {
		sql = fmt.Sprintf(`
            SELECT *
            FROM
            (
                SELECT *
                FROM cpu
                WHERE (tags_id, created_at) IN
                (
                    SELECT
                        tags_id,
                        max(created_at)
                    FROM cpu
                    GROUP BY tags_id
                )
            ) AS c
            ANY INNER JOIN tags AS t ON c.tags_id = t.id
            ORDER BY
                t.hostname ASC,
                c.time DESC
            `)
	} else {
		sql = fmt.Sprintf(`
            SELECT DISTINCT(hostname), *
            FROM cpu
            ORDER BY
                hostname ASC,
                created_at DESC
            `)
	}

	humanLabel := "ClickHouse last row per host"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// GroupByTime selects the MAX for numMetrics metrics under 'cpu',
// per minute for nhosts hosts,
// e.g. in pseudo-SQL:
//
// SELECT minute, max(metric1), ..., max(metricN)
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
func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	selectClauses := d.getSelectClausesAggMetrics("max", metrics)

	sql := fmt.Sprintf(`
        SELECT
            toStartOfMinute(created_at) AS minute,
            %s
        FROM cpu
        WHERE %s AND (created_at >= '%s') AND (created_at < '%s')
        GROUP BY minute
        ORDER BY minute ASC
        `,
		strings.Join(selectClauses, ", "),
		d.getHostWhereString(nHosts),
		interval.Start().Format(clickhouseTimeStringFormat),
		interval.End().Format(clickhouseTimeStringFormat))

	humanLabel := fmt.Sprintf("ClickHouse %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// fill Query fills the query struct with data
func (d *Devops) fillInQuery(qi query.Query, humanLabel, humanDesc, sql string) {
	q := qi.(*query.ClickHouse)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.Table = []byte("cpu")
	q.SqlQuery = []byte(sql)
}
