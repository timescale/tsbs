package ceresdb

import (
	"fmt"
	"strings"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/pkg/query"
)

type Devops struct {
	*BaseGenerator
	*devops.Core
}

func panicIfErr(err error) {
	if err != nil {
		panic(err.Error())
	}
}

const (
	oneMintue = 60
	oneHour   = oneMintue * 60

	timeStringFormat = "2006-01-02 15:04:05"
	timestampColumn  = "timestamp"
)

func (d *Devops) getTimeBucket(seconds int) string {
	return fmt.Sprintf("time_bucket(%s, 'PT%dS')", timestampColumn, seconds)
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

// getHostWhereWithHostnames creates WHERE SQL statement for multiple hostnames.
// NOTE: 'WHERE' itself is not included, just hostname filter clauses, ready to concatenate to 'WHERE' string
func (d *Devops) getHostWhereWithHostnames(hostnames []string) string {
	hostnameSelectionClauses := []string{}

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

func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	selectClauses := d.getSelectClausesAggMetrics("max", metrics)

	sql := fmt.Sprintf(`
        SELECT
            %s as minute,
            %s
        FROM cpu
        WHERE %s AND (timestamp >= '%s') AND (timestamp < '%s')
        GROUP BY minute
        ORDER BY minute ASC
        `,
		d.getTimeBucket(oneMintue),
		strings.Join(selectClauses, ", "),
		d.getHostWhereString(nHosts),
		interval.Start().Format(timeStringFormat),
		interval.End().Format(timeStringFormat))

	humanLabel := fmt.Sprintf("CeresDB %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}
