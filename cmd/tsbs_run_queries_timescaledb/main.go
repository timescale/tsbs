// tsbs_run_queries_timescaledb speed tests TimescaleDB using requests from stdin or file
//
// It reads encoded Query objects from stdin or file, and makes concurrent requests
// to the provided PostgreSQL/TimescaleDB endpoint.
// This program has no knowledge of the internals of the endpoint.
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/query"
)

const pgxDriver = "pgx" // default driver
const pqDriver = "postgres"

// Program option vars:
var (
	postgresConnect string
	hostList        []string
	user            string
	pass            string
	port            string
	showExplain     bool
	forceTextFormat bool
)

// Global vars:
var (
	runner *query.BenchmarkRunner
	driver string
)

// Parse args:
func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("postgres", "host=postgres user=postgres sslmode=disable",
		"String of additional PostgreSQL connection parameters, e.g., 'sslmode=disable'. Parameters for host and database will be ignored.")
	pflag.String("hosts", "localhost", "Comma separated list of PostgreSQL hosts (pass multiple values for sharding reads on a multi-node setup)")
	pflag.String("user", "postgres", "User to connect to PostgreSQL as")
	pflag.String("pass", "", "Password for the user connecting to PostgreSQL (leave blank if not password protected)")
	pflag.String("port", "5432", "Which port to connect to on the database host")

	pflag.Bool("show-explain", false, "Print out the EXPLAIN output for sample query")
	pflag.Bool("force-text-format", false, "Send/receive data in text format")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	postgresConnect = viper.GetString("postgres")
	hosts := viper.GetString("hosts")
	user = viper.GetString("user")
	pass = viper.GetString("pass")
	port = viper.GetString("port")
	showExplain = viper.GetBool("show-explain")
	forceTextFormat = viper.GetBool("force-text-format")

	runner = query.NewBenchmarkRunner(config)

	if showExplain {
		runner.SetLimit(1)
	}

	if forceTextFormat {
		driver = pqDriver
	} else {
		driver = pgxDriver
	}

	// Parse comma separated string of hosts and put in a slice (for multi-node setups)
	for _, host := range strings.Split(hosts, ",") {
		hostList = append(hostList, host)
	}
}

func main() {
	runner.Run(&query.TimescaleDBPool, newProcessor)
}

// Get the connection string for a connection to PostgreSQL.

// If we're running queries against multiple nodes we need to balance the queries
// across replicas. Each worker is assigned a sequence number -- we'll use that
// to evenly distribute hosts to worker connections
func getConnectString(workerNumber int) string {
	// User might be passing in host=hostname the connect string out of habit which may override the
	// multi host configuration. Same for dbname= and user=. This sanitizes that.
	re := regexp.MustCompile(`(host|dbname|user)=\S*\b`)
	connectString := re.ReplaceAllString(postgresConnect, "")

	// Round robin the host/worker assignment by assigning a host based on workerNumber % totalNumberOfHosts
	host := hostList[workerNumber%len(hostList)]
	connectString = fmt.Sprintf("host=%s dbname=%s user=%s %s", host, runner.DatabaseName(), user, connectString)

	// For optional parameters, ensure they exist then interpolate them into the connectString
	if len(port) > 0 {
		connectString = fmt.Sprintf("%s port=%s", connectString, port)
	}
	if len(pass) > 0 {
		connectString = fmt.Sprintf("%s password=%s", connectString, pass)
	}
	if forceTextFormat {
		connectString = fmt.Sprintf("%s disable_prepared_binary_result=yes binary_parameters=no", connectString)
	}

	return connectString
}

// prettyPrintResponse prints a Query and its response in JSON format with two
// keys: 'query' which has a value of the SQL used to generate the second key
// 'results' which is an array of each row in the return set.
func prettyPrintResponse(rows *sql.Rows, q *query.TimescaleDB) {
	resp := make(map[string]interface{})
	resp["query"] = string(q.SqlQuery)
	resp["results"] = mapRows(rows)

	line, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		panic(err)
	}

	fmt.Println(string(line) + "\n")
}

func mapRows(r *sql.Rows) []map[string]interface{} {
	rows := []map[string]interface{}{}
	cols, _ := r.Columns()
	for r.Next() {
		row := make(map[string]interface{})
		values := make([]interface{}, len(cols))
		for i := range values {
			values[i] = new(interface{})
		}

		err := r.Scan(values...)
		if err != nil {
			panic(errors.Wrap(err, "error while reading values"))
		}

		for i, column := range cols {
			row[column] = *values[i].(*interface{})
		}
		rows = append(rows, row)
	}
	return rows
}

type queryExecutorOptions struct {
	showExplain   bool
	debug         bool
	printResponse bool
}

type processor struct {
	db   *sql.DB
	opts *queryExecutorOptions
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(workerNumber int) {
	db, err := sql.Open(driver, getConnectString(workerNumber))
	if err != nil {
		panic(err)
	}
	p.db = db
	p.opts = &queryExecutorOptions{
		showExplain:   showExplain,
		debug:         runner.DebugLevel() > 0,
		printResponse: runner.DoPrintResponses(),
	}
}

func (p *processor) ProcessQuery(q query.Query, isWarm bool) ([]*query.Stat, error) {
	// No need to run again for EXPLAIN
	if isWarm && p.opts.showExplain {
		return nil, nil
	}
	tq := q.(*query.TimescaleDB)

	start := time.Now()
	qry := string(tq.SqlQuery)
	if showExplain {
		qry = "EXPLAIN ANALYZE " + qry
	}
	rows, err := p.db.Query(qry)
	if err != nil {
		return nil, err
	}

	if p.opts.debug {
		fmt.Println(qry)
	}
	if showExplain {
		text := ""
		for rows.Next() {
			var s string
			if err2 := rows.Scan(&s); err2 != nil {
				panic(err2)
			}
			text += s + "\n"
		}
		fmt.Printf("%s\n\n%s\n-----\n\n", qry, text)
	} else if p.opts.printResponse {
		prettyPrintResponse(rows, tq)
	}
	// Fetching all the rows to confirm that the query is fully completed.
	for rows.Next() {
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	took := float64(time.Since(start).Nanoseconds()) / 1e6
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), took)

	return []*query.Stat{stat}, err
}
