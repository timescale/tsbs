// tsbs_run_queries_zigtsdb speed tests zigtsdb using requests from stdin or a file.
//
// It reads encoded Query objects from stdin and makes HTTP requests to the
// provided zigtsdb endpoint(s). A TSBS query may carry several ZQL
// statements (one per CPU field, because zigtsdb stores one value per
// series); they are executed sequentially on one keep-alive connection and
// the reported latency is the wall-clock time over all of them.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// Program option vars:
var (
	daemonURLs  []string
	fmtFull     bool
	consistency string
	maxRetries  int
)

// Global vars:
var (
	runner *query.BenchmarkRunner
)

// Parse args:
func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("urls", "http://localhost:8080",
		"Comma-separated list of zigtsdb node URLs, used in a round-robin fashion")
	pflag.Bool("fmt-full", false,
		"Append fmt=full so results carry full float precision (correctness checks)")
	pflag.String("consistency", "local",
		"Read consistency: local (default), linearizable or bounded")
	pflag.Int("max-retries", 60,
		"Retries per statement when the server answers 429 query_capacity")

	pflag.Parse()

	if err := utils.SetupConfigFile(); err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}
	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	urls := viper.GetString("urls")
	if len(urls) == 0 {
		log.Fatalf("missing `urls` flag")
	}
	daemonURLs = strings.Split(urls, ",")
	fmtFull = viper.GetBool("fmt-full")
	consistency = viper.GetString("consistency")
	maxRetries = viper.GetInt("max-retries")
	runner = query.NewBenchmarkRunner(config)
}

func main() {
	runner.Run(&query.HTTPPool, newProcessor)
}

func newProcessor() query.Processor {
	return &processor{}
}

type processor struct {
	url    string
	client *http.Client
	extra  string

	debug                int
	prettyPrintResponses bool
}

func (p *processor) Init(workerNum int) {
	p.url = strings.TrimSuffix(daemonURLs[workerNum%len(daemonURLs)], "/")
	p.client = &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 1024}}
	p.extra = extraParams(fmtFull, consistency)
	p.debug = runner.DebugLevel()
	p.prettyPrintResponses = runner.DoPrintResponses()
}

// extraParams builds the query-string suffix shared by every statement.
func extraParams(full bool, cons string) string {
	var b strings.Builder
	if full {
		b.WriteString("&fmt=full")
	}
	if cons != "" && cons != "local" {
		b.WriteString("&consistency=")
		b.WriteString(url.QueryEscape(cons))
	}
	return b.String()
}

// statementPaths expands a query into the request paths to execute: the
// first statement is already encoded in Path, the remaining raw statements
// (newline-separated in Body) are encoded here.
func statementPaths(q *query.HTTP, extra string) []string {
	paths := []string{string(q.Path) + extra}
	if len(q.Body) == 0 {
		return paths
	}
	for _, stmt := range bytes.Split(q.Body, []byte("\n")) {
		if len(stmt) == 0 {
			continue
		}
		paths = append(paths, "/query?zql="+encodeZQL(string(stmt))+extra)
	}
	return paths
}

// encodeZQL percent-encodes a statement; spaces become %20 rather than '+'
// so any RFC 3986 decoder reads it back correctly.
func encodeZQL(s string) string {
	return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
}

func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	hq := q.(*query.HTTP)
	lag, err := p.do(hq)
	if err != nil {
		return nil, err
	}
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), lag)
	return []*query.Stat{stat}, nil
}

type errorResponse struct {
	Error      string  `json:"error"`
	RetryAfter float64 `json:"retry_after_seconds"`
}

func (p *processor) do(q *query.HTTP) (float64, error) {
	paths := statementPaths(q, p.extra)
	start := time.Now()
	var bodies [][]byte
	for _, path := range paths {
		body, err := p.exec(path)
		if err != nil {
			return 0, fmt.Errorf("%s: %s", q.HumanLabel, err)
		}
		bodies = append(bodies, body)
	}
	lag := float64(time.Since(start).Nanoseconds()) / 1e6 // milliseconds

	switch p.debug {
	case 1:
		fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms\n", q.HumanLabel, lag)
	case 2:
		fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms -- %s\n", q.HumanLabel, lag, q.HumanDescription)
	case 3:
		fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms -- %s\n", q.HumanLabel, lag, q.HumanDescription)
		fmt.Fprintf(os.Stderr, "debug:   request: %s\n", q.RawQuery)
	case 4:
		fmt.Fprintf(os.Stderr, "debug: %s in %7.2fms -- %s\n", q.HumanLabel, lag, q.HumanDescription)
		fmt.Fprintf(os.Stderr, "debug:   request: %s\n", q.RawQuery)
		for _, b := range bodies {
			fmt.Fprintf(os.Stderr, "debug:   response: %s\n", b)
		}
	}

	if p.prettyPrintResponses {
		prefix := fmt.Sprintf("ID %d: ", q.GetID())
		for i, b := range bodies {
			var pretty bytes.Buffer
			if err := json.Indent(&pretty, b, prefix, "  "); err != nil {
				return lag, err
			}
			fmt.Fprintf(os.Stderr, "%s[%d/%d] %s\n", prefix, i+1, len(bodies), pretty.Bytes())
		}
	}
	return lag, nil
}

// checkResultBody rejects anything that is not a successful, finite result.
// Some zigtsdb versions answer an unknown aggregate (e.g. last() before
// 1.2.0) with status ok and NaN values, which must not count as a result.
func checkResultBody(body []byte) error {
	if !bytes.Contains(body, []byte(`"status":"ok"`)) {
		return fmt.Errorf("query did not succeed: %s", truncate(body, 300))
	}
	if bytes.Contains(body, []byte(":nan")) || bytes.Contains(body, []byte(":-nan")) {
		return fmt.Errorf("query returned NaN values (aggregate unsupported by this server version?): %s", truncate(body, 300))
	}
	return nil
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}

// exec runs one statement, retrying on the server's query-capacity valve.
func (p *processor) exec(path string) ([]byte, error) {
	for attempt := 0; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequest(http.MethodGet, p.url+path, nil)
		if err != nil {
			return nil, fmt.Errorf("error while creating request: %s", err)
		}
		resp, err := p.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("query execution error: %s", err)
		}
		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("error while reading response body: %s", err)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			var er errorResponse
			_ = json.Unmarshal(body, &er)
			wait := time.Duration(er.RetryAfter * float64(time.Second))
			if wait <= 0 {
				wait = time.Second
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("non-200 status code received: %d; body: %s", resp.StatusCode, body)
		}
		if err := checkResultBody(body); err != nil {
			return nil, err
		}
		return body, nil
	}
	return nil, fmt.Errorf("server kept answering 429 after %d retries", maxRetries)
}
