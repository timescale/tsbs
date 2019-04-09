package inputs

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/databases/cassandra"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/databases/clickhouse"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/databases/influx"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/databases/mongo"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/databases/siridb"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/databases/timescaledb"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/query"
)

func TestQueryGeneratorConfigValidate(t *testing.T) {
	c := &QueryGeneratorConfig{
		BaseConfig: BaseConfig{
			Seed:   123,
			Format: FormatTimescaleDB,
			Use:    useCaseDevops,
			Scale:  10,
		},
		QueryType:            "foo",
		InterleavedGroupID:   0,
		InterleavedNumGroups: 1,
	}

	// Test base validation
	err := c.Validate()
	if err != nil {
		t.Errorf("unexpected error for correct config: %v", err)
	}

	c.Format = "bad format"
	err = c.Validate()
	if err == nil {
		t.Errorf("unexpected lack of error for bad format")
	}
	c.Format = FormatTimescaleDB

	// Test QueryType validation
	c.QueryType = ""
	err = c.Validate()
	if err == nil {
		t.Errorf("unexpected lack of error for empty query type")
	}
	c.QueryType = "foo"

	// Test groups validation
	c.InterleavedNumGroups = 0
	err = c.Validate()
	if err == nil {
		t.Errorf("unexpected lack of error for 0 groups")
	} else if got := err.Error(); got != errTotalGroupsZero {
		t.Errorf("incorrect error for 0 groups: got\n%s\nwant\n%s", got, errTotalGroupsZero)
	}
	c.InterleavedNumGroups = 1

	c.InterleavedGroupID = 2
	err = c.Validate()
	if err == nil {
		t.Errorf("unexpected lack of error for group id > num groups")
	} else {
		want := fmt.Sprintf(errInvalidGroupsFmt, 2, 1)
		if got := err.Error(); got != want {
			t.Errorf("incorrect error for group id > num groups: got\n%s\nwant\n%s", got, want)
		}
	}
}

func TestNewQueryGenerator(t *testing.T) {
	m := map[string]map[string]utils.QueryFillerMaker{
		"foo": {
			"bar": nil,
			"baz": nil,
		},
		"bar": {
			"baz": nil,
		},
	}
	g := NewQueryGenerator(m)
	if !reflect.DeepEqual(g.useCaseMatrix, m) {
		t.Errorf("useCaseMatrix not properly set")
	}
}

func TestQueryGeneratorInit(t *testing.T) {
	const okQueryType = "single-groupby-1-1-1"
	g := &QueryGenerator{
		useCaseMatrix: map[string]map[string]utils.QueryFillerMaker{
			useCaseDevops: {
				okQueryType: nil,
			},
		},
	}
	// Test that empty config fails
	err := g.init(nil)
	if err == nil {
		t.Errorf("unexpected lack of error with empty config")
	} else if got := err.Error(); got != ErrNoConfig {
		t.Errorf("incorrect error: got\n%s\nwant\n%s", got, ErrNoConfig)
	}

	// Test that wrong type of config fails
	err = g.init(&BaseConfig{})
	if err == nil {
		t.Errorf("unexpected lack of error with invalid config")
	} else if got := err.Error(); got != ErrInvalidDataConfig {
		t.Errorf("incorrect error: got\n%s\nwant\n%s", got, ErrInvalidDataConfig)
	}

	// Test that empty, invalid config fails
	err = g.init(&QueryGeneratorConfig{})
	if err == nil {
		t.Errorf("unexpected lack of error with empty QueryGeneratorConfig")
	}

	c := &QueryGeneratorConfig{
		BaseConfig: BaseConfig{
			Format: FormatTimescaleDB,
			Use:    useCaseCPUOnly, // not in the useCaseMatrix
			Scale:  1,
		},
		QueryType:            "unknown query type",
		InterleavedNumGroups: 1,
	}

	// Test use case not in matrix
	err = g.init(c)
	want := fmt.Sprintf(errBadUseFmt, useCaseCPUOnly)
	if err == nil {
		t.Errorf("unexpected lack of error with bad use case")
	} else if got := err.Error(); got != want {
		t.Errorf("incorrect error for bad use case:\ngot\n%s\nwant\n%s", got, want)
	}
	c.Use = useCaseDevops

	// Test unknown query type
	err = g.init(c)
	want = fmt.Sprintf(errBadQueryTypeFmt, useCaseDevops, "unknown query type")
	if err == nil {
		t.Errorf("unexpected lack of error with bad query type")
	} else if got := err.Error(); got != want {
		t.Errorf("incorrect error for bad query type:\ngot\n%s\nwant\n%s", got, want)
	}
	c.QueryType = okQueryType

	const errTimePrefix = "cannot parse time from string"

	// Test incorrect time format for start
	c.TimeStart = "2006 Jan 2"
	err = g.init(c)
	if err == nil {
		t.Errorf("unexpected lack of error with bad start date")
	} else if got := err.Error(); !strings.HasPrefix(got, errTimePrefix) {
		t.Errorf("unexpected error for bad start date: got\n%s", got)
	}
	c.TimeStart = defaultTimeStart

	// Test incorrect time format for end
	c.TimeEnd = "Jan 3rd 2016"
	err = g.init(c)
	if err == nil {
		t.Errorf("unexpected lack of error with bad end date")
	} else if got := err.Error(); !strings.HasPrefix(got, errTimePrefix) {
		t.Errorf("unexpected error for bad end date: got\n%s", got)
	}
	c.TimeEnd = defaultTimeEnd

	// Test that Out is set to os.Stdout if unset
	err = g.init(c)
	if err != nil {
		t.Errorf("unexpected error when checking Out: got %v", err)
	} else if g.Out != os.Stdout {
		t.Errorf("Out not set to Stdout")
	}

	// Test that Out is same if set
	var buf bytes.Buffer
	g.Out = &buf
	err = g.init(c)
	if err != nil {
		t.Errorf("unexpected error when checking Out: got %v", err)
	} else if g.Out != &buf {
		t.Errorf("Out not set to explicit io.Writer")
	}

	// Test that DebugOut is set to os.Stderr if unset
	err = g.init(c)
	if err != nil {
		t.Errorf("unexpected error when checking DebugOut: got %v", err)
	} else if g.DebugOut != os.Stderr {
		t.Errorf("DebugOut not set to Stderr")
	}
}

func TestGetUseCaseGenerator(t *testing.T) {
	const scale = 10
	tsStart, _ := ParseUTCTime(defaultTimeStart)
	tsEnd, _ := ParseUTCTime(defaultTimeEnd)
	c := &QueryGeneratorConfig{
		BaseConfig: BaseConfig{
			Scale: scale,
		},
	}
	g := &QueryGenerator{
		config:  c,
		tsStart: tsStart,
		tsEnd:   tsEnd,
	}
	checkType := func(format string, want utils.DevopsGenerator) utils.DevopsGenerator {
		wantType := reflect.TypeOf(want)
		c.Format = format
		useGen, err := g.getUseCaseGenerator(c)
		if err != nil {
			t.Errorf("unexpected error with format '%s': %v", format, err)
		}
		if got := reflect.TypeOf(useGen); got != wantType {
			t.Errorf("format '%s' does not give right use case gen: got %v want %v", format, got, wantType)
		}

		return useGen
	}

	checkType(FormatCassandra, cassandra.NewDevops(tsStart, tsEnd, scale))
	checkType(FormatInflux, influx.NewDevops(tsStart, tsEnd, scale))
	checkType(FormatSiriDB, siridb.NewDevops(tsStart, tsEnd, scale))
	checkType(FormatMongo, mongo.NewDevops(tsStart, tsEnd, scale))
	c.MongoUseNaive = true
	checkType(FormatMongo, mongo.NewNaiveDevops(tsStart, tsEnd, scale))

	useGen := checkType(FormatClickhouse, clickhouse.NewDevops(tsStart, tsEnd, scale))
	if got := useGen.(*clickhouse.Devops).UseTags; got != c.ClickhouseUseTags {
		t.Errorf("clickhouse UseTags not set correctly: got %v want %v", got, c.ClickhouseUseTags)
	}

	c.ClickhouseUseTags = true
	useGen = checkType(FormatClickhouse, clickhouse.NewDevops(tsStart, tsEnd, scale))
	if got := useGen.(*clickhouse.Devops).UseTags; got != c.ClickhouseUseTags {
		t.Errorf("clickhouse UseTags not set correctly: got %v want %v", got, c.ClickhouseUseTags)
	}

	useGen = checkType(FormatTimescaleDB, timescaledb.NewDevops(tsStart, tsEnd, scale))
	if got := useGen.(*timescaledb.Devops).UseTags; got != c.TimescaleUseTags {
		t.Errorf("timescaledb UseTags not set correctly: got %v want %v", got, c.TimescaleUseTags)
	}
	if got := useGen.(*timescaledb.Devops).UseJSON; got != c.TimescaleUseJSON {
		t.Errorf("timescaledb UseJSON not set correctly: got %v want %v", got, c.TimescaleUseJSON)
	}
	if got := useGen.(*timescaledb.Devops).UseTimeBucket; got != c.TimescaleUseTimeBucket {
		t.Errorf("timescaledb UseTimeBucket not set correctly: got %v want %v", got, c.TimescaleUseTimeBucket)
	}

	c.TimescaleUseJSON = true
	c.TimescaleUseTags = true
	c.TimescaleUseTimeBucket = true
	useGen = checkType(FormatTimescaleDB, timescaledb.NewDevops(tsStart, tsEnd, scale))
	if got := useGen.(*timescaledb.Devops).UseTags; got != c.TimescaleUseTags {
		t.Errorf("timescaledb UseTags not set correctly: got %v want %v", got, c.TimescaleUseTags)
	}
	if got := useGen.(*timescaledb.Devops).UseJSON; got != c.TimescaleUseJSON {
		t.Errorf("timescaledb UseJSON not set correctly: got %v want %v", got, c.TimescaleUseJSON)
	}
	if got := useGen.(*timescaledb.Devops).UseTimeBucket; got != c.TimescaleUseTimeBucket {
		t.Errorf("timescaledb UseTimeBucket not set correctly: got %v want %v", got, c.TimescaleUseTimeBucket)
	}

	// Test error condition
	c.Format = "bad format"
	useGen, err := g.getUseCaseGenerator(c)
	if err == nil {
		t.Errorf("unexpected lack of error for bad format")
	} else if got := err.Error(); got != fmt.Sprintf(errUnknownFormatFmt, c.Format) {
		t.Errorf("incorrect error:\ngot\n%s\nwant\n%s", got, fmt.Sprintf(errUnknownFormatFmt, c.Format))
	} else if useGen != nil {
		t.Errorf("useGen was not nil")
	}
}

// Decoded previously
var wantQueries = []query.TimescaleDB{
	{
		Hypertable:       []byte("cpu"),
		HumanLabel:       []byte("TimescaleDB 1 cpu metric(s), random    1 hosts, random 1h0m0s by 1m"),
		HumanDescription: []byte("TimescaleDB 1 cpu metric(s), random    1 hosts, random 1h0m0s by 1m: 2016-01-01T02:17:08Z"),
		SqlQuery: []byte(`SELECT time_bucket('60 seconds', time) AS minute,
        max(usage_user) as max_usage_user
        FROM cpu
        WHERE tags_id IN (SELECT id FROM tags WHERE hostname IN ('host_9')) AND time >= '2016-01-01 02:17:08.646325 +0000' AND time < '2016-01-01 03:17:08.646325 +0000'
        GROUP BY minute ORDER BY minute ASC`),
	},
	{
		Hypertable:       []byte("cpu"),
		HumanLabel:       []byte("TimescaleDB 1 cpu metric(s), random    1 hosts, random 1h0m0s by 1m"),
		HumanDescription: []byte("TimescaleDB 1 cpu metric(s), random    1 hosts, random 1h0m0s by 1m: 2016-01-01T14:03:26Z"),
		SqlQuery: []byte(`SELECT time_bucket('60 seconds', time) AS minute,
        max(usage_user) as max_usage_user
        FROM cpu
        WHERE tags_id IN (SELECT id FROM tags WHERE hostname IN ('host_5')) AND time >= '2016-01-01 14:03:26.894865 +0000' AND time < '2016-01-01 15:03:26.894865 +0000'
        GROUP BY minute ORDER BY minute ASC`),
	},
	{
		Hypertable:       []byte("cpu"),
		HumanLabel:       []byte("TimescaleDB 1 cpu metric(s), random    1 hosts, random 1h0m0s by 1m"),
		HumanDescription: []byte("TimescaleDB 1 cpu metric(s), random    1 hosts, random 1h0m0s by 1m: 2016-01-01T09:11:43Z"),
		SqlQuery: []byte(`SELECT time_bucket('60 seconds', time) AS minute,
        max(usage_user) as max_usage_user
        FROM cpu
        WHERE tags_id IN (SELECT id FROM tags WHERE hostname IN ('host_9')) AND time >= '2016-01-01 09:11:43.311177 +0000' AND time < '2016-01-01 10:11:43.311177 +0000'
        GROUP BY minute ORDER BY minute ASC`),
	},
}

func getTestConfigAndGenerator() (*QueryGeneratorConfig, *QueryGenerator) {
	const scale = 10
	tsStart, _ := ParseUTCTime(defaultTimeStart)
	tsEnd, _ := ParseUTCTime(defaultTimeEnd)
	tsEnd = tsEnd.Add(time.Second)
	c := &QueryGeneratorConfig{
		BaseConfig: BaseConfig{
			Format:    FormatTimescaleDB,
			Use:       useCaseCPUOnly,
			Scale:     scale,
			TimeStart: defaultTimeStart,
			TimeEnd:   strings.Replace(defaultTimeEnd, ":00Z", ":01Z", 1),
			Limit:     3,
			Seed:      123,
		},
		QueryType:              "single-groupby-1-1-1",
		TimescaleUseTimeBucket: true,
		TimescaleUseTags:       true,
		InterleavedNumGroups:   1,
	}
	g := &QueryGenerator{
		useCaseMatrix: map[string]map[string]utils.QueryFillerMaker{
			useCaseCPUOnly: {
				"single-groupby-1-1-1": devops.NewSingleGroupby(1, 1, 1),
			},
		},
		config:   c,
		tsStart:  tsStart,
		tsEnd:    tsEnd,
		DebugOut: os.Stderr,
	}

	return c, g
}

func checkGeneratedOutput(t *testing.T, buf *bytes.Buffer) {
	r := bufio.NewReader(buf)
	decoder := gob.NewDecoder(r)
	i := 0
	for {
		var q query.TimescaleDB
		err := decoder.Decode(&q)
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("unexpected error while decoding: got %v", err)
		}
		want := string(wantQueries[i].SqlQuery)
		if got := string(q.SqlQuery); got != want {
			t.Errorf("incorrect query:\ngot\n%s\nwant\n%s", got, want)
		}
		i++
	}
	if i != len(wantQueries) {
		t.Errorf("incorrect number of queries: got %d want %d", i, len(wantQueries))
	}
}

func TestQueryGeneratorRunQueryGeneration(t *testing.T) {
	seedLine := "using random seed 123"
	summaryLine := "TimescaleDB 1 cpu metric(s), random    1 hosts, random 1h0m0s by 1m: 3 points"
	cases := []struct {
		level     int
		wantDebug []string
	}{
		{
			level:     0,
			wantDebug: []string{summaryLine},
		},
		{
			level: 1,
			wantDebug: []string{
				seedLine,
				string(wantQueries[0].HumanLabelName()),
				string(wantQueries[1].HumanLabelName()),
				string(wantQueries[2].HumanLabelName()),
				summaryLine,
			},
		},
		{
			level: 2,
			wantDebug: []string{
				seedLine,
				string(wantQueries[0].HumanDescriptionName()),
				string(wantQueries[1].HumanDescriptionName()),
				string(wantQueries[2].HumanDescriptionName()),
				summaryLine,
			},
		},
		{
			level: 3,
			wantDebug: []string{
				seedLine,
				wantQueries[0].String(),
				wantQueries[1].String(),
				wantQueries[2].String(),
				summaryLine,
			},
		},
	}

	for _, c := range cases {
		config, g := getTestConfigAndGenerator()
		config.Debug = c.level
		var buf bytes.Buffer
		g.bufOut = bufio.NewWriter(&buf)
		var debug bytes.Buffer
		g.DebugOut = &debug

		useGen, err := g.getUseCaseGenerator(config)
		if err != nil {
			t.Fatalf("could not get use case gen: %v", err)
		}
		filler := g.useCaseMatrix[config.Use][config.QueryType](useGen)

		err = g.runQueryGeneration(useGen, filler, config)
		if err != nil {
			t.Errorf("unexpected error: got %v", err)
		}

		checkGeneratedOutput(t, &buf)

		// Check that the proper debug output was written
		wantDebug := strings.TrimSpace(strings.Join(c.wantDebug, "\n"))
		if got := strings.TrimSpace(debug.String()); got != wantDebug {
			t.Errorf("incorrect line for debug level %d:\ngot\n%s\nwant\n%s", c.level, got, wantDebug)
		}
	}
}

type badWriter struct {
	when  int
	count int
}

func (w *badWriter) Write(p []byte) (int, error) {
	if w.count >= w.when {
		return 0, fmt.Errorf("error writing")
	}
	w.count++
	return len(p), nil
}

func TestQueryGeneratorRunQueryGenerationErrors(t *testing.T) {
	c, g := getTestConfigAndGenerator()
	var buf bytes.Buffer
	g.bufOut = bufio.NewWriter(&buf)

	useGen, err := g.getUseCaseGenerator(c)
	if err != nil {
		t.Fatalf("could not get use case gen: %v", err)
	}
	filler := g.useCaseMatrix[c.Use][c.QueryType](useGen)

	checkErr := func(want string) {
		err = g.runQueryGeneration(useGen, filler, c)
		if err == nil {
			t.Errorf("unexpected lack of error")
		} else if got := err.Error(); !strings.HasPrefix(got, want) {
			t.Errorf("incorrect error for output query stats:\ngot\n%s\nwant\n%s", got, want)
		}
	}

	// Test error condition when printing stats
	g.DebugOut = &badWriter{}
	want := fmt.Sprintf(errCouldNotQueryStatsFmt, "error writing")
	checkErr(want)

	// Test error condition when printing seed
	c.Debug = 1
	want = fmt.Sprintf(errCouldNotDebugFmt, "error writing")
	checkErr(want)

	// Test error condition inside loop; first debug is success
	g.DebugOut = &badWriter{when: 1}
	checkErr(want)

	g.DebugOut = &badWriter{when: 2}
	checkErr(want)

	// Test error on encoding
	g.DebugOut = &badWriter{when: 10000}
	g.bufOut = bufio.NewWriterSize(&badWriter{}, 8) // small buffer forces it to write to underlying
	want = fmt.Sprintf(errCouldNotEncodeQueryFmt, "error writing")
	checkErr(want)
}

func TestQueryGeneratorGenerate(t *testing.T) {
	g := &QueryGenerator{}

	// Test that an invalid config fails
	c := &QueryGeneratorConfig{}
	err := g.Generate(c)
	if err == nil {
		t.Errorf("unexpected lack of error with empty QueryGeneratorConfig")
	}

	c, g = getTestConfigAndGenerator()
	var buf bytes.Buffer
	g.Out = &buf
	g.DebugOut = ioutil.Discard
	err = g.Generate(c)
	if err != nil {
		t.Errorf("unexpected error when generating: got %v", err)
	}
	checkGeneratedOutput(t, &buf)
}
