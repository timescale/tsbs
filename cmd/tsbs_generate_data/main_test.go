package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

const (
	correctTimeStr   = "2016-01-01T00:00:00Z"
	incorrectTimeStr = "2017-01-01"
)

var correctTime = time.Date(2016, time.January, 1, 0, 0, 0, 0, time.UTC)

func TestParseTimeFromStrong(t *testing.T) {
	parsedTime := parseTimeFromString(correctTimeStr)
	if parsedTime != correctTime {
		t.Errorf("did not get correct time back: got %v want %v", parsedTime, correctTime)
	}

	oldFatal := fatal
	fatalCalled := false
	fatal = func(format string, args ...interface{}) {
		fatalCalled = true
	}
	_ = parseTimeFromString(incorrectTimeStr)
	if !fatalCalled {
		t.Errorf("fatal not called when it should have been")
	}
	fatal = oldFatal
}

func TestValidateGroups(t *testing.T) {
	cases := []struct {
		desc        string
		groupID     uint
		totalGroups uint
		shouldErr   bool
		errFmt      string
	}{
		{
			desc:        "id < total, no err",
			groupID:     0,
			totalGroups: 1,
			shouldErr:   false,
		},
		{
			desc:        "id = total, should err",
			groupID:     1,
			totalGroups: 1,
			shouldErr:   true,
			errFmt:      errInvalidGroupsFmt,
		},
		{
			desc:        "total = 0, should err",
			groupID:     0,
			totalGroups: 0,
			shouldErr:   true,
			errFmt:      errTotalGroupsZero,
		},
	}
	for _, c := range cases {
		ok, err := validateGroups(c.groupID, c.totalGroups)
		if ok != c.shouldErr {
			magic := 46 // first 45 chars are the same for both error messages, so check up to 46 to make sure its different
			if c.shouldErr && err.Error()[:magic] != c.errFmt[:magic] {
				t.Errorf("%s: did not get correct error: got\n%v\nwant\n%v\n", c.desc, err, c.errFmt)
			}
			if !c.shouldErr && err != nil {
				t.Errorf("%s: got unexpected error: %v", c.desc, err)
			}
		}
	}
}

func TestValidateFormat(t *testing.T) {
	for _, f := range formatChoices {
		if !validateFormat(f) {
			t.Errorf("format '%s' did not return true when it should", f)
		}
	}
	if validateFormat("incorrect format!") {
		t.Errorf("validateFormat returned true for invalid format")
	}
}

func TestValidateUseCase(t *testing.T) {
	for _, f := range useCaseChoices {
		if !validateUseCase(f) {
			t.Errorf("use-case '%s' did not return true when it should", f)
		}
	}
	if validateUseCase("incorrect use-case!") {
		t.Errorf("validateUseCase returned true for invalid use-case")
	}
}

func TestPostFlagsParse(t *testing.T) {
	scale = 100
	timestampStart = time.Time{}
	timestampEnd = time.Time{}
	boringPFV := parseableFlagVars{
		initialScale:      1,
		seed:              123,
		timestampStartStr: correctTimeStr,
		timestampEndStr:   correctTimeStr,
	}
	postFlagParse(boringPFV)
	if initialScale != boringPFV.initialScale {
		t.Errorf("specified initScale not set correctly: got %d", initialScale)
	}
	if seed != boringPFV.seed {
		t.Errorf("specified seed not set correctly: got %d", seed)
	}
	if timestampStart != correctTime {
		t.Errorf("start time not parsed correctly: got %v", timestampStart)
	}
	if timestampEnd != correctTime {
		t.Errorf("end time not parsed correctly: got %v", timestampEnd)
	}

	// initScale should set to the same as scale
	testPFV := parseableFlagVars{
		initialScale:      0,
		seed:              boringPFV.seed,
		timestampStartStr: boringPFV.timestampStartStr,
		timestampEndStr:   boringPFV.timestampEndStr,
	}
	postFlagParse(testPFV)
	if initialScale != scale {
		t.Errorf("initScale = 0 not parsed correctly: got %d", initialScale)
	}

	// seed should set to current time
	testPFV = parseableFlagVars{
		initialScale:      boringPFV.initialScale,
		seed:              0,
		timestampStartStr: boringPFV.timestampStartStr,
		timestampEndStr:   boringPFV.timestampEndStr,
	}
	postFlagParse(testPFV)
	if seed == boringPFV.seed || seed == 0 {
		t.Errorf("seed = 0 not parsed correctly: got %d", seed)
	}

	// check that incorrect times fail
	oldFatal := fatal
	fatalCalled := false
	fatal = func(format string, args ...interface{}) {
		fatalCalled = true
	}
	testPFV = parseableFlagVars{
		initialScale:      boringPFV.initialScale,
		seed:              boringPFV.seed,
		timestampStartStr: incorrectTimeStr,
		timestampEndStr:   boringPFV.timestampEndStr,
	}
	postFlagParse(testPFV)
	if !fatalCalled {
		t.Errorf("fatal not called when it should have been")
	}

	testPFV = parseableFlagVars{
		initialScale:      boringPFV.initialScale,
		seed:              boringPFV.seed,
		timestampStartStr: boringPFV.timestampStartStr,
		timestampEndStr:   incorrectTimeStr,
	}
	postFlagParse(testPFV)
	if !fatalCalled {
		t.Errorf("fatal not called when it should have been")
	}
	fatal = oldFatal
}

var keyIteration = []byte("iteration")

type testSimulator struct {
	limit            uint64
	shouldWriteLimit uint64
	iteration        uint64
}

func (s *testSimulator) Finished() bool {
	return s.iteration >= s.limit
}

func (s *testSimulator) Next(p *serialize.Point) bool {
	p.AppendField(keyIteration, s.iteration)
	ret := s.iteration < s.shouldWriteLimit
	s.iteration++
	return ret
}

func (s *testSimulator) Fields() map[string][][]byte {
	return nil
}

func (s *testSimulator) TagKeys() [][]byte {
	return nil
}

type testSerializer struct {
	shouldError bool
}

func (s *testSerializer) Serialize(p *serialize.Point, w io.Writer) error {
	if s.shouldError {
		return fmt.Errorf("erroring")
	}
	w.Write(keyIteration)
	w.Write([]byte("="))
	str := fmt.Sprintf("%d", p.GetFieldValue(keyIteration).(uint64))
	w.Write([]byte(str))
	w.Write([]byte("\n"))
	return nil
}

func TestRunSimulator(t *testing.T) {
	cases := []struct {
		desc             string
		limit            uint64
		shouldWriteLimit uint64
		groupID          uint
		totalGroups      uint
		shouldError      bool
		wantPoints       uint
	}{
		{
			desc:             "shouldWriteLimit = limit",
			limit:            10,
			shouldWriteLimit: 10,
			totalGroups:      1,
			wantPoints:       10,
		},
		{
			desc:             "shouldWriteLimit < limit",
			limit:            10,
			shouldWriteLimit: 5,
			totalGroups:      1,
			wantPoints:       5,
		},
		{
			desc:             "shouldWriteLimit > limit",
			limit:            10,
			shouldWriteLimit: 15,
			totalGroups:      1,
			wantPoints:       10,
		},
		{
			desc:             "shouldWriteLimit = limit, totalGroups=2",
			limit:            10,
			shouldWriteLimit: 10,
			totalGroups:      2,
			wantPoints:       5,
		},
		{
			desc:             "shouldWriteLimit < limit, totalGroups=2",
			limit:            10,
			shouldWriteLimit: 6,
			totalGroups:      2,
			wantPoints:       3,
		},
		{
			desc:             "shouldWriteLimit < limit, totalGroups=2, other half",
			limit:            10,
			shouldWriteLimit: 6,
			groupID:          1,
			totalGroups:      2,
			wantPoints:       3,
		},
		{
			desc:             "should error in serializer",
			shouldError:      true,
			limit:            10,
			totalGroups:      1,
			shouldWriteLimit: 10,
		},
	}
	oldFatal := fatal
	for _, c := range cases {
		fatalCalled := false
		if c.shouldError {
			fatal = func(format string, args ...interface{}) {
				fatalCalled = true
			}
		}
		var buf bytes.Buffer
		sim := &testSimulator{
			limit:            c.limit,
			shouldWriteLimit: c.shouldWriteLimit,
		}
		serializer := &testSerializer{shouldError: c.shouldError}

		runSimulator(sim, serializer, &buf, c.groupID, c.totalGroups)
		if c.shouldError && !fatalCalled {
			t.Errorf("%s: did not fatal when should", c.desc)
		} else if !c.shouldError {
			scanner := bufio.NewScanner(bytes.NewReader(buf.Bytes()))
			lines := uint(0)
			for {
				ok := scanner.Scan()
				if !ok && scanner.Err() != nil {
					t.Fatal(scanner.Err())
				} else if !ok {
					break
				}
				line := scanner.Text()
				want := fmt.Sprintf("iteration=%d", (lines*c.totalGroups)+c.groupID)
				if line != want {
					t.Errorf("%s: incorrect line: got\n%s\nwant\n%s\n", c.desc, line, want)
				}
				lines++
			}
			if lines != c.wantPoints {
				t.Errorf("%s: incorrect number of points: got %d want %d", c.desc, lines, c.wantPoints)
			}
		}
	}
	fatal = oldFatal
}

func TestGetConfig(t *testing.T) {
	cfg := getConfig(useCaseDevops)
	switch got := cfg.(type) {
	case *devops.DevopsSimulatorConfig:
	default:
		t.Errorf("use case '%s' does not run the right type: got %T", useCaseDevops, got)
	}

	cfg = getConfig(useCaseCPUOnly)
	switch got := cfg.(type) {
	case *devops.CPUOnlySimulatorConfig:
	default:
		t.Errorf("use case '%s' does not run the right type: got %T", useCaseDevops, got)
	}

	cfg = getConfig(useCaseCPUSingle)
	switch got := cfg.(type) {
	case *devops.CPUOnlySimulatorConfig:
	default:
		t.Errorf("use case '%s' does not run the right type: got %T", useCaseDevops, got)
	}

	oldFatal := fatal
	fatalCalled := false
	fatal = func(f string, args ...interface{}) {
		fatalCalled = true
	}
	cfg = getConfig("bogus config")
	if !fatalCalled {
		t.Errorf("fatal not called on bogus use case")
	}
	if cfg != nil {
		t.Errorf("got a non-nil config for bogus use case: got %T", cfg)
	}
	fatal = oldFatal
}

func TestGetSerializer(t *testing.T) {
	cfg := getConfig(useCaseCPUOnly)
	sim := cfg.NewSimulator(logInterval, 0)
	buf := bytes.NewBuffer(make([]byte, 1024))
	out := bufio.NewWriter(buf)
	defer out.Flush()

	s := getSerializer(sim, formatCassandra, out)
	switch got := s.(type) {
	case *serialize.CassandraSerializer:
	default:
		t.Errorf("format '%s' does not run the right serializer: got %T", formatCassandra, got)
	}

	s = getSerializer(sim, formatInflux, out)
	switch got := s.(type) {
	case *serialize.InfluxSerializer:
	default:
		t.Errorf("format '%s' does not run the right serializer: got %T", formatInflux, got)
	}

	s = getSerializer(sim, formatMongo, out)
	switch got := s.(type) {
	case *serialize.MongoSerializer:
	default:
		t.Errorf("format '%s' does not run the right serializer: got %T", formatMongo, got)
	}

	s = getSerializer(sim, formatTimescaleDB, out)
	switch got := s.(type) {
	case *serialize.TimescaleDBSerializer:
	default:
		t.Errorf("format '%s' does not run the right serializer: got %T", formatTimescaleDB, got)
	}

	oldFatal := fatal
	fatalCalled := false
	fatal = func(f string, args ...interface{}) {
		fatalCalled = true
	}
	s = getSerializer(sim, "bogus format", out)
	if !fatalCalled {
		t.Errorf("fatal not called on bogus format")
	}
	if s != nil {
		t.Errorf("got a non-nil config for bogus format: got %T", cfg)
	}
	fatal = oldFatal
}
