package inputs

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

func TestDataGeneratorConfigValidate(t *testing.T) {
	c := &DataGeneratorConfig{
		BaseConfig: BaseConfig{
			Seed:   123,
			Format: FormatTimescaleDB,
			Use:    useCaseDevops,
			Scale:  10,
		},
		LogInterval:          time.Second,
		InitialScale:         0,
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

	// Test InitialScale validation
	c.InitialScale = 0
	err = c.Validate()
	if err != nil {
		t.Errorf("unexpected error for InitialScale of 0: %v", err)
	}
	if c.InitialScale != c.Scale {
		t.Errorf("InitialScale not set correctly for 0: got %d want %d", c.InitialScale, c.Scale)
	}

	c.InitialScale = 5
	err = c.Validate()
	if err != nil {
		t.Errorf("unexpected error for InitialScale of 5: %v", err)
	}
	if c.InitialScale != 5 {
		t.Errorf("InitialScale not set correctly for 0: got %d want %d", c.InitialScale, 5)
	}

	// Test LogInterval validation
	c.LogInterval = 0
	err = c.Validate()
	if err == nil {
		t.Errorf("unexpected lack of error for 0 log interval")
	} else if got := err.Error(); got != errLogIntervalZero {
		t.Errorf("incorrect error for 0 log interval: got\n%s\nwant\n%s", got, errLogIntervalZero)
	}
	c.LogInterval = time.Second

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

func TestDataGeneratorInit(t *testing.T) {
	// Test that empty config fails
	dg := &DataGenerator{}
	err := dg.init(nil)
	if err == nil {
		t.Errorf("unexpected lack of error with empty config")
	} else if got := err.Error(); got != ErrNoConfig {
		t.Errorf("incorrect error: got\n%s\nwant\n%s", got, ErrNoConfig)
	}

	// Test that wrong type of config fails
	err = dg.init(&BaseConfig{})
	if err == nil {
		t.Errorf("unexpected lack of error with invalid config")
	} else if got := err.Error(); got != ErrInvalidDataConfig {
		t.Errorf("incorrect error: got\n%s\nwant\n%s", got, ErrInvalidDataConfig)
	}

	// Test that empty, invalid config fails
	err = dg.init(&DataGeneratorConfig{})
	if err == nil {
		t.Errorf("unexpected lack of error with empty DataGeneratorConfig")
	}

	c := &DataGeneratorConfig{
		BaseConfig: BaseConfig{
			Format: FormatTimescaleDB,
			Use:    useCaseDevops,
			Scale:  1,
		},
		LogInterval:          time.Second,
		InterleavedNumGroups: 1,
	}
	const errTimePrefix = "cannot parse time from string"

	// Test incorrect time format for start
	c.TimeStart = "2006 Jan 2"
	err = dg.init(c)
	if err == nil {
		t.Errorf("unexpected lack of error with bad start date")
	} else if got := err.Error(); !strings.HasPrefix(got, errTimePrefix) {
		t.Errorf("unexpected error for bad start date: got\n%s", got)
	}
	c.TimeStart = defaultTimeStart

	// Test incorrect time format for end
	c.TimeEnd = "Jan 3rd 2016"
	err = dg.init(c)
	if err == nil {
		t.Errorf("unexpected lack of error with bad end date")
	} else if got := err.Error(); !strings.HasPrefix(got, errTimePrefix) {
		t.Errorf("unexpected error for bad end date: got\n%s", got)
	}
	c.TimeEnd = defaultTimeEnd

	// Test that Out is set to os.Stdout if unset
	err = dg.init(c)
	if err != nil {
		t.Errorf("unexpected error when checking Out: got %v", err)
	} else if dg.Out != os.Stdout {
		t.Errorf("Out not set to Stdout")
	}

	// Test that Out is same if set
	var buf bytes.Buffer
	dg.Out = &buf
	err = dg.init(c)
	if err != nil {
		t.Errorf("unexpected error when checking Out: got %v", err)
	} else if dg.Out != &buf {
		t.Errorf("Out not set to explicit io.Writer")
	}
}

const correctData = `tags,hostname,region,datacenter,rack,os,arch,team,service,service_version,service_environment
cpu,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice

tags,hostname=host_0,region=eu-central-1,datacenter=eu-central-1a,rack=6,os=Ubuntu15.10,arch=x86,team=SF,service=19,service_version=1,service_environment=test
cpu,1451606400000000000,58,2,24,61,22,63,6,44,80,38
tags,hostname=host_0,region=eu-central-1,datacenter=eu-central-1a,rack=6,os=Ubuntu15.10,arch=x86,team=SF,service=19,service_version=1,service_environment=test
cpu,1451606401000000000,57,3,23,60,23,64,5,44,76,36
tags,hostname=host_0,region=eu-central-1,datacenter=eu-central-1a,rack=6,os=Ubuntu15.10,arch=x86,team=SF,service=19,service_version=1,service_environment=test
cpu,1451606402000000000,58,2,25,62,23,65,5,45,78,36
`

func TestDataGeneratorGenerate(t *testing.T) {
	dg := &DataGenerator{}

	// Test that an invalid config fails
	c := &DataGeneratorConfig{}
	err := dg.Generate(c)
	if err == nil {
		t.Errorf("unexpected lack of error with empty DataGeneratorConfig")
	}

	c = &DataGeneratorConfig{
		BaseConfig: BaseConfig{
			Seed:      123,
			Limit:     3,
			Format:    FormatTimescaleDB,
			Use:       useCaseCPUOnly,
			Scale:     1,
			TimeStart: defaultTimeStart,
			TimeEnd:   defaultTimeEnd,
		},
		InitialScale:         1,
		LogInterval:          time.Second,
		InterleavedNumGroups: 1,
	}
	var buf bytes.Buffer
	dg.Out = &buf
	err = dg.Generate(c)
	if err != nil {
		t.Errorf("unexpected error when generating: got %v", err)
	} else if got := string(buf.Bytes()); got != correctData {
		t.Errorf("incorrect data written:\ngot\n%s\nwant\n%s", got, correctData)
	}

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
	for _, c := range cases {
		var buf bytes.Buffer
		dgc := &DataGeneratorConfig{
			BaseConfig: BaseConfig{
				Scale: 1,
				Limit: c.limit,
			},
			InitialScale:         1,
			LogInterval:          defaultLogInterval,
			InterleavedGroupID:   c.groupID,
			InterleavedNumGroups: c.totalGroups,
		}
		g := &DataGenerator{
			config: dgc,
			bufOut: bufio.NewWriter(&buf),
		}
		sim := &testSimulator{
			limit:            c.limit,
			shouldWriteLimit: c.shouldWriteLimit,
		}
		serializer := &testSerializer{shouldError: c.shouldError}

		err := g.runSimulator(sim, serializer, dgc)
		if c.shouldError && err == nil {
			t.Errorf("%s: unexpected lack of error", c.desc)
		} else if !c.shouldError && err != nil {
			t.Errorf("%s: unexpected error: got %v", c.desc, err)
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
}

func TestGetSimulatorConfig(t *testing.T) {
	dgc := &DataGeneratorConfig{
		BaseConfig: BaseConfig{
			Scale: 1,
		},
		InitialScale: 1,
		LogInterval:  defaultLogInterval,
	}
	g := &DataGenerator{config: dgc}

	checkType := func(use string, want common.SimulatorConfig) {
		wantType := reflect.TypeOf(want)
		dgc.Use = use
		scfg, err := g.getSimulatorConfig(dgc)
		if err != nil {
			t.Errorf("unexpected error with use case %s: %v", use, err)
		}
		if got := reflect.TypeOf(scfg); got != wantType {
			t.Errorf("use '%s' does not give right scfg: got %v want %v", use, got, wantType)
		}
	}

	checkType(useCaseDevops, &devops.DevopsSimulatorConfig{})
	checkType(useCaseCPUOnly, &devops.CPUOnlySimulatorConfig{})
	checkType(useCaseCPUSingle, &devops.CPUOnlySimulatorConfig{})

	dgc.Use = "bogus use case"
	_, err := g.getSimulatorConfig(dgc)
	if err == nil {
		t.Errorf("unexpected lack of error for bogus use case")
	}
}

func TestGetSerializer(t *testing.T) {
	dgc := &DataGeneratorConfig{
		BaseConfig: BaseConfig{
			Use:   useCaseCPUOnly,
			Scale: 1,
		},
		InitialScale: 1,
		LogInterval:  defaultLogInterval,
	}
	g := &DataGenerator{
		config: dgc,
	}

	scfg, err := g.getSimulatorConfig(dgc)
	if err != nil {
		t.Errorf("unexpected error creating scfg: %v", err)
	}

	sim := scfg.NewSimulator(dgc.LogInterval, 0)
	var buf bytes.Buffer
	g.bufOut = bufio.NewWriter(&buf)
	defer g.bufOut.Flush()

	checkType := func(format string, want serialize.PointSerializer) {
		wantType := reflect.TypeOf(want)
		s, err := g.getSerializer(sim, format)
		if err != nil {
			t.Errorf("unexpected error making serializer: %v", err)
		}
		if got := reflect.TypeOf(s); got != wantType {
			t.Errorf("format '%s' does not run the right serializer: got %v want %v", format, got, wantType)
		}
	}

	checkType(FormatCassandra, &serialize.CassandraSerializer{})
	checkType(FormatClickhouse, &serialize.TimescaleDBSerializer{})
	checkType(FormatInflux, &serialize.InfluxSerializer{})
	checkType(FormatMongo, &serialize.MongoSerializer{})
	checkType(FormatSiriDB, &serialize.SiriDBSerializer{})
	checkType(FormatClickhouse, &serialize.TimescaleDBSerializer{})
	checkType(FormatCrateDB, &serialize.CrateDBSerializer{})

	_, err = g.getSerializer(sim, "bogus format")
	if err == nil {
		t.Errorf("unexpected lack of error creating bogus serializer")
	}
}
