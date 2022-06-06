package inputs

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/data/usecases"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
)

const (
	defaultTimeStart   = "2016-01-01T00:00:00Z"
	defaultTimeEnd     = "2016-01-02T00:00:00Z"
	defaultLogInterval = 10 * time.Second
)

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
	err = dg.init(&common.BaseConfig{})
	if err == nil {
		t.Errorf("unexpected lack of error with invalid config")
	} else if got := err.Error(); got != ErrInvalidDataConfig {
		t.Errorf("incorrect error: got\n%s\nwant\n%s", got, ErrInvalidDataConfig)
	}

	// Test that empty, invalid config fails
	err = dg.init(&common.DataGeneratorConfig{})
	if err == nil {
		t.Errorf("unexpected lack of error with empty DataGeneratorConfig")
	}

	c := &common.DataGeneratorConfig{
		BaseConfig: common.BaseConfig{
			Format: constants.FormatTimescaleDB,
			Use:    common.UseCaseDevops,
			Scale:  1,
		},
		LogInterval:          time.Second,
		InterleavedNumGroups: 1,
	}

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

func TestDataGeneratorGenerate(t *testing.T) {
	targetName := constants.FormatTimescaleDB
	dg := &DataGenerator{}
	mockTarget := &mockTarget{
		name: targetName,
	}
	// Test that an invalid config fails
	c := &common.DataGeneratorConfig{}
	err := dg.Generate(c, mockTarget)
	if err == nil {
		t.Errorf("unexpected lack of error with empty DataGeneratorConfig")
	}

	c = &common.DataGeneratorConfig{
		BaseConfig: common.BaseConfig{
			Seed:      123,
			Format:    targetName,
			Use:       common.UseCaseCPUOnly,
			Scale:     1,
			TimeStart: defaultTimeStart,
			TimeEnd:   defaultTimeEnd,
		},
		Limit:                3,
		InitialScale:         1,
		LogInterval:          time.Second,
		InterleavedNumGroups: 1,
	}
	var buf bytes.Buffer
	dg.Out = &buf
	mockSerializer := &mockSerializer{}
	mockTarget.serializer = mockSerializer
	err = dg.Generate(c, mockTarget)
	if err != nil {
		t.Errorf("unexpected error when generating: got %v", err)
	} else if len(mockSerializer.sentPoints) != int(c.Limit) {
		t.Errorf("unexpected number of points sent to serializer. expected %d, got %d", c.Limit, len(mockSerializer.sentPoints))
	}
}

var keyIteration = []byte("iteration")

type testSimulator struct {
	limit            uint64
	shouldWriteLimit uint64
	iteration        uint64
}

func (s *testSimulator) Headers() *common.GeneratedDataHeaders {
	return &common.GeneratedDataHeaders{
		TagTypes:  s.TagTypes(),
		TagKeys:   s.TagKeys(),
		FieldKeys: s.Fields(),
	}
}

func (s *testSimulator) Finished() bool {
	return s.iteration >= s.limit
}

func (s *testSimulator) Next(p *data.Point) bool {
	p.AppendField(keyIteration, s.iteration)
	ret := s.iteration < s.shouldWriteLimit
	s.iteration++
	return ret
}

func (s *testSimulator) Fields() map[string][]string {
	return nil
}

func (s *testSimulator) TagKeys() []string {
	return nil
}

func (s *testSimulator) TagTypes() []string {
	return nil
}

type testSerializer struct {
	shouldError bool
}

func (s *testSerializer) Serialize(p *data.Point, w io.Writer) error {
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
		dgc := &common.DataGeneratorConfig{
			BaseConfig: common.BaseConfig{
				Scale: 1,
			},
			Limit:                c.limit,
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

func TestGetSerializer(t *testing.T) {
	dgc := &common.DataGeneratorConfig{
		BaseConfig: common.BaseConfig{
			Use:       common.UseCaseCPUOnly,
			Scale:     1,
			TimeStart: defaultTimeStart,
			TimeEnd:   defaultTimeEnd,
		},
		InitialScale: 1,
		LogInterval:  defaultLogInterval,
	}
	g := &DataGenerator{
		config: dgc,
	}

	scfg, err := usecases.GetSimulatorConfig(dgc)
	if err != nil {
		t.Errorf("unexpected error creating scfg: %v", err)
	}

	sim := scfg.NewSimulator(dgc.LogInterval, 0)
	checkWriteHeader := func(format string, shouldWriteHeader bool) {
		var buf bytes.Buffer
		g.bufOut = bufio.NewWriter(&buf)
		serializer := &mockSerializer{}
		target := &mockTarget{
			name:       format,
			serializer: serializer,
		}
		s, err := g.getSerializer(sim, target)
		if err != nil {
			t.Errorf("unexpected error making serializer: %v", err)
		}
		if s.(*mockSerializer).numCalledSerialize > 0 {
			t.Errorf("expected Serialize function not to be called")
		}
		g.bufOut.Flush()
		if shouldWriteHeader && buf.Len() == 0 {
			t.Errorf("expected header to be written for format %s", format)
		} else if !shouldWriteHeader && buf.Len() > 0 {
			t.Errorf("unexpected header for format %s", format)
		}
	}

	checkWriteHeader(constants.FormatCassandra, false)
	checkWriteHeader(constants.FormatClickhouse, true)
	checkWriteHeader(constants.FormatInflux, false)
	checkWriteHeader(constants.FormatMongo, false)
	checkWriteHeader(constants.FormatSiriDB, false)
	checkWriteHeader(constants.FormatCrateDB, true)
	checkWriteHeader(constants.FormatPrometheus, false)
	checkWriteHeader(constants.FormatTimescaleDB, true)
	checkWriteHeader(constants.FormatVictoriaMetrics, false)
	checkWriteHeader(constants.FormatQuestDB, false)
}

type mockSerializer struct {
	numCalledSerialize int
	sentPoints         []*data.Point
}

func (m *mockSerializer) Serialize(p *data.Point, w io.Writer) error {
	m.numCalledSerialize++
	m.sentPoints = append(m.sentPoints, p)
	return nil
}

type mockTarget struct {
	name       string
	serializer serialize.PointSerializer
}

func (m *mockTarget) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	panic("implement me")
}

func (m *mockTarget) Serializer() serialize.PointSerializer {
	return m.serializer
}

func (m *mockTarget) TargetSpecificFlags(string, *pflag.FlagSet) {
	panic("implement me")
}

func (m *mockTarget) TargetName() string {
	return m.name
}
