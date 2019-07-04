package common

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

const testGeneratorScale = 100

var (
	dummyMeasurementName           = []byte("dummy")
	dummyFieldLabel                = []byte("label")
	dummyFieldValue                = []byte("value")
	dummyGeneratorMeasurementCount = 9
	testTime                       = time.Now()
	testBaseConf                   = &BaseSimulatorConfig{
		Start:                testTime,
		End:                  testTime.Add(3 * time.Second),
		InitGeneratorScale:   10,
		GeneratorScale:       testGeneratorScale,
		GeneratorConstructor: dummyGeneratorConstructor,
	}
)

type dummyMeasurement struct {
	*SubsystemMeasurement
}

func (m *dummyMeasurement) ToPoint(p *serialize.Point) {
	p.SetMeasurementName(dummyMeasurementName)

	p.AppendField(dummyFieldLabel, dummyFieldValue)
}

type dummyGenerator struct{}

func (d dummyGenerator) Measurements() []SimulatedMeasurement {
	sm := make([]SimulatedMeasurement, dummyGeneratorMeasurementCount)

	for i := range sm {
		sm[i] = &dummyMeasurement{}
	}

	return sm
}

func (d dummyGenerator) Tags() []Tag {
	tags := make([]Tag, 1)

	tags[0] = Tag{
		Key:   []byte("key"),
		Value: "value",
	}

	return tags
}

func (d dummyGenerator) TickAll(duration time.Duration) {
}

func dummyGeneratorConstructor(i int, start time.Time) Generator {
	return &dummyGenerator{}
}

func TestBaseSimulatorNext(t *testing.T) {
	s := testBaseConf.NewSimulator(time.Second, 0).(*BaseSimulator)
	// There are two epochs for the test configuration, and a difference of 90
	// from init to final, so each epoch should add 45 devices to be written.
	writtenIdx := []int{10, 55, 100}
	p := serialize.NewPoint()
	totalPerRun := testGeneratorScale * dummyGeneratorMeasurementCount

	runFn := func(run int) {
		for i := 0; i < totalPerRun; i++ {
			write := s.Next(p)
			generatorIdx := i % testGeneratorScale
			if got := int(s.generatorIndex); got != generatorIdx+1 {
				t.Errorf("run %d: generator index incorrect, i = %d: got %d want %d", run, i, got, i+1)
			}
			if generatorIdx < writtenIdx[run-1] && !write {
				t.Errorf("run %d: should write point at i = %d, but not", run, i)
			} else if generatorIdx >= writtenIdx[run-1] && write {
				t.Errorf("run %d: should not write point at i = %d, but am", run, i)
			}

			if got := int(s.epoch); got != run-1 {
				t.Errorf("run %d: epoch prematurely turned over", run)
			}
		}
	}

	// First run through:
	runFn(1)
	// Second run through, should wrap around and do hosts again
	runFn(2)
	// Final run through, should be all hosts:
	runFn(3)
}

func TestBaseSimulatorTagKeys(t *testing.T) {
	s := testBaseConf.NewSimulator(time.Second, 0).(*BaseSimulator)

	tagKeys := s.TagKeys()

	if got := len(tagKeys); got != 1 {
		t.Fatalf("tag key count incorrect, got %d want 1", got)
	}

	if got := string(tagKeys[0]); got != "key" {
		t.Errorf("tag key incorrect, got %s want key", got)
	}
}

func TestBaseSimulatorTagKeysPanic(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("did not panic when should")
		}
	}()

	s := BaseSimulator{}
	s.TagKeys()

	t.Fatalf("test should have stopped at this point")
}

func TestBaseSimulatorTagTypes(t *testing.T) {
	s := testBaseConf.NewSimulator(time.Second, 0).(*BaseSimulator)

	tagTypes := s.TagTypes()

	if got := len(tagTypes); got != 1 {
		t.Fatalf("tag key count incorrect, got %d want 1", got)
	}

	if got := tagTypes[0]; got != reflect.TypeOf("string") {
		t.Errorf("tag type incorrect, got %s want string", got)
	}
}

func TestBaseSimulatorTagTypesPanic(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("did not panic when should")
		}
	}()

	s := BaseSimulator{}
	s.TagTypes()

	t.Fatalf("test should have stopped at this point")
}

func TestBaseSimulatorFields(t *testing.T) {
	s := testBaseConf.NewSimulator(time.Second, 0).(*BaseSimulator)

	fields := s.Fields()

	if got := len(fields); got != 1 {
		t.Fatalf("fields count incorrect, got %d want 1", got)
	}

	got, ok := fields[string(dummyMeasurementName)]

	if !ok {
		t.Fatalf("field key not set, want %s", string(dummyMeasurementName))
	}

	if len(got) != 1 {
		t.Fatalf("field count incorrect, got %d want 1", len(got))
	}

	if string(got[0]) != string(dummyFieldLabel) {
		t.Errorf("unexpected field value, got %s want %s", string(got[0]), string(dummyFieldLabel))
	}
}

func TestBaseSimulatorFieldsPanic(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("did not panic when should")
		}
	}()

	s := BaseSimulator{}
	s.Fields()

	t.Fatalf("test should have stopped at this point")
}

func TestBaseSimulatorConfigNewSimulator(t *testing.T) {
	duration := time.Second
	start := time.Now()
	end := start.Add(10 * time.Second)
	numGenerators := uint64(100)
	initGenerators := uint64(0)
	conf := &BaseSimulatorConfig{
		Start:                start,
		End:                  end,
		InitGeneratorScale:   initGenerators,
		GeneratorScale:       numGenerators,
		GeneratorConstructor: dummyGeneratorConstructor,
	}
	cases := []uint64{0, 5, 10}

	for _, limit := range cases {
		t.Run(fmt.Sprintf("limit %d", limit), func(t *testing.T) {
			sim := conf.NewSimulator(duration, limit).(*BaseSimulator)
			if got := sim.madePoints; got != 0 {
				t.Errorf("incorrect initial points: got %d want %d", got, 0)
			}
			if got := sim.epoch; got != 0 {
				t.Errorf("incorrect initial epoch: got %d want %d", got, 0)
			}
			if got := sim.generatorIndex; got != 0 {
				t.Errorf("incorrect initial generator index: got %d want %d", got, 0)
			}
			if got := sim.simulatedMeasurementIndex; got != 0 {
				t.Errorf("incorrect simulated measurement index: got %d want %d", got, 0)
			}
			if got := sim.epochGenerators; got != initGenerators {
				t.Errorf("incorrect initial epoch generators: got %d want %d", got, initGenerators)
			}
			if got := sim.initGenerators; got != initGenerators {
				t.Errorf("incorrect initial init generators: got %d want %d", got, initGenerators)
			}
			if got := sim.timestampStart; got != start {
				t.Errorf("incorrect start time: got %v want %v", got, start)
			}
			if got := sim.timestampEnd; got != end {
				t.Errorf("incorrect end time: got %v want %v", got, end)
			}
			wantEpochs := uint64(10) // 10 seconds between start & end, interval is 1s, so 10 / 1 = 10
			if got := sim.epochs; got != wantEpochs {
				t.Errorf("incorrect epochs: got %d want %d", got, wantEpochs)
			}
			wantMaxPoints := wantEpochs * numGenerators * 9 // 9 measurements per dummy generator
			if limit != 0 {
				wantMaxPoints = limit
			}
			if got := sim.maxPoints; got != wantMaxPoints {
				t.Errorf("incorrect max points: got %d want %d", got, wantMaxPoints)
			}
		})
	}

}
