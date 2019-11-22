package siemens

import (
	"os"
	"reflect"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

type SimulatorConfig struct {
	// Start is the beginning time for the Simulator
	Start time.Time
	// End is the ending time for the Simulator
	End time.Time
	// InitGeneratorScale is the number of Generators to start with in the first reporting period
	InitGeneratorScale uint64
	// GeneratorScale is the total number of Generators to have in the last reporting period
	GeneratorScale uint64

	InFile       *os.File
	shift        int
	OutliersFreq float64
}

func (sc *SimulatorConfig) NewSimulator(interval time.Duration, limit uint64) common.Simulator {
	epochs := sc.calculateEpochs(interval)
	maxPoints := epochs * sc.GeneratorScale
	generators := make([]common.Generator, sc.GeneratorScale)
	for i := 0; i < len(generators); i++ {
		generators[i] = sc.GeneratorConstructor(i, sc.Start)
	}
	return &Simulator{
		shift:          sc.shift,
		outliersFreq:   sc.OutliersFreq,
		timestampStart: sc.Start,
		timestampEnd:   sc.End,
		interval:       interval,
		maxPoints:      maxPoints,
		generators:     generators,
	}
}

func (sc SimulatorConfig) GeneratorConstructor(i int, start time.Time) common.Generator{
	return NewSiemensGenerator(i, start, sc.InFile, sc.OutliersFreq)
}

func (sc SimulatorConfig) calculateEpochs(interval time.Duration) uint64 {
	return uint64(sc.End.Sub(sc.Start).Nanoseconds() / interval.Nanoseconds())
}

type Simulator struct {
	source string
	sensorPrefix string

	timestampStart time.Time
	timestampEnd   time.Time
	interval       time.Duration

	shift        int
	outliersFreq float64
	maxPoints    uint64

	madePoints uint64

	generatorIndex uint64
	generators []common.Generator
}

// Fields returns the fields of an entry.
func (s *Simulator) Fields() map[string][][]byte {
	if len(s.generators) <= 0 {
		panic("cannot get fields because no Generators added")
	}

	data := make(map[string][][]byte, len(s.generators))
	for _, sm := range s.generators[0].Measurements() {
		point := serialize.NewPoint()
		sm.ToPoint(point)
		data[string(point.MeasurementName())] = point.FieldKeys()
	}

	return data
}

// TagKeys returns all the tag keys for the device.
func (s *Simulator) TagKeys() [][]byte {
	if len(s.generators) <= 0 {
		panic("cannot get tag keys because no Generators added")
	}

	tags := s.generators[0].Tags()
	data := make([][]byte, len(tags))
	for i, tag := range tags {
		data[i] = tag.Key
	}

	return data
}

// TagTypes returns the type for each tag, extracted from the generated values.
func (s *Simulator) TagTypes() []reflect.Type {
	if len(s.generators) <= 0 {
		panic("cannot get tag types because no Generators added")
	}

	tags := s.generators[0].Tags()
	data := make([]reflect.Type, len(tags))
	for i, tag := range tags {
		data[i] = reflect.TypeOf(tag.Value)
	}

	return data
}

// Finished checks if the simulator is done.
func (s *Simulator) Finished() bool {
	return s.madePoints >= s.maxPoints
}

// Next populates the serialize.Point with the next entry from the batch.
// If the current pregenerated batch is empty, it tries to generate a new one
// in order to populate the next entry.
//func (s *Simulator) Next(p *serialize.Point) bool {
//	if s.batchSize == 0 {
//		return s.base.Next(p)
//	}
//
//	if len(s.currBatch) > 0 || s.simulateNextBatch() {
//		p.Copy(s.currBatch[0])
//		s.currBatch = s.currBatch[1:]
//		return true
//	}
//
//	return false
//}

func (s *Simulator) Next(p *serialize.Point) bool {
	if s.generatorIndex == uint64(len(s.generators)) {
		s.generatorIndex = 0
	}

	for i := 0; i < len(s.generators); i++ {
		s.generators[i].TickAll(s.interval)
	}

	generator := s.generators[s.generatorIndex]

	// Populate the Generator tags.
	for _, tag := range generator.Tags() {
		p.AppendTag(tag.Key, tag.Value)
	}

	// Populate measurement-specific tags and fields:
	generator.Measurements()[0].ToPoint(p)

	//ret := s.generatorIndex < s.epochGenerators
	s.madePoints++
	s.generatorIndex++
	//return ret
	return true
}


