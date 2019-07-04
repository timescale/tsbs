package common

import (
	"reflect"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

// SimulatorConfig is an interface to create a Simulator from a time.Duration.
type SimulatorConfig interface {
	NewSimulator(time.Duration, uint64) Simulator
}

// BaseSimulatorConfig is used to create a BaseSimulator.
type BaseSimulatorConfig struct {
	// Start is the beginning time for the Simulator
	Start time.Time
	// End is the ending time for the Simulator
	End time.Time
	// InitGeneratorScale is the number of Generators to start with in the first reporting period
	InitGeneratorScale uint64
	// GeneratorScale is the total number of Generators to have in the last reporting period
	GeneratorScale uint64
	// GeneratorConstructor is the function used to create a new Generator given an id number and start time
	GeneratorConstructor func(i int, start time.Time) Generator
}

func calculateEpochs(duration time.Duration, interval time.Duration) uint64 {
	return uint64(duration.Nanoseconds() / interval.Nanoseconds())
}

// NewSimulator produces a Simulator that conforms to the given config over the specified interval.
func (sc *BaseSimulatorConfig) NewSimulator(interval time.Duration, limit uint64) Simulator {
	generators := make([]Generator, sc.GeneratorScale)
	for i := 0; i < len(generators); i++ {
		generators[i] = sc.GeneratorConstructor(i, sc.Start)
	}

	epochs := calculateEpochs(sc.End.Sub(sc.Start), interval)
	maxPoints := epochs * sc.GeneratorScale * uint64(len(generators[0].Measurements()))
	if limit > 0 && limit < maxPoints {
		// Set specified points number limit
		maxPoints = limit
	}
	sim := &BaseSimulator{
		madePoints: 0,
		maxPoints:  maxPoints,

		generatorIndex: 0,
		generators:     generators,

		epoch:           0,
		epochs:          epochs,
		epochGenerators: sc.InitGeneratorScale,
		initGenerators:  sc.InitGeneratorScale,
		timestampStart:  sc.Start,
		timestampEnd:    sc.End,
		interval:        interval,

		simulatedMeasurementIndex: 0,
	}

	return sim
}

// Simulator simulates a use case.
type Simulator interface {
	Finished() bool
	Next(*serialize.Point) bool
	Fields() map[string][][]byte
	TagKeys() [][]byte
	TagTypes() []reflect.Type
}

// BaseSimulator generates data similar to truck readings.
type BaseSimulator struct {
	madePoints uint64
	maxPoints  uint64

	generatorIndex uint64
	generators     []Generator

	epoch           uint64
	epochs          uint64
	epochGenerators uint64
	initGenerators  uint64

	timestampStart time.Time
	timestampEnd   time.Time
	interval       time.Duration

	simulatedMeasurementIndex int
}

// Finished tells whether we have simulated all the necessary points.
func (s *BaseSimulator) Finished() bool {
	return s.madePoints >= s.maxPoints
}

// Next advances a Point to the next state in the generator.
func (s *BaseSimulator) Next(p *serialize.Point) bool {
	if s.generatorIndex == uint64(len(s.generators)) {
		s.generatorIndex = 0
		s.simulatedMeasurementIndex++
	}

	if s.simulatedMeasurementIndex == len(s.generators[0].Measurements()) {
		s.simulatedMeasurementIndex = 0

		for i := 0; i < len(s.generators); i++ {
			s.generators[i].TickAll(s.interval)
		}

		s.adjustNumHostsForEpoch()
	}

	generator := s.generators[s.generatorIndex]

	// Populate the Generator tags.
	for _, tag := range generator.Tags() {
		p.AppendTag(tag.Key, tag.Value)
	}

	// Populate measurement-specific tags and fields:
	generator.Measurements()[s.simulatedMeasurementIndex].ToPoint(p)

	ret := s.generatorIndex < s.epochGenerators
	s.madePoints++
	s.generatorIndex++
	return ret
}

// Fields returns all the simulated measurements for the device.
func (s *BaseSimulator) Fields() map[string][][]byte {
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
func (s *BaseSimulator) TagKeys() [][]byte {
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

// TagTypes returns the type for each tag, extracted from the generated values
func (s *BaseSimulator) TagTypes() []reflect.Type {
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

// TODO(rrk) - Can probably turn this logic into a separate interface and implement other
// types of scale up, e.g., exponential
//
// To "scale up" the number of reporting items, we need to know when
// which epoch we are currently in. Once we know that, we can take the "missing"
// amount of scale -- i.e., the max amount of scale less the initial amount
// -- and add it in proportion to the percentage of epochs that have passed. This
// way we simulate all items at each epoch, but at the end of the function
// we check whether the point should be recorded by the calling process.
func (s *BaseSimulator) adjustNumHostsForEpoch() {
	s.epoch++
	missingScale := float64(uint64(len(s.generators)) - s.initGenerators)
	s.epochGenerators = s.initGenerators + uint64(missingScale*float64(s.epoch)/float64(s.epochs-1))
}

// SimulatedMeasurement simulates one measurement (e.g. Redis for DevOps).
type SimulatedMeasurement interface {
	Tick(time.Duration)
	ToPoint(*serialize.Point)
}
