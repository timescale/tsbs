package siemens

import (
	"bufio"
	"fmt"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	valueDev = .5
	sensorFmt = "sensor_%d"
)

var (
	labelValue        = []byte("value")
)

type SiemensGenerator struct {
	simulatedMeasurements []common.SimulatedMeasurement
	tags                  []common.Tag
	//rnd *rand.Rand
}

// TickAll advances all Distributions of a Truck.
func (s *SiemensGenerator) TickAll(d time.Duration) {
	s.simulatedMeasurements[0].Tick(d)
}

func (s SiemensGenerator) Measurements() []common.SimulatedMeasurement {
	return s.simulatedMeasurements
}

func (t SiemensGenerator) Tags() []common.Tag {
	return t.tags
}

func (s *SiemensGenerator) initMeasurement(label []byte, start time.Time, inFile *os.File, outliersFreq float64) {
	s.simulatedMeasurements = []common.SimulatedMeasurement{
		&SensorMeasurement{
			//rnd: s.rnd,
			Timestamp:     start,
			outliersFreq: outliersFreq,
			inFile: inFile,
			Label: label,
		},
	}
}

func NewSiemensGenerator(i int, start time.Time, inFile *os.File, outliersFreq float64) common.Generator {
	sg := SiemensGenerator{}
	label := fmt.Sprintf(sensorFmt, i)
	sg.initMeasurement([]byte(label), start, inFile, outliersFreq)
	return &sg
}

type SensorMeasurement struct {
	Timestamp time.Time
	Label []byte
	//rnd *rand.Rand
	outliersFreq float64
	inFile *os.File
	value float64
	scanner *bufio.Scanner
	inFileShift int
}

func (m *SensorMeasurement) getLine() string{
	if m.scanner == nil || !m.scanner.Scan(){
		m.inFile.Seek(0, 0)
		m.scanner = bufio.NewScanner(m.inFile)
	}
	return m.scanner.Text()
}

func (m *SensorMeasurement) baseValue() float64{
	parts := make([]string, 0)
	for len(parts) < 3{
		line := m.getLine()
		fmt.Println(line)
		parts = strings.Split(line, ";")
	}
	base, _ := strconv.ParseFloat(parts[2], 64)
	return base
}

// ToPoint serializes ReadingsMeasurement to serialize.Point.
func (m *SensorMeasurement) ToPoint(p *serialize.Point) {
	p.SetMeasurementName(m.Label)
	copy := m.Timestamp
	p.SetTimestamp(&copy)
	p.AppendField(labelValue, m.getValue())
}

func (m *SensorMeasurement) Tick(d time.Duration){
	m.Timestamp = m.Timestamp.Add(d)
	base := m.baseValue()
	m.advance(base)
}

func (m *SensorMeasurement) advance(base float64){
	rv := rand.Float64()
	m.value = base + rand.Float64()*valueDev - valueDev/2
	if rv > 1.0 - m.outliersFreq {
		rv = rand.Float64()
		if rv > 0.5 {
			m.value += base * 10
		} else {
			m.value -= base * 10
		}
	}
}

func (m *SensorMeasurement) getValue() float64 {
	return m.value
}