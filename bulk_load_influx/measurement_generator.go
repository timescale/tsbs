package main

import (
	"fmt"
	"io"
	"math/rand"
)

// MeasurementGeneratorConfig stores parameters used to construct a
// MeasurementGenerator.
type MeasurementGeneratorConfig struct {
	NameLen int

	TagKeyLen   int
	TagKeyCount int

	TagValueLen   int
	TagValueCount int

	FieldNameLen int
	FieldCount   int
	FieldStdDevs []float64
	FieldMeans   []float64
}

// Validate checks that a MeasurementGeneratorConfig is sane.
func (cfg *MeasurementGeneratorConfig) Validate() error {
	if cfg.NameLen == 0 {
		return fmt.Errorf("empty name")
	}

	if cfg.TagKeyCount > 0 && cfg.TagValueCount == 0 {
		return fmt.Errorf("tag value count is too small")
	}

	if cfg.TagValueLen == 0 && cfg.TagValueCount > 0 {
		return fmt.Errorf("tag value len is too small")
	}

	if cfg.TagKeyLen == 0 && cfg.TagKeyCount > 0 {
		return fmt.Errorf("tag key len is too small")
	}

	if cfg.FieldNameLen == 0 && cfg.FieldCount > 0 {
		return fmt.Errorf("field name len is too small")
	}

	if len(cfg.FieldStdDevs) != cfg.FieldCount {
		return fmt.Errorf("field std deviations slice is the wrong size")
	}

	if len(cfg.FieldStdDevs) != cfg.FieldCount {
		return fmt.Errorf("field means slice is the wrong size")
	}

	return nil
}

// Point wraps a single data point. It is currently only used by
// MeasurementGenerator instances.
//
// Internally, Point stores byte slices, instead of strings, to minimize
// overhead.
type Point struct {
	MeasurementName []byte
	TagKeys         [][]byte
	TagValues       [][]byte
	FieldKeys       [][]byte
	FieldValues     []float64
	Timestamp       uint64
}

// Using these literals prevents the slices from escaping to the heap, saving
// a few micros per call:
var (
	charComma  = []byte(",")
	charEquals = []byte("=")
	charSpace  = []byte(" ")
)

// Serialize writes Point data to the given writer, conforming to the InfluxDB
// wire protocol.
//
// This function writes output that looks like:
// <measurement>,<tag key>=<tag value> <field name>=<field value> <timestamp>\n
// For example:
// foo,tag0=bar baz=-1.0 100\n
//
// This function is the most expensive in the entire program.
func (p *Point) Serialize(w io.Writer) error {
	_, err := w.Write(p.MeasurementName)
	if err != nil {
		return err
	}

	for i := 0; i < len(p.TagKeys); i++ {
		_, err = w.Write(charComma)
		if err != nil {
			return err
		}

		_, err = w.Write(p.TagKeys[i])
		if err != nil {
			return err
		}
		_, err = w.Write(charEquals)
		if err != nil {
			return err
		}

		_, err = w.Write(p.TagValues[i])
		if err != nil {
			return err
		}
	}

	if len(p.FieldKeys) > 0 {
		_, err = w.Write(charSpace)
		if err != nil {
			return err
		}
	}

	for i := 0; i < len(p.FieldKeys); i++ {
		_, err = w.Write(p.FieldKeys[i])
		if err != nil {
			return err
		}

		_, err = fmt.Fprintf(w, "=%f", p.FieldValues[i])
		if err != nil {
			return err
		}

		if i+1 < len(p.FieldKeys) {
			_, err = w.Write(charComma)
			if err != nil {
				return err
			}
		}

	}

	_, err = fmt.Fprintf(w, " %d\n", p.Timestamp)
	if err != nil {
		return err
	}

	return err
}

// MeasurementGenerator creates data for populating a given measurement.
// Instances are created using a MeasurementGeneratorConfig.
//
// Field values are sampled from a normal distribution.
type MeasurementGenerator struct {
	Name            []byte
	TagKeys         [][]byte
	TagValueChoices [][][]byte
	FieldKeys       [][]byte

	P            *Point
	FieldMeans   []float64
	FieldStdDevs []float64

	Count int64

	Config *MeasurementGeneratorConfig
}

// NewMeasurementGenerator creates a MeasurementGenerator from the given
// MeasurementGeneratorConfig.
func NewMeasurementGenerator(cfg *MeasurementGeneratorConfig) MeasurementGenerator {
	tvc := make([][][]byte, cfg.TagKeyCount)
	for i := 0; i < len(tvc); i++ {
		tvc[i] = randBytesSeq(nil, cfg.TagValueCount, cfg.TagValueLen)
	}
	g := MeasurementGenerator{
		Name:            randBytes([]byte("measurement_"), cfg.NameLen),
		TagKeys:         randBytesSeq([]byte("tag_"), cfg.TagKeyCount, cfg.TagKeyLen),
		TagValueChoices: tvc,
		FieldKeys:       randBytesSeq([]byte("field_"), cfg.FieldCount, cfg.FieldNameLen),

		P:            &Point{},
		FieldMeans:   cfg.FieldMeans,
		FieldStdDevs: cfg.FieldStdDevs,

		Config: cfg,
	}
	g.initPoint()
	return g
}

// initPoint initializes bookkeeping for a MeasurementGenerator. It should be
// called exactly once.
func (mg *MeasurementGenerator) initPoint() {
	mg.P.MeasurementName = []byte(mg.Name)

	mg.P.TagKeys = mg.TagKeys
	mg.P.TagValues = make([][]byte, len(mg.TagKeys))

	mg.P.FieldKeys = mg.FieldKeys
	mg.P.FieldValues = make([]float64, len(mg.FieldKeys))

	mg.P.Timestamp = 0
}

// Next updates the internal Point object with new data.
func (mg *MeasurementGenerator) Next() {
	// choose tag values
	for i := 0; i < len(mg.TagKeys); i++ {
		idx := rand.Int63n(int64(len(mg.TagValueChoices[i])))
		v := mg.TagValueChoices[i][idx]
		mg.P.TagValues[i] = v
	}

	// choose field values
	for i := 0; i < len(mg.FieldKeys); i++ {
		v := rand.NormFloat64()*mg.FieldStdDevs[i] + mg.FieldMeans[i]
		mg.P.FieldValues[i] = v
	}
	mg.P.Timestamp += 100 // ns

	mg.Count++
}

// randBytes creates random bytes of the given length. If the prefix is
// non-empty, it will be prepended to the result.
func randBytes(prefix []byte, length int) []byte {
	b := make([]byte, 0, length+len(prefix))
	if len(prefix) > 0 {
		b = append(prefix, b...)
	}
	for i := 0; i < length; i++ {
		c := letters[rand.Intn(len(letters))]
		b = append(b, c)
	}
	return b
}

// randBytesSeq creates a nested slice of `count` byte slices, each element
// element having `length` random bytes. If the prefix is non-empty, it will be
// prepended to each element.
func randBytesSeq(prefix []byte, count, length int) [][]byte {
	bb := make([][]byte, count)
	for i := 0; i < count; i++ {
		bb[i] = randBytes(prefix, length)
	}
	return bb
}

