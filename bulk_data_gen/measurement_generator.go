package main

import (
	"fmt"
	"io"
	"math/rand"
	"time"
)

// Used for generating random field names:
const letters = "abcdefghijklmnopqrstuvwxyz"

// MeasurementGeneratorConfig stores parameters used to construct a
// MeasurementGenerator.
type MeasurementGeneratorConfig struct {
	Count int64

	NameLen int

	TagKeyLen   int
	TagKeyCount int

	TagValueLen   int
	TagValueCount int

	FieldNameLen int
	FieldCount   int
	FieldStdDevs []float64
	FieldMeans   []float64

	TimestampStart time.Time
	TimestampEnd   time.Time
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

	if !cfg.TimestampStart.Before(cfg.TimestampEnd) {
		return fmt.Errorf("start time is not less than end time")
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
	Timestamp       time.Time
}

// Using these literals prevents the slices from escaping to the heap, saving
// a few micros per call:
var (
	charComma  = []byte(",")
	charEquals = []byte("=")
	charSpace  = []byte(" ")
)

// SerializeInfluxBulk writes Point data to the given writer, conforming to the
// InfluxDB wire protocol.
//
// This function writes output that looks like:
// <measurement>,<tag key>=<tag value> <field name>=<field value> <timestamp>\n
//
// For example:
// foo,tag0=bar baz=-1.0 100\n
func (p *Point) SerializeInfluxBulk(w io.Writer) error {
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

	_, err = fmt.Fprintf(w, " %d\n", p.Timestamp.UnixNano())
	if err != nil {
		return err
	}

	return err
}

// SerializeESBulk writes Point data to the given writer, conforming to the
// ElasticSearch bulk load protocol.
//
// This function writes output that looks like:
// <action line>
// <tags, fields, and timestamp>
//
// For example:
// { "create" : { "_index" : "measurement_otqio", "_type" : "point" } }\n
// { "tag_launx": "btkuw", "tag_gaijk": "jiypr", "field_wokxf": 0.08463898963964356, "field_zqstf": -0.043641533500086316, "timestamp": 171300 }\n
func (p *Point) SerializeESBulk(w io.Writer) error {
	action := "{ \"create\" : { \"_index\" : \"%s\", \"_type\" : \"point\" } }\n"
	_, err := fmt.Fprintf(w, action, p.MeasurementName)
	if err != nil {
		return err
	}
	_, err = w.Write([]byte("{ "))
	if err != nil {
		return err
	}

	for i := 0; i < len(p.TagKeys); i++ {
		if i > 0 {
			_, err = fmt.Fprintf(w, ", ")
			if err != nil {
				return err
			}
		}
		_, err = fmt.Fprintf(w, "\"%s\": ", p.TagKeys[i])
		if err != nil {
			return err
		}

		_, err = fmt.Fprintf(w, "\"%s\"", p.TagValues[i])
		if err != nil {
			return err
		}
	}

	if len(p.TagKeys) > 0 && len(p.FieldKeys) > 0 {
		_, err = fmt.Fprintf(w, ", ")
		if err != nil {
			return err
		}
	}

	for i := 0; i < len(p.FieldKeys); i++ {
		if i > 0 {
			_, err = fmt.Fprintf(w, ", ")
			if err != nil {
				return err
			}
		}
		_, err = fmt.Fprintf(w, "\"%s\": ", p.FieldKeys[i])
		if err != nil {
			return err
		}

		_, err = fmt.Fprintf(w, "%#v", p.FieldValues[i])
		if err != nil {
			return err
		}
	}

	if len(p.TagKeys) > 0 || len(p.FieldKeys) > 0 {
		_, err = fmt.Fprintf(w, ", ")
		if err != nil {
			return err
		}
	}
	// Timestamps in ES must be millisecond precision:
	_, err = fmt.Fprintf(w, "\"timestamp\": %d }\n", p.Timestamp.UnixNano()/1e6)
	if err != nil {
		return err
	}

	return nil
}

// MeasurementGenerator creates data for populating a given measurement.
// Instances are created using a MeasurementGeneratorConfig.
//
// Field values are sampled from a normal distribution.
type MeasurementGenerator struct {
	Total int64
	Seen  int64

	Name            []byte
	TagKeys         [][]byte
	TagValueChoices [][][]byte
	FieldKeys       [][]byte

	P            *Point
	FieldMeans   []float64
	FieldStdDevs []float64

	TimestampStart     time.Time
	TimestampIncrement time.Duration
	TimestampEnd       time.Time

	Config *MeasurementGeneratorConfig
}

// NewMeasurementGenerator creates a MeasurementGenerator from the given
// MeasurementGeneratorConfig.
func NewMeasurementGenerator(cfg *MeasurementGeneratorConfig) MeasurementGenerator {
	tvc := make([][][]byte, cfg.TagKeyCount)
	for i := 0; i < len(tvc); i++ {
		tvc[i] = randBytesSeq(nil, cfg.TagValueCount, cfg.TagValueLen)
	}

	diffNanos := cfg.TimestampEnd.UnixNano() - cfg.TimestampStart.UnixNano()
	if diffNanos <= 0 {
		panic("logic error: diffNanos <= 0")
	}

	perPointNanos := diffNanos / cfg.Count
	if perPointNanos == 0 {
		perPointNanos = 1
	}

	timestampIncrement := time.Duration(perPointNanos) * time.Nanosecond

	g := MeasurementGenerator{
		Total: cfg.Count,
		Seen:  0,

		Name:            randBytes([]byte("measurement_"), cfg.NameLen),
		TagKeys:         randBytesSeq([]byte("tag_"), cfg.TagKeyCount, cfg.TagKeyLen),
		TagValueChoices: tvc,
		FieldKeys:       randBytesSeq([]byte("field_"), cfg.FieldCount, cfg.FieldNameLen),

		P:            &Point{},
		FieldMeans:   cfg.FieldMeans,
		FieldStdDevs: cfg.FieldStdDevs,

		TimestampStart:     cfg.TimestampStart,
		TimestampIncrement: timestampIncrement,
		TimestampEnd:       cfg.TimestampEnd,

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

	mg.P.Timestamp = mg.TimestampStart
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
	mg.P.Timestamp = mg.P.Timestamp.Add(mg.TimestampIncrement)

	mg.Seen++
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
// having `length` random bytes. If the prefix is non-empty, it will be
// prepended to each element.
func randBytesSeq(prefix []byte, count, length int) [][]byte {
	bb := make([][]byte, count)
	for i := 0; i < count; i++ {
		bb[i] = randBytes(prefix, length)
	}
	return bb
}
