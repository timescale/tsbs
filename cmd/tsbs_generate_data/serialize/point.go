package serialize

// Point wraps a single data point. It stores database-agnostic data
import (
	"io"
	"sync"
	"time"
)

// Point wraps a single data point. It stores database-agnostic data
// representing one point in time of one measurement.
//
// Internally, Point uses byte slices instead of strings to try to minimize
// overhead.
type Point struct {
	measurementName []byte
	tagKeys         [][]byte
	tagValues       [][]byte
	fieldKeys       [][]byte
	fieldValues     []interface{}
	timestamp       *time.Time
}

// NewPoint returns a new empty Point
func NewPoint() *Point {
	return &Point{
		measurementName: nil,
		tagKeys:         make([][]byte, 0),
		tagValues:       make([][]byte, 0),
		fieldKeys:       make([][]byte, 0),
		fieldValues:     make([]interface{}, 0),
		timestamp:       &time.Time{},
	}
}

// Reset clears all information from this Point so it can be reused.
func (p *Point) Reset() {
	p.measurementName = nil
	p.tagKeys = p.tagKeys[:0]
	p.tagValues = p.tagValues[:0]
	p.fieldKeys = p.fieldKeys[:0]
	p.fieldValues = p.fieldValues[:0]
	p.timestamp = nil
}

// SetTimestamp sets the Timestamp for this data point
func (p *Point) SetTimestamp(t *time.Time) {
	p.timestamp = t
}

// SetMeasurementName sets the name of the measurement for this data point
func (p *Point) SetMeasurementName(s []byte) {
	p.measurementName = s
}

// MeasurementName returns the name of the Point's measurement
func (p *Point) MeasurementName() []byte {
	return p.measurementName
}

// FieldKeys returns the Point's field keys
func (p *Point) FieldKeys() [][]byte {
	return p.fieldKeys
}

// AppendTag adds a tag with a given key and value to this data point
func (p *Point) AppendTag(key, value []byte) {
	p.tagKeys = append(p.tagKeys, key)
	p.tagValues = append(p.tagValues, value)
}

// AppendField adds a field with a given key and value to this data point
func (p *Point) AppendField(key []byte, value interface{}) {
	p.fieldKeys = append(p.fieldKeys, key)
	p.fieldValues = append(p.fieldValues, value)
}

// PointSerializer serializes a Point for writing
type PointSerializer interface {
	Serialize(p *Point, w io.Writer) error
}

// scratchBufPool helps reuse serialization scratch buffers.
var scratchBufPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 1024)
	},
}
