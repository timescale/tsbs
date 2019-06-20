package serialize

// Point wraps a single data point. It stores database-agnostic data
import (
	"bytes"
	"io"
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
		timestamp:       nil,
	}
}

// Copy duplicates all the values from a given Point.
func (p *Point) Copy(from *Point) {
	p.measurementName = from.measurementName
	p.tagKeys = from.tagKeys
	p.tagValues = from.tagValues
	p.fieldKeys = from.fieldKeys
	p.fieldValues = from.fieldValues
	timeCopy := *from.timestamp
	p.timestamp = &timeCopy
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

// AppendField adds a field with a given key and value to this data point
func (p *Point) AppendField(key []byte, value interface{}) {
	p.fieldKeys = append(p.fieldKeys, key)
	p.fieldValues = append(p.fieldValues, value)
}

// GetFieldValue returns the corresponding value for a given field key or nil if it does not exist.
// This will panic if the internal state has been altered to not have the same number of field keys as field values.
func (p *Point) GetFieldValue(key []byte) interface{} {
	if len(p.fieldKeys) != len(p.fieldValues) {
		panic("field keys and field values are out of sync")
	}
	for i, v := range p.fieldKeys {
		if bytes.Equal(v, key) {
			return p.fieldValues[i]
		}
	}
	return nil
}

// ClearFieldValue sets the field value to nil for a given field key.
// This will panic if the internal state has been altered to not have the same number of field keys as field values.
func (p *Point) ClearFieldValue(key []byte) {
	if len(p.fieldKeys) != len(p.fieldValues) {
		panic("field keys and field values are out of sync")
	}
	for i, v := range p.fieldKeys {
		if bytes.Equal(v, key) {
			p.fieldValues[i] = nil
			return
		}
	}
}

// TagKeys returns the Point's tag keys
func (p *Point) TagKeys() [][]byte {
	return p.tagKeys
}

// AppendTag adds a tag with a given key and value to this data point
func (p *Point) AppendTag(key, value []byte) {
	p.tagKeys = append(p.tagKeys, key)
	p.tagValues = append(p.tagValues, value)
}

// GetTagValue returns the corresponding value for a given tag key or nil if it does not exist.
// This will panic if the internal state has been altered to not have the same number of tag keys as tag values.
func (p *Point) GetTagValue(key []byte) []byte {
	if len(p.tagKeys) != len(p.tagValues) {
		panic("tag keys and tag values are out of sync")
	}
	for i, v := range p.tagKeys {
		if bytes.Equal(v, key) {
			return p.tagValues[i]
		}
	}
	return nil
}

// ClearTagValue sets the tag value to nil for a given field key.
// This will panic if the internal state has been altered to not have the same number of tag keys as tag values.
func (p *Point) ClearTagValue(key []byte) {
	if len(p.tagKeys) != len(p.tagValues) {
		panic("tag keys and tag values are out of sync")
	}
	for i, v := range p.tagKeys {
		if bytes.Equal(v, key) {
			p.tagValues[i] = []byte{}
			return
		}
	}
}

// PointSerializer serializes a Point for writing
type PointSerializer interface {
	Serialize(p *Point, w io.Writer) error
}
