package main

import (
	"fmt"
	"io"
	"strconv"
	"time"
)

// Point wraps a single data point. It stores database-agnostic data
// representing one point in time of one measurement.
//
// Internally, Point uses byte slices instead of strings to try to minimize
// overhead.
type Point struct {
	MeasurementName []byte
	TagKeys         [][]byte
	TagValues       [][]byte
	FieldKeys       [][]byte
	FieldValues     []interface{}
	Timestamp       *time.Time
}

// Using these literals prevents the slices from escaping to the heap, saving
// a few micros per call:
var (
	charComma  = byte(',')
	charEquals = byte('=')
	charSpace  = byte(' ')
)

func (p *Point) Reset() {
	p.MeasurementName = nil
	p.TagKeys = p.TagKeys[:0]
	p.TagValues = p.TagValues[:0]
	p.FieldKeys = p.FieldKeys[:0]
	p.FieldValues = p.FieldValues[:0]
	p.Timestamp = nil
}

func (p *Point) SetTimestamp(t *time.Time) {
	p.Timestamp = t
}

func (p *Point) SetMeasurementName(s []byte) {
	p.MeasurementName = s
}

func (p *Point) AppendTag(key, value []byte) {
	p.TagKeys = append(p.TagKeys, key)
	p.TagValues = append(p.TagValues, value)
}

func (p *Point) AppendField(key []byte, value interface{}) {
	p.FieldKeys = append(p.FieldKeys, key)
	p.FieldValues = append(p.FieldValues, value)
}

// SerializeInfluxBulk writes Point data to the given writer, conforming to the
// InfluxDB wire protocol.
//
// This function writes output that looks like:
// <measurement>,<tag key>=<tag value> <field name>=<field value> <timestamp>\n
//
// For example:
// foo,tag0=bar baz=-1.0 100\n
//
// TODO(rw): Speed up this function. The bulk of time is spent in strconv.
func (p *Point) SerializeInfluxBulk(w io.Writer) (err error) {
	buf := make([]byte, 0, 256)
	buf = append(buf, p.MeasurementName...)

	for i := 0; i < len(p.TagKeys); i++ {
		buf = append(buf, charComma)
		buf = append(buf, p.TagKeys[i]...)
		buf = append(buf, charEquals)
		buf = append(buf, p.TagValues[i]...)
	}

	if len(p.FieldKeys) > 0 {
		buf = append(buf, charSpace)
	}

	for i := 0; i < len(p.FieldKeys); i++ {
		buf = append(buf, p.FieldKeys[i]...)
		buf = append(buf, charEquals)

		v := p.FieldValues[i]
		buf = fastFormatAppend(v, buf)

		// Influx uses 'i' to indicate integers:
		switch v.(type) {
		case int, int64:
			buf = append(buf, byte('i'))
		}

		if i+1 < len(p.FieldKeys) {
			buf = append(buf, charComma)
		}
	}

	buf = append(buf, []byte(fmt.Sprintf(" %d\n", p.Timestamp.UnixNano()))...)
	_, err = w.Write(buf)

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
//
// TODO(rw): Speed up this function. The bulk of time is spent in strconv.
func (p *Point) SerializeESBulk(w io.Writer) error {
	action := "{ \"create\" : { \"_index\" : \"%s\", \"_type\" : \"point\" } }\n"
	_, err := fmt.Fprintf(w, action, p.MeasurementName)
	if err != nil {
		return err
	}

	buf := make([]byte, 0, 256)
	buf = append(buf, []byte("{")...)

	for i := 0; i < len(p.TagKeys); i++ {
		if i > 0 {
			buf = append(buf, []byte(", ")...)
		}
		buf = append(buf, []byte(fmt.Sprintf("\"%s\": ", p.TagKeys[i]))...)
		buf = append(buf, []byte(fmt.Sprintf("\"%s\"", p.TagValues[i]))...)
	}

	if len(p.TagKeys) > 0 && len(p.FieldKeys) > 0 {
		buf = append(buf, []byte(", ")...)
	}

	for i := 0; i < len(p.FieldKeys); i++ {
		if i > 0 {
			buf = append(buf, []byte(", ")...)
		}
		buf = append(buf, "\""...)
		buf = append(buf, p.FieldKeys[i]...)
		buf = append(buf, "\": "...)

		v := p.FieldValues[i]
		buf = fastFormatAppend(v, buf)
	}

	if len(p.TagKeys) > 0 || len(p.FieldKeys) > 0 {
		buf = append(buf, []byte(", ")...)
	}
	// Timestamps in ES must be millisecond precision:
	buf = append(buf, []byte(fmt.Sprintf("\"timestamp\": %d }\n", p.Timestamp.UnixNano()/1e6))...)

	_, err = w.Write(buf)
	if err != nil {
		return err
	}

	return nil
}

func fastFormatAppend(v interface{}, buf []byte) []byte {
	switch v.(type) {
	case int:
		return strconv.AppendInt(buf, int64(v.(int)), 10)
	case int64:
		return strconv.AppendInt(buf, v.(int64), 10)
	case float64:
		return strconv.AppendFloat(buf, v.(float64), 'f', 16, 64)
	case float32:
		return strconv.AppendFloat(buf, float64(v.(float32)), 'f', 16, 32)
	case bool:
		return strconv.AppendBool(buf, v.(bool))
	case []byte:
		buf = append(buf, v.([]byte)...)
		return buf
	case string:
		buf = append(buf, v.(string)...)
		return buf
	default:
		panic("unknown field type")
	}
}
