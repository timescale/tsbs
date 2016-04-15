package main

import (
	"fmt"
	"io"
	"time"
)

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
	FieldValues     []interface{}
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
	buf := make([]byte, 0, 256)
	buf = append(buf, p.MeasurementName...)

	for i := 0; i < len(p.TagKeys); i++ {
		buf = append(buf, charComma...)
		buf = append(buf, p.TagKeys[i]...)
		buf = append(buf, charEquals...)
		buf = append(buf, p.TagValues[i]...)
	}

	if len(p.FieldKeys) > 0 {
		buf = append(buf, charSpace...)
	}

	for i := 0; i < len(p.FieldKeys); i++ {
		buf = append(buf, p.FieldKeys[i]...)
		buf = append(buf, charEquals...)

		v := p.FieldValues[i]
		format := formatFor(v)

		buf = append(buf, []byte(fmt.Sprintf(format, v))...)
		if i+1 < len(p.FieldKeys) {
			buf = append(buf, charComma...)
		}

	}

	buf = append(buf, []byte(fmt.Sprintf("%d\n", p.Timestamp.UnixNano()))...)
	_, err := w.Write(buf)

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
		buf = append(buf, []byte(fmt.Sprintf("\"%s\": ", p.FieldKeys[i]))...)

		v := p.FieldValues[i]
		format := formatFor(v)
		buf = append(buf, []byte(fmt.Sprintf(format, v))...)
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

func formatFor(v interface{}) string {
	switch v.(type) {
	case int, int64:
		return "%di"
	case float32, float64:
		return "%f"
	case bool:
		return "%t"
	case string, []byte:
		return "%s"
	default:
		panic("unknown field type")
	}
}
