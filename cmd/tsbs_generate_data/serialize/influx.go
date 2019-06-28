package serialize

import (
	"io"
)

// InfluxSerializer writes a Point in a serialized form for MongoDB
type InfluxSerializer struct{}

// Serialize writes Point data to the given writer, conforming to the
// InfluxDB wire protocol.
//
// This function writes output that looks like:
// <measurement>,<tag key>=<tag value> <field name>=<field value> <timestamp>\n
//
// For example:
// foo,tag0=bar baz=-1.0 100\n
func (s *InfluxSerializer) Serialize(p *Point, w io.Writer) (err error) {
	buf := make([]byte, 0, 1024)
	buf = append(buf, p.measurementName...)

	fakeTags := make([]int, 0)
	for i := 0; i < len(p.tagKeys); i++ {
		if p.tagValues[i] == nil {
			continue
		}
		switch v := p.tagValues[i].(type) {
		case string:
			buf = append(buf, ',')
			buf = append(buf, p.tagKeys[i]...)
			buf = append(buf, '=')
			buf = append(buf, []byte(v)...)
		default:
			fakeTags = append(fakeTags, i)
		}
	}

	if len(fakeTags) > 0 || len(p.fieldKeys) > 0 {
		buf = append(buf, ' ')
	}
	for i := 0; i < len(fakeTags); i++ {
		tagIndex := fakeTags[i]
		buf = appendField(buf, p.fieldKeys[tagIndex], p.fieldValues[tagIndex])
		if i+1 < len(p.fieldKeys) || len(p.fieldKeys) > 0 {
			buf = append(buf, ',')
		}
	}

	for i := 0; i < len(p.fieldKeys); i++ {
		buf = appendField(buf, p.fieldKeys[i], p.fieldValues[i])
		if i+1 < len(p.fieldKeys) {
			buf = append(buf, ',')
		}
	}

	buf = append(buf, ' ')
	buf = fastFormatAppend(p.timestamp.UTC().UnixNano(), buf)
	buf = append(buf, '\n')
	_, err = w.Write(buf)

	return err
}

func appendField(buf, key []byte, v interface{}) []byte {
	buf = append(buf, key...)
	buf = append(buf, '=')

	buf = fastFormatAppend(v, buf)

	// Influx uses 'i' to indicate integers:
	switch v.(type) {
	case int, int64:
		buf = append(buf, 'i')
	}

	return buf
}
