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
	firstFieldFormatted := false
	for i := 0; i < len(fakeTags); i++ {
		tagIndex := fakeTags[i]
		// don't append a comma before the first field
		if firstFieldFormatted {
			buf = append(buf, ',')
		}
		firstFieldFormatted = true
		buf = appendField(buf, p.tagKeys[tagIndex], p.tagValues[tagIndex])
	}

	for i := 0; i < len(p.fieldKeys); i++ {
		value := p.fieldValues[i]
		if value == nil {
			continue
		}
		// don't append a comma before the first field
		if firstFieldFormatted {
			buf = append(buf, ',')
		}
		firstFieldFormatted = true
		buf = appendField(buf, p.fieldKeys[i], value)
	}

	// first field wasn't formatted, because all the fields were nil, InfluxDB will reject the insert
	if !firstFieldFormatted {
		return nil
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
