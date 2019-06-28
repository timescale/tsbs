package serialize

import (
	"fmt"
	"io"
)

// TimescaleDBSerializer writes a Point in a serialized form for TimescaleDB
type TimescaleDBSerializer struct{}

// Serialize writes Point p to the given Writer w, so it can be
// loaded by the TimescaleDB loader. The format is CSV with two lines per Point,
// with the first row being the tags and the second row being the field values.
//
// e.g.,
// tags,<tag1>,<tag2>,<tag3>,...
// <measurement>,<timestamp>,<field1>,<field2>,<field3>,...
func (s *TimescaleDBSerializer) Serialize(p *Point, w io.Writer) error {
	// Tag row first, prefixed with name 'tags'
	buf := make([]byte, 0, 256)
	buf = append(buf, []byte("tags")...)
	for i, v := range p.tagValues {
		buf = append(buf, ',')
		buf = append(buf, p.tagKeys[i]...)
		buf = append(buf, '=')
		buf = fastFormatAppend(v, buf)
	}
	buf = append(buf, '\n')
	_, err := w.Write(buf)
	if err != nil {
		return err
	}

	// Field row second
	buf = make([]byte, 0, 256)
	buf = append(buf, p.measurementName...)
	buf = append(buf, ',')
	buf = append(buf, []byte(fmt.Sprintf("%d", p.timestamp.UTC().UnixNano()))...)

	for _, v := range p.fieldValues {
		buf = append(buf, ',')
		buf = fastFormatAppend(v, buf)
	}
	buf = append(buf, '\n')
	_, err = w.Write(buf)
	return err
}
