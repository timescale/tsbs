package timescaledb

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"io"
)

// Serializer writes a Point in a serialized form for TimescaleDB
type Serializer struct{}

// Serialize writes Point p to the given Writer w, so it can be
// loaded by the TimescaleDB loader. The format is CSV with two lines per Point,
// with the first row being the tags and the second row being the field values.
//
// e.g.,
// tags,<tag1>,<tag2>,<tag3>,...
// <measurement>,<timestamp>,<field1>,<field2>,<field3>,...
func (s *Serializer) Serialize(p *data.Point, w io.Writer) error {
	// Tag row first, prefixed with name 'tags'
	buf := make([]byte, 0, 256)
	buf = append(buf, []byte("tags")...)
	tagKeys := p.TagKeys()
	tagValues := p.TagValues()
	for i, v := range tagValues {
		buf = append(buf, ',')
		buf = append(buf, tagKeys[i]...)
		buf = append(buf, '=')
		buf = serialize.FastFormatAppend(v, buf)
	}
	buf = append(buf, '\n')
	_, err := w.Write(buf)
	if err != nil {
		return err
	}

	// Field row second
	buf = make([]byte, 0, 256)
	buf = append(buf, p.MeasurementName()...)
	buf = append(buf, ',')
	buf = append(buf, []byte(fmt.Sprintf("%d", p.Timestamp().UTC().UnixNano()))...)
	fieldValues := p.FieldValues()
	for _, v := range fieldValues {
		buf = append(buf, ',')
		buf = serialize.FastFormatAppend(v, buf)
	}
	buf = append(buf, '\n')
	_, err = w.Write(buf)
	return err
}
