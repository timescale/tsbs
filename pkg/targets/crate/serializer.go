package crate

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"io"
)

const TAB = '\t'

// CrateDBSerializer writes a Point in a serialized form for CrateDB
type Serializer struct{}

// Serialize Point p to the given Writer w, so it can be  loaded by the CrateDB
// loader. The format is TSV with one line per point, that contains the
// measurement type, tags with keys and values as a JSON object, timestamp,
// and metric values.
//
// An example of a serialized point:
//     cpu\t{"hostname":"host_0","rack":"1"}\t1451606400000000000\t38\t0\t50\t41234
func (s *Serializer) Serialize(p *data.Point, w io.Writer) error {
	buf := make([]byte, 0, 256)

	// measurement type
	buf = append(buf, p.MeasurementName()...)
	buf = append(buf, TAB)

	// tags
	tagKeys := p.TagKeys()
	tagValues := p.TagValues()
	if len(tagKeys) > 0 {
		buf = append(buf, '{')
		for i, key := range tagKeys {
			buf = append(buf, '"')
			buf = append(buf, key...)
			buf = append(buf, []byte("\":\"")...)
			buf = serialize.FastFormatAppend(tagValues[i], buf)
			buf = append(buf, []byte("\",")...)
		}
		buf = buf[:len(buf)-1]
		buf = append(buf, '}')
	} else {
		buf = append(buf, []byte("null")...)
	}

	// timestamp
	buf = append(buf, TAB)
	ts := fmt.Sprintf("%d", p.Timestamp().UTC().UnixNano())
	buf = append(buf, ts...)

	// metrics
	fieldValues := p.FieldValues()
	for _, v := range fieldValues {
		buf = append(buf, TAB)
		buf = serialize.FastFormatAppend(v, buf)
	}
	buf = append(buf, '\n')
	_, err := w.Write(buf)
	return err
}
