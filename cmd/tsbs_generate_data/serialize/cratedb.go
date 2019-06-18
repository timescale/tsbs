package serialize

import (
	"fmt"
	"io"
)

const TAB = '\t'

// CrateDBSerializer writes a Point in a serialized form for CrateDB
type CrateDBSerializer struct{}

// Serialize Point p to the given Writer w, so it can be  loaded by the CrateDB
// loader. The format is TSV with one line per point, that contains the
// measurement type, tags with keys and values as a JSON object, timestamp,
// and metric values.
//
// An example of a serialized point:
//     cpu\t{"hostname":"host_0","rack":"1"}\t1451606400000000000\t38\t0\t50\t41234
func (s *CrateDBSerializer) Serialize(p *Point, w io.Writer) error {
	buf := make([]byte, 0, 256)

	// measurement type
	buf = append(buf, p.measurementName...)
	buf = append(buf, TAB)

	// tags
	if len(p.tagKeys) > 0 {
		buf = append(buf, '{')
		for i, key := range p.tagKeys {
			buf = append(buf, '"')
			buf = append(buf, key...)
			buf = append(buf, []byte("\":\"")...)
			buf = append(buf, p.tagValues[i]...)
			buf = append(buf, []byte("\",")...)
		}
		buf = buf[:len(buf)-1]
		buf = append(buf, '}')
	} else {
		buf = append(buf, []byte("null")...)
	}

	// timestamp
	buf = append(buf, TAB)
	ts := fmt.Sprintf("%d", p.timestamp.UTC().UnixNano())
	buf = append(buf, ts...)

	// metrics
	for _, v := range p.fieldValues {
		buf = append(buf, TAB)
		buf = fastFormatAppend(v, buf)
	}
	buf = append(buf, '\n')
	_, err := w.Write(buf)
	return err
}
