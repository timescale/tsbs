package serialize

import (
	"fmt"
	"io"
)

// QuestDBSerializer writes a Point in a serialized form for QuestDB
type QuestDBSerializer struct{}

// Serialize Point p to the given Writer w, so it can be  loaded by the QuestDB
// loader. 
func (s *QuestDBSerializer) Serialize(p *Point, w io.Writer) error {
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
			buf = fastFormatAppend(p.tagValues[i], buf)
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
