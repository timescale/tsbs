package serialize

import (
	"io"
)

type PeregreenSerializer struct{}

func (s *PeregreenSerializer) Serialize(p *Point, w io.Writer) (err error) {
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
		buf = fastFormatAppend(value, buf)
	}

	if !firstFieldFormatted {
		return nil
	}
	buf = append(buf, ' ')
	buf = fastFormatAppend(p.timestamp.UTC().UnixNano(), buf)
	buf = append(buf, '\n')
	_, err = w.Write(buf)

	return err
}
