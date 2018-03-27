package serialize

import (
	"fmt"
	"io"
	"strconv"
)

// TimescaleDBSerializer writes a Point in a serialized form for TimescaleDB
type TimescaleDBSerializer struct {
	PointSerializer
}

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
	for _, v := range p.TagValues {
		buf = append(buf, ',')
		buf = append(buf, v...)
	}
	buf = append(buf, '\n')
	_, err := w.Write(buf)
	if err != nil {
		return err
	}

	// Field row second
	buf = make([]byte, 0, 256)
	buf = append(buf, p.MeasurementName...)
	buf = append(buf, ',')
	buf = append(buf, []byte(fmt.Sprintf("%d", p.Timestamp.UTC().UnixNano()))...)

	for i := 0; i < len(p.FieldKeys); i++ {
		buf = append(buf, ',')
		v := p.FieldValues[i]
		buf = fastFormatAppend(v, buf)
	}
	buf = append(buf, '\n')
	_, err = w.Write(buf)
	return err
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
		panic(fmt.Sprintf("unknown field type for %#v", v))
	}
}
