package openmetrics

import (
	"fmt"
	"io"
	"strconv"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
)

// Serializer writes a Point in a text form for OpenMetrics
// https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md#text-format
type Serializer struct{}

// This function writes output that looks like:
// <measurement>_<field_name>{<tag key>="<tag value>",<tag key>="<tag value>"} <field value> <timestamp>\n
//
// For example:
// foo_baz{tag0="bar1",tag1="bar2"} -1.0 100\n
func (s *Serializer) Serialize(p *data.Point, w io.Writer) (err error) {
	commonTags := make([]byte, 0, 1024)

	tagKeys := p.TagKeys()
	tagValues := p.TagValues()

	if len(tagKeys) > 0 {
		commonTags = append(commonTags, '{')
	}

	for i := 0; i < len(tagKeys); i++ {
		if i > 0 {
			commonTags = append(commonTags, ',')
		}
		commonTags = append(commonTags, tagKeys[i]...)
		commonTags = append(commonTags, '=')
		if tagValues[i] == nil {
			commonTags = append(commonTags, '"')
			commonTags = append(commonTags, '"')
			continue
		}
		switch v := tagValues[i].(type) {
		case string:
			commonTags = append(commonTags, '"')
			commonTags = append(commonTags, []byte(v)...)
			commonTags = append(commonTags, '"')
		case float32:
			commonTags = append(commonTags, '"')
			commonTags = append(commonTags, strconv.FormatFloat(float64(tagValues[i].(float32)), 'f', -1, 64)...)
			commonTags = append(commonTags, '"')
		case float64:
			commonTags = append(commonTags, '"')
			commonTags = append(commonTags, strconv.FormatFloat(float64(tagValues[i].(float64)), 'f', -1, 64)...)
			commonTags = append(commonTags, '"')
		default:
			panic(fmt.Errorf("non-string tags not implemented for openmetrics %s", v))
		}
	}

	if len(tagKeys) > 0 {
		commonTags = append(commonTags, '}')
	}

	commonTags = append(commonTags, ' ')

	buf := make([]byte, 0, 1024)

	fieldKeys := p.FieldKeys()
	fieldValues := p.FieldValues()
	for i := 0; i < len(fieldKeys); i++ {

		buf = append(buf, p.MeasurementName()...)
		buf = append(buf, '_')
		buf = append(buf, fieldKeys[i]...)

		buf = append(buf, commonTags...)

		var value string
		switch t := fieldValues[i].(type) {
		case nil:
			value = "nil"
		case string:
			value = fieldValues[i].(string)
		case int:
			value = strconv.FormatInt(int64(fieldValues[i].(int)), 10)
		case int64:
			value = strconv.FormatInt(fieldValues[i].(int64), 10)
		case float32:
			value = strconv.FormatFloat(float64(fieldValues[i].(float32)), 'f', -1, 64)
		case float64:
			value = strconv.FormatFloat(fieldValues[i].(float64), 'f', -1, 64)
		default:
			panic(fmt.Errorf("non-string tags not implemented for prometheus %s", t))
		}
		buf = append(buf, value...)

		if p.Timestamp() != nil {
			buf = append(buf, ' ')
			buf = serialize.FastFormatAppend(p.Timestamp().UTC().UnixMilli(), buf)
		}
		buf = append(buf, '\n')
	}

	_, err = w.Write(buf)

	return err
}
