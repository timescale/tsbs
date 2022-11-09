package iotdb

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/timescale/tsbs/pkg/data"
)

// Serializer writes a Point in a serialized form for MongoDB
type Serializer struct{}

// Serialize writes Point p to the given Writer w, so it can be
// loaded by the IoTDB loader. The format is CSV with two lines per Point,
// with the first row being the names of fields and the second row being the
// field values.
//
// e.g.,
// deviceID,timestamp,<fieldName1>,<fieldName2>,<fieldName3>,...
// <deviceID>,<timestamp>,<field1>,<field2>,<field3>,...
//
// deviceID,timestamp,hostname,tag2
// root.cpu.host_1,1666281600000,'host_1',44.0
func (s *Serializer) Serialize(p *data.Point, w io.Writer) error {
	// Tag row first, prefixed with 'time,path'
	buf1 := make([]byte, 0, 1024)
	buf1 = append(buf1, []byte("deviceID,timestamp")...)
	tempBuf := make([]byte, 0, 1024)
	var hostname string
	foundHostname := false
	tagKeys := p.TagKeys()
	tagValues := p.TagValues()
	for i, v := range tagValues {
		if keyStr := string(tagKeys[i]); keyStr == "hostname" {
			foundHostname = true
			hostname = v.(string)
		} else {
			buf1 = append(buf1, ',')
			buf1 = append(buf1, tagKeys[i]...)
			tempBuf = append(tempBuf, ',')
			tempBuf = iotdbFormatAppend(v, tempBuf)
		}
	}
	if !foundHostname {
		// Unable to find hostname as part of device id
		hostname = "unknown"
	}
	buf2 := make([]byte, 0, 1024)
	buf2 = append(buf2, []byte(fmt.Sprintf("root.%s.%s,", modifyHostname(string(p.MeasurementName())), hostname))...)
	buf2 = append(buf2, []byte(fmt.Sprintf("%d", p.Timestamp().UTC().UnixMicro()))...)
	buf2 = append(buf2, tempBuf...)
	// Fields
	fieldKeys := p.FieldKeys()
	fieldValues := p.FieldValues()
	for i, v := range fieldValues {
		buf1 = append(buf1, ',')
		buf1 = append(buf1, fieldKeys[i]...)
		buf2 = append(buf2, ',')
		buf2 = iotdbFormatAppend(v, buf2)
	}
	buf1 = append(buf1, '\n')
	buf2 = append(buf2, '\n')
	_, err := w.Write(buf1)
	if err == nil {
		_, err = w.Write(buf2)
	}
	return err
}

// modifyHostnames makes sure IP address can appear in the path.
// Node names in path can NOT contain "." unless enclosing it within either single quote (') or double quote (").
// In this case, quotes are recognized as part of the node name to avoid ambiguity.
func modifyHostname(hostname string) string {
	if strings.Contains(hostname, ".") {
		if !(hostname[:1] == "`" && hostname[len(hostname)-1:] == "`") {
			// not modified yet
			hostname = "`" + hostname + "`"
		}

	}
	return hostname
}

// Utility function for appending various data types to a byte string
func iotdbFormatAppend(v interface{}, buf []byte) []byte {
	switch v.(type) {
	case int:
		return strconv.AppendInt(buf, int64(v.(int)), 10)
	case int64:
		return strconv.AppendInt(buf, v.(int64), 10)
	case float64:
		// Why -1 ?
		// From Golang source on genericFtoa (called by AppendFloat): 'Negative precision means "only as much as needed to be exact."'
		// Using this instead of an exact number for precision ensures we preserve the precision passed in to the function, allowing us
		// to use different precision for different use cases.
		return strconv.AppendFloat(buf, v.(float64), 'f', -1, 64)
	case float32:
		return strconv.AppendFloat(buf, float64(v.(float32)), 'f', -1, 32)
	case bool:
		return strconv.AppendBool(buf, v.(bool))
	case []byte:
		buf = append(buf, []byte("'")...)
		buf = append(buf, v.([]byte)...)
		buf = append(buf, []byte("'")...)
		return buf
	case string:
		// buf = append(buf, []byte(fmt.Sprintf("\"%s\"", v.(string)))...)
		buf = append(buf, []byte("'")...)
		buf = append(buf, v.(string)...)
		buf = append(buf, []byte("'")...)
		return buf
	case nil:
		return buf
	default:
		panic(fmt.Sprintf("unknown field type for %#v", v))
	}
}
