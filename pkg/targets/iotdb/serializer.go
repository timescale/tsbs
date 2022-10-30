package iotdb

import (
	"fmt"
	"io"
	"strings"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
)

// Serializer writes a Point in a serialized form for MongoDB
type Serializer struct{}

// Serialize writes Point p to the given Writer w, so it can be
// loaded by the IoTDB loader. The format is CSV with two lines per Point,
// with the first row being the names of fields and the second row being the
// field values.
//
// e.g.,
// time,deviceID,<fieldName1>,<fieldName2>,<fieldName3>,...
// <timestamp>,<deviceID>,<field1>,<field2>,<field3>,...
//
// time,deviceID,hostname,tag2
// 2022-10-26 16:44:55,root.cpu.host_1,host_1,44.0
// time,deviceID,hostname,tag2
// 1666281600000,root.cpu.host_1,host_1,44.0
func (s *Serializer) Serialize(p *data.Point, w io.Writer) error {
	// Tag row first, prefixed with 'time,path'
	buf1 := make([]byte, 0, 1024)
	buf1 = append(buf1, []byte("time,deviceID")...)
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
			tempBuf = serialize.FastFormatAppend(v, tempBuf)
		}
	}
	if !foundHostname {
		// errMsg := "IoTDB Serialize Error, 'hostname' tag not found.\n Tags are:"
		// for i, _ := range tagKeys {
		// 	errMsg += fmt.Sprintf("%s, ", string(tagKeys[i]))
		// }
		// return fmt.Errorf("%s", errMsg)
		hostname = "unknown"
	}
	buf2 := make([]byte, 0, 1024)
	buf2 = append(buf2, []byte(fmt.Sprintf("%d,", p.Timestamp().UTC().UnixMicro()))...)
	buf2 = append(buf2, []byte(fmt.Sprintf("root.%s.%s", modifyHostname(string(p.MeasurementName())), hostname))...)
	buf2 = append(buf2, tempBuf...)
	// Fields
	fieldKeys := p.FieldKeys()
	fieldValues := p.FieldValues()
	for i, v := range fieldValues {
		buf1 = append(buf1, ',')
		buf1 = append(buf1, fieldKeys[i]...)
		buf2 = append(buf2, ',')
		buf2 = serialize.FastFormatAppend(v, buf2)
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
		if !(hostname[:1] == "\"" && hostname[len(hostname)-1:] == "\"") {
			// not modified yet
			hostname = "\"" + hostname + "\""
		}

	}
	return hostname
}
