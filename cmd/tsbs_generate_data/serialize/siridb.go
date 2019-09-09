package serialize

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"strconv"

	qpack "github.com/transceptor-technology/go-qpack"
)

// SiriDBSerializer writes a Point in a serialized form for SiriDB
type SiriDBSerializer struct{}

// Serialize writes Point data to the given writer.
//
// The format is is serialized in such a way that a known number of bytes
// acting as a header indicate the number of bytes of content that will follow.
//
// The first 8 bytes are reserved for the main header that is filled at the end
// of the script. 4 bytes of this are for number of metrics and 4 bytes for
// the length of the measuremnt name and tags.
//
// There are also sub headers of 8 bytes reserved for every data point that are
// filled in later. 4 bytes for the length of the field key and 4 bytes for the
// length of the packed data (timestamp and value).
//
// The output looks like this:
// <number of metrics> <length of name and tags> <name and tags> <length of field key_1> <length of timestamp_1 and field value_1> <field key_1> <packed timestamp_1 and value_1> <length of field key_2> <length of timestamp_1 and field value_2> <field key_2> <packed timestamp_1 and value_2>... etc.
func (s *SiriDBSerializer) Serialize(p *Point, w io.Writer) error {
	line := make([]byte, 8, 1024)
	line = append(line, p.measurementName...)
	line = append(line, '|')
	for i, v := range p.tagValues {
		if i != 0 {
			line = append(line, ',')
		}
		switch t := v.(type) {
		case string:
			line = append(line, p.tagKeys[i]...)
			line = append(line, '=')
			line = append(line, []byte(t)...)
		default:
			panic("Non string tags not supported")
		}

	}

	lenName := len(line) - 8

	var err error
	metricCount := 0

	for i, value := range p.fieldValues {

		indexLenData := len(line) + 4

		key := make([]byte, 9, 64)
		key[8] = '|'
		key = append(key, p.fieldKeys[i]...)

		binary.LittleEndian.PutUint32(key[0:], uint32(len(key)-8))
		line = append(line, key...)

		preQpack := len(line)
		ts, _ := strconv.ParseInt(fmt.Sprintf("%d", p.timestamp.UTC().UnixNano()), 10, 64)
		err := qpack.PackTo(&line, []interface{}{ts, value}) // packs a byte array in the right format for SiriDB
		if err != nil {
			log.Fatal(err)
		}
		postQpack := len(line)

		binary.LittleEndian.PutUint32(line[indexLenData:], uint32(postQpack-preQpack))
		metricCount++
	}

	binary.LittleEndian.PutUint32(line[0:], uint32(metricCount))
	binary.LittleEndian.PutUint32(line[4:], uint32(lenName))

	_, err = w.Write(line)
	return err
}
