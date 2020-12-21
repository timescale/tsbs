package serialize

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"
)

// RedisTimeSeriesSerializer writes a Point in a serialized form for RedisTimeSeries
type RedisTimeSeriesSerializer struct{}

var keysSoFar map[string]bool
var hashSoFar map[string][]byte

// Serialize writes Point data to the given writer, in a format that will be easy to create a redis-timeseries command
// from.
//
// This function writes output that looks like:
//cpu_usage_user{md5(hostname=host_0|region=eu-central-1...)} 1451606400 58 LABELS hostname host_0 region eu-central-1 ... measurement cpu fieldname usage_user
//
// Which the loader will decode into a set of TS.ADD commands for each fieldKey. Once labels have been created for a each fieldKey,
// subsequent rows are ommitted with them and are ingested with TS.MADD for a row's metrics.
func (s *RedisTimeSeriesSerializer) Serialize(p *Point, w io.Writer) (err error) {
	if keysSoFar == nil {
		keysSoFar = make(map[string]bool)
	}

	if hashSoFar == nil {
		hashSoFar = make(map[string][]byte)
	}

	var hashBytes []byte
	//var hashExists bool
	hostname := p.tagValues[0]

	for fieldID := 0; fieldID < len(p.fieldKeys); fieldID++ {
		fieldName := p.fieldKeys[fieldID]
		keyName := fmt.Sprintf("%s%s", hostname, fieldName)
		//fmt.Errorf("%s\n",fieldName)
		//if hashBytes, hashExists = hashSoFar[keyName]; hashExists == false {
		//do something here
		labelsHash := md5.Sum([]byte(fmt.Sprintf("%s", hostname)))
		hashBytes = fastFormatAppend(int(binary.BigEndian.Uint32(labelsHash[:])), []byte{})
		//hashSoFar[keyName] = hashBytes
		//}

		// if this key was already inserted and created, we don't to specify the labels again
		if keysSoFar[keyName] == false {
			w.Write([]byte("TS.CREATE "))
			writeKeyName(w, p, fieldName, hashBytes)
			w.Write([]byte("LABELS"))
			for i, v := range p.tagValues {
				w.Write([]byte(" "))
				w.Write(p.tagKeys[i])
				w.Write([]byte(" "))
				w.Write(fastFormatAppend(v, []byte{}))
			}
			w.Write([]byte(" measurement "))
			// add measurement name as additional label to be used in queries
			w.Write(p.measurementName)

			// additional label of fieldname
			w.Write([]byte(" fieldname "))
			w.Write(fieldName)
			w.Write([]byte("\n"))
			keysSoFar[keyName] = true
		}
	}
	w.Write([]byte("TS.MADD "))

	for fieldID := 0; fieldID < len(p.fieldKeys); fieldID++ {
		fieldName := p.fieldKeys[fieldID]

		//keyName := fmt.Sprintf("%s%s", hostname, fieldName)
		//fmt.Fprint(os.Stderr, fmt.Sprintf("%s\n", keyName))

		labelsHash := md5.Sum([]byte(fmt.Sprintf("%s", hostname)))
		hashBytes = fastFormatAppend(int(binary.BigEndian.Uint32(labelsHash[:])), []byte{})

		fieldValue := p.fieldValues[fieldID]
		writeKeyName(w, p, fieldName, hashBytes)
		writeTS_and_Value(w, p, fieldValue)
		if fieldID < len(p.fieldKeys)-1 {
			w.Write([]byte(" "))
		}
	}
	w.Write([]byte("\n"))

	return err
}

func appendTS_and_Value(lbuf []byte, p *Point, fieldValue interface{}) []byte {
	// write timestamp in ms
	lbuf = fastFormatAppend(p.timestamp.UTC().Unix()*1000, lbuf)
	lbuf = append(lbuf, ' ')
	// write value
	lbuf = fastFormatAppend(fieldValue, lbuf)
	return lbuf
}

func writeTS_and_Value(w io.Writer, p *Point, fieldValue interface{}) (err error) {
	// write timestamp in ms
	w.Write(fastFormatAppend(p.timestamp.UTC().Unix()*1000, []byte{}))
	w.Write([]byte(" "))
	// write value
	_, err = w.Write(fastFormatAppend(fieldValue, []byte{}))
	return
}

func appendKeyName(lbuf []byte, p *Point, fieldName []byte, hashBytes []byte) []byte {
	lbuf = append(lbuf, p.measurementName...)
	lbuf = append(lbuf, '_')
	lbuf = append(lbuf, fieldName...)

	lbuf = append(lbuf, '{')
	lbuf = append(lbuf, hashBytes...)
	lbuf = append(lbuf, '}', ' ')
	return lbuf
}

func writeKeyName(w io.Writer, p *Point, fieldName []byte, hashBytes []byte) (err error) {
	w.Write(p.measurementName)
	w.Write([]byte("_"))
	w.Write(fieldName)
	w.Write([]byte("{"))
	w.Write(hashBytes)
	_, err = w.Write([]byte("} "))
	return
}
