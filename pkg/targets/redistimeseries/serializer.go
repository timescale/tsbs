package redistimeseries

import (
	"fmt"
	"github.com/mediocregopher/radix/v3"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"io"
)

var keysSoFar map[string]bool
var hashSoFar map[string][]byte

// Serializer writes a Point in a serialized form for RedisTimeSeries
type Serializer struct{}

// Serialize writes Point data to the given writer, in a format that will be easy to create a redis-timeseries command
// from.
//
// This function writes output that looks like:
// [CLUSTER SLOT of {host_0}] cpu_{host_0}_usage_user 1451606400 58 LABELS hostname host_0 region eu-central-1 ... measurement cpu fieldname usage_user
//
// Which the loader will decode into a set of TS.CREATE commands for each fieldKey. Once labels have been created for a each fieldKey,
// subsequent rows are omitted with them and are ingested with TS.MADD for a row's metrics.
func (s *Serializer) Serialize(p *data.Point, w io.Writer) (err error) {
	if keysSoFar == nil {
		keysSoFar = make(map[string]bool)
	}

	if hashSoFar == nil {
		hashSoFar = make(map[string][]byte)
	}

	var hashBytes []byte
	//var hashExists bool
	p.TagValues()
	hostname := []byte(fmt.Sprintf("%s", p.TagValues()[0]))
	labelsHash := int(radix.ClusterSlot(hostname))
	hashBytes = serialize.FastFormatAppend(labelsHash, []byte{})

	for fieldID := 0; fieldID < len(p.FieldKeys()); fieldID++ {
		fieldName := p.FieldKeys()[fieldID]
		keyName := fmt.Sprintf("%s%s", hostname, fieldName)

		// if this key was already inserted and created, we don't to specify the labels again
		if keysSoFar[keyName] == false {
			w.Write(hashBytes)
			w.Write([]byte(" TS.CREATE "))
			writeKeyName(w, p, hostname, fieldName)
			w.Write([]byte("LABELS"))
			for i, v := range p.TagValues() {
				w.Write([]byte(" "))
				w.Write(p.TagKeys()[i])
				w.Write([]byte(" "))
				w.Write(serialize.FastFormatAppend(v, []byte{}))
			}
			w.Write([]byte(" measurement "))
			// add measurement name as additional label to be used in queries
			w.Write(p.MeasurementName())

			// additional label of fieldname
			w.Write([]byte(" fieldname "))
			w.Write(fieldName)
			w.Write([]byte("\n"))
			keysSoFar[keyName] = true
		}
	}
	w.Write(hashBytes)
	w.Write([]byte(" TS.MADD "))

	for fieldID := 0; fieldID < len(p.FieldKeys()); fieldID++ {
		fieldName := p.FieldKeys()[fieldID]
		fieldValue := p.FieldValues()[fieldID]
		writeKeyName(w, p, hostname, fieldName)
		writeTS_and_Value(w, p, fieldValue)
		if fieldID < len(p.FieldKeys())-1 {
			w.Write([]byte(" "))
		}
	}
	w.Write([]byte("\n"))

	return err
}

func writeTS_and_Value(w io.Writer, p *data.Point, fieldValue interface{}) (err error) {
	// write timestamp in ms
	w.Write(serialize.FastFormatAppend(p.Timestamp().UTC().Unix()*1000, []byte{}))
	w.Write([]byte(" "))
	// write value
	_, err = w.Write(serialize.FastFormatAppend(fieldValue, []byte{}))
	return
}

func writeKeyName(w io.Writer, p *data.Point, hostname []byte, fieldName []byte) (err error) {
	w.Write([]byte("{"))
	w.Write(hostname)
	w.Write([]byte("}_"))
	w.Write(p.MeasurementName())
	w.Write([]byte("_"))
	w.Write(fieldName)
	w.Write([]byte(" "))
	return
}
