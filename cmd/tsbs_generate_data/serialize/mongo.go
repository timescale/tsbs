package serialize

import (
	"encoding/gob"
	"io"

	"github.com/globalsign/mgo/bson"
)

// MongoSerializer writes a Point in a serialized form for MongoDB
type MongoSerializer struct{}

var enc *gob.Encoder

// Serialize writes Point data to the given Writer, using basic gob encoding
func (s *MongoSerializer) Serialize(p *Point, w io.Writer) (err error) {
	gob.Register(map[string]interface{}{})
	if enc == nil {
		enc = gob.NewEncoder(w)
	}

	fieldsMap := make(map[string]interface{})
	for i, val := range p.fieldKeys {
		fieldsMap[string(val)] = p.fieldValues[i]
	}
	tagsMap := make(map[string]interface{})
	for i, val := range p.tagKeys {
		tagsMap[string(val)] = string(p.tagValues[i])
	}

	pBson := bson.M{
		"measurement":  string(p.MeasurementName()),
		"fields":       fieldsMap,
		"timestamp_ns": p.timestamp.UTC().UnixNano(),
		"tags":         tagsMap,
	}

	err = enc.Encode(pBson)
	if err != nil {
		return err
	}

	return nil
}
