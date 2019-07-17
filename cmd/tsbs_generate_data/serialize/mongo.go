package serialize

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"
)

var fbBuilderPool = &sync.Pool{
	New: func() interface{} {
		return flatbuffers.NewBuilder(0)
	},
}

// MongoSerializer writes a Point in a serialized form for MongoDB
type MongoSerializer struct{}

// Serialize writes Point data to the given Writer, using basic gob encoding
func (s *MongoSerializer) Serialize(p *Point, w io.Writer) (err error) {
	b := fbBuilderPool.Get().(*flatbuffers.Builder)

	timestampNanos := p.timestamp.UTC().UnixNano()
	tags := []flatbuffers.UOffsetT{}
	// In order to keep the ordering the same on deserialization, we need
	// to go in reverse order since we are prepending rather than appending.
	for i := len(p.tagKeys); i > 0; i-- {
		switch v := p.tagValues[i-1].(type) {
		case string:
			k := string(p.tagKeys[i-1])
			key := b.CreateString(k)
			val := b.CreateString(v)
			MongoTagStart(b)
			MongoTagAddKey(b, key)
			MongoTagAddValue(b, val)
			tags = append(tags, MongoTagEnd(b))
		case nil:
			continue
		default:
			panic("non-string tags not implemented for mongo db")
		}
	}
	MongoPointStartTagsVector(b, len(tags))
	for _, t := range tags {
		b.PrependUOffsetT(t)
	}
	tagsArr := b.EndVector(len(tags))

	fields := []flatbuffers.UOffsetT{}
	// In order to keep the ordering the same on deserialization, we need
	// to go in reverse order since we are prepending rather than appending.
	for i := len(p.fieldKeys); i > 0; i-- {
		val := p.fieldValues[i-1]
		if val == nil {
			continue
		}
		newField := createField(b, p.fieldKeys[i-1], val)
		fields = append(fields, newField)
	}
	MongoPointStartFieldsVector(b, len(fields))
	for _, f := range fields {
		b.PrependUOffsetT(f)
	}
	fieldsArr := b.EndVector(len(fields))

	measurement := b.CreateString(string(p.measurementName))
	MongoPointStart(b)
	MongoPointAddMeasurementName(b, measurement)
	MongoPointAddTimestamp(b, timestampNanos)
	MongoPointAddTags(b, tagsArr)
	MongoPointAddFields(b, fieldsArr)
	point := MongoPointEnd(b)
	b.Finish(point)
	buf := b.FinishedBytes()

	// Write the metadata for the flatbuffer object:
	lenBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(lenBuf, uint64(len(buf)))
	_, err = w.Write(lenBuf)
	if err != nil {
		return err
	}

	// Write the flatbuffer object:
	_, err = w.Write(buf)
	if err != nil {
		return err
	}

	// Give the flatbuffers builder back to a pool:
	b.Reset()
	fbBuilderPool.Put(b)

	return nil
}

func createField(b *flatbuffers.Builder, key []byte, val interface{}) flatbuffers.UOffsetT {
	keyStr := b.CreateString(string(key))
	MongoReadingStart(b)
	MongoReadingAddKey(b, keyStr)
	prependValue(b, val)
	return MongoReadingEnd(b)
}
func prependValue(b *flatbuffers.Builder, value interface{}) {
	switch val := value.(type) {
	case float64:
		MongoReadingAddValue(b, val)
	case float32:
		MongoReadingAddValue(b, float64(val))
	case int:
		MongoReadingAddValue(b, float64(val))
	case int64:
		MongoReadingAddValue(b, float64(val))
	default:
		panic(fmt.Sprintf("cannot covert %T to float64", val))
	}
}
