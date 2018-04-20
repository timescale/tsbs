package serialize

import (
	"encoding/binary"
	"io"

	flatbuffers "github.com/google/flatbuffers/go"
)

// MongoSerializer writes a Point in a serialized form for MongoDB
type MongoSerializer struct{}

// Serialize writes Point data to the given Writer, using basic gob encoding
func (s *MongoSerializer) Serialize(p *Point, w io.Writer) (err error) {
	lenBuf := make([]byte, 8)
	b := fbBuilderPool.Get().(*flatbuffers.Builder)

	timestampNanos := p.timestamp.UTC().UnixNano()

	fieldsMap := make(map[string]interface{})
	for i, val := range p.fieldKeys {
		fieldsMap[string(val)] = p.fieldValues[i]
	}
	tagsMap := make(map[string]string)
	for i, val := range p.tagKeys {
		tagsMap[string(val)] = string(p.tagValues[i])
	}

	tags := []flatbuffers.UOffsetT{}
	for k, v := range tagsMap {
		key := b.CreateString(k)
		val := b.CreateString(v)
		MongoTagStart(b)
		MongoTagAddKey(b, key)
		MongoTagAddValue(b, val)
		tags = append(tags, MongoTagEnd(b))
	}
	MongoPointStartTagsVector(b, len(tags))
	for _, t := range tags {
		b.PrependUOffsetT(t)
	}
	tagsArr := b.EndVector(len(tags))

	fields := []flatbuffers.UOffsetT{}
	for k, v := range fieldsMap {
		key := b.CreateString(k)
		val := v.(float64)
		MongoReadingStart(b)
		MongoReadingAddKey(b, key)
		MongoReadingAddValue(b, val)
		fields = append(fields, MongoReadingEnd(b))
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
