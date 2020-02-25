package mongo

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"io"
	"log"
	"testing"

	flatbuffers "github.com/google/flatbuffers/go"
)

func TestMongoSerializerSerialize(t *testing.T) {
	type output struct {
		name        string
		ts          int64
		tagKeys     [][]byte
		tagVals     []interface{}
		readingKeys [][]byte
		readingVals []interface{}
	}
	cases := []struct {
		desc       string
		inputPoint *data.Point
		want       output
	}{
		{
			desc:       "a regular Point",
			inputPoint: serialize.TestPointDefault(),
			want: output{
				name:        string(serialize.TestMeasurement),
				ts:          serialize.TestNow.UnixNano(),
				tagKeys:     serialize.TestTagKeys,
				tagVals:     serialize.TestTagVals,
				readingKeys: serialize.TestPointDefault().FieldKeys(),
				readingVals: serialize.TestPointDefault().FieldValues(),
			},
		},
		{
			desc:       "a regular Point using int as value",
			inputPoint: serialize.TestPointInt(),
			want: output{
				name:        string(serialize.TestMeasurement),
				ts:          serialize.TestNow.UnixNano(),
				tagKeys:     serialize.TestTagKeys,
				tagVals:     serialize.TestTagVals,
				readingKeys: serialize.TestPointInt().FieldKeys(),
				readingVals: serialize.TestPointInt().FieldValues(),
			},
		},
		{
			desc:       "a regular Point with multiple fields",
			inputPoint: serialize.TestPointMultiField(),
			want: output{
				name:        string(serialize.TestMeasurement),
				ts:          serialize.TestNow.UnixNano(),
				tagKeys:     serialize.TestTagKeys,
				tagVals:     serialize.TestTagVals,
				readingKeys: serialize.TestPointMultiField().FieldKeys(),
				readingVals: serialize.TestPointMultiField().FieldValues(),
			},
		},
		{
			desc:       "a Point with no tags",
			inputPoint: serialize.TestPointNoTags(),
			want: output{
				name:        string(serialize.TestMeasurement),
				ts:          serialize.TestNow.UnixNano(),
				tagKeys:     [][]byte{},
				tagVals:     []interface{}{},
				readingKeys: serialize.TestPointNoTags().FieldKeys(),
				readingVals: serialize.TestPointNoTags().FieldValues(),
			},
		},
	}

	ps := &Serializer{}
	for _, c := range cases {
		b := new(bytes.Buffer)
		ps.Serialize(c.inputPoint, b)
		br := bufio.NewReader(bytes.NewReader(b.Bytes()))
		mp := deserializeMongo(br)

		if got := string(mp.MeasurementName()); got != c.want.name {
			t.Errorf("%s: incorrect measreuement name: got %s want %s", c.desc, got, c.want.name)
		}
		if got := mp.Timestamp(); got != c.want.ts {
			t.Errorf("%s: incorrect timestamp: got %d want %d", c.desc, got, c.want.ts)
		}

		// Verify tags
		if got := mp.TagsLength(); got != len(c.want.tagKeys) {
			t.Errorf("%s: incorrect tag keys length: got %d want %d", c.desc, got, len(c.want.tagKeys))
		}
		if got := mp.TagsLength(); got != len(c.want.tagVals) {
			t.Errorf("%s: incorrect tag vals length: got %d want %d", c.desc, got, len(c.want.tagVals))
		}
		tag := &MongoTag{}
		for i := 0; i < mp.TagsLength(); i++ {
			mp.Tags(tag, i)
			want := string(c.want.tagKeys[i])
			if got := string(tag.Key()); got != want {
				t.Errorf("%s: incorrect tag key %d: got %s want %s", c.desc, i, got, want)
			}
			want = c.want.tagVals[i].(string)
			if got := string(tag.Value()); got != want {
				t.Errorf("%s: incorrect tag val %d: got %s want %s", c.desc, i, got, want)
			}
		}

		// Verify fields
		if got := mp.FieldsLength(); got != len(c.want.readingKeys) {
			t.Errorf("%s: incorrect reading keys length: got %d want %d", c.desc, got, len(c.want.readingKeys))
		}
		if got := mp.FieldsLength(); got != len(c.want.readingVals) {
			t.Errorf("%s: incorrect reading vals length: got %d want %d", c.desc, got, len(c.want.readingVals))
		}

		reading := &MongoReading{}
		for i := 0; i < mp.FieldsLength(); i++ {
			mp.Fields(reading, i)
			want := string(c.want.readingKeys[i])
			if got := string(reading.Key()); got != want {
				t.Errorf("%s: incorrect reading key %d: got %s want %s", c.desc, i, got, want)
			}

			var wantVal float64
			switch x := c.want.readingVals[i].(type) {
			case int:
				wantVal = float64(x)
			case int64:
				wantVal = float64(x)
			case float64:
				wantVal = x
			}
			if got := reading.Value(); got != wantVal {
				t.Errorf("%s: incorrect reading val %d: got %v want %v", c.desc, i, got, wantVal)
			}
		}
	}
}

func deserializeMongo(r *bufio.Reader) *MongoPoint {
	item := &MongoPoint{}
	lenBuf := make([]byte, 8)

	_, err := r.Read(lenBuf)
	if err == io.EOF {
		return nil
	}
	if err != nil {
		log.Fatal(err.Error())
	}

	// ensure correct len of receiving buffer
	l := int(binary.LittleEndian.Uint64(lenBuf))
	itemBuf := make([]byte, l)

	// read the bytes and init the flatbuffer object
	totRead := 0
	for totRead < l {
		m, err := r.Read(itemBuf[totRead:])
		// (EOF is also fatal)
		if err != nil {
			log.Fatal(err.Error())
		}
		totRead += m
	}
	if totRead != len(itemBuf) {
		panic(fmt.Sprintf("reader/writer logic error, %d != %d", totRead, len(itemBuf)))
	}
	n := flatbuffers.GetUOffsetT(itemBuf)
	item.Init(itemBuf, n)

	return item
}

func TestMongoSerializerTypePanic(t *testing.T) {
	testPanic := func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("did not panic when should")
			}
		}()
		p := &data.Point{}
		p.SetMeasurementName(serialize.TestMeasurement)
		p.SetTimestamp(&serialize.TestNow)
		p.AppendField([]byte("broken"), "a string?")
		ps := &Serializer{}
		b := new(bytes.Buffer)

		ps.Serialize(p, b)
	}
	testPanic()
}

func TestMongoSerializerSerializeErr(t *testing.T) {
	p := serialize.TestPointMultiField()
	s := &Serializer{}
	err := s.Serialize(p, &serialize.ErrWriter{})
	if err == nil {
		t.Errorf("no error returned when expected")
	} else if err.Error() != serialize.ErrWriterAlwaysErr {
		t.Errorf("unexpected writer error: %v", err)
	}

	// check second error condition works
	err = s.Serialize(p, &serialize.ErrWriter{SkipOne: true})
	if err == nil {
		t.Errorf("no error returned when expected")
	} else if err.Error() != serialize.ErrWriterSometimesErr {
		t.Errorf("unexpected writer error: %v", err)
	}
}
