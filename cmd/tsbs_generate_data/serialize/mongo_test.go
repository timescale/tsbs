package serialize

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
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
		inputPoint *Point
		want       output
	}{
		{
			desc:       "a regular Point",
			inputPoint: testPointDefault,
			want: output{
				name:        string(testMeasurement),
				ts:          testNow.UnixNano(),
				tagKeys:     testTagKeys,
				tagVals:     testTagVals,
				readingKeys: testPointDefault.fieldKeys,
				readingVals: testPointDefault.fieldValues,
			},
		},
		{
			desc:       "a regular Point using int as value",
			inputPoint: testPointInt,
			want: output{
				name:        string(testMeasurement),
				ts:          testNow.UnixNano(),
				tagKeys:     testTagKeys,
				tagVals:     testTagVals,
				readingKeys: testPointInt.fieldKeys,
				readingVals: testPointInt.fieldValues,
			},
		},
		{
			desc:       "a regular Point with multiple fields",
			inputPoint: testPointMultiField,
			want: output{
				name:        string(testMeasurement),
				ts:          testNow.UnixNano(),
				tagKeys:     testTagKeys,
				tagVals:     testTagVals,
				readingKeys: testPointMultiField.fieldKeys,
				readingVals: testPointMultiField.fieldValues,
			},
		},
		{
			desc:       "a Point with no tags",
			inputPoint: testPointNoTags,
			want: output{
				name:        string(testMeasurement),
				ts:          testNow.UnixNano(),
				tagKeys:     [][]byte{},
				tagVals:     []interface{}{},
				readingKeys: testPointNoTags.fieldKeys,
				readingVals: testPointNoTags.fieldValues,
			},
		},
	}

	ps := &MongoSerializer{}
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
		p := &Point{
			measurementName: testMeasurement,
			timestamp:       &testNow,
		}
		p.AppendField([]byte("broken"), "a string?")
		ps := &MongoSerializer{}
		b := new(bytes.Buffer)

		ps.Serialize(p, b)
	}
	testPanic()
}

func TestMongoSerializerSerializeErr(t *testing.T) {
	p := testPointMultiField
	s := &MongoSerializer{}
	err := s.Serialize(p, &errWriter{})
	if err == nil {
		t.Errorf("no error returned when expected")
	} else if err.Error() != errWriterAlwaysErr {
		t.Errorf("unexpected writer error: %v", err)
	}

	// check second error condition works
	err = s.Serialize(p, &errWriter{skipOne: true})
	if err == nil {
		t.Errorf("no error returned when expected")
	} else if err.Error() != errWriterSometimesErr {
		t.Errorf("unexpected writer error: %v", err)
	}
}
