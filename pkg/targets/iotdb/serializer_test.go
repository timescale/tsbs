package iotdb

import (
	"bytes"
	"testing"

	"github.com/apache/iotdb-client-go/client"
	"github.com/stretchr/testify/require"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
)

func TestIotdbFormat(t *testing.T) {
	cases := []struct {
		description  string
		input        interface{}
		expectedByte []byte
		expectedType client.TSDataType
	}{
		{
			description:  "boolean true",
			input:        interface{}(true),
			expectedByte: []byte("true"),
			expectedType: client.BOOLEAN,
		},
		{
			description:  "boolean false",
			input:        interface{}(false),
			expectedByte: []byte("false"),
			expectedType: client.BOOLEAN,
		},
		{
			description:  "int32 -1",
			input:        interface{}(int32(-1)),
			expectedByte: []byte("-1"),
			expectedType: client.INT32,
		},
		{
			description:  "int64 2147483648",
			input:        interface{}(int64(2147483648)),
			expectedByte: []byte("2147483648"),
			expectedType: client.INT64,
		},
		{
			description:  "int64 9223372036854775801",
			input:        interface{}(int64(9223372036854775801)),
			expectedByte: []byte("9223372036854775801"),
			expectedType: client.INT64,
		},
		{
			description:  "float32 0.1",
			input:        interface{}(float32(0.1)),
			expectedByte: []byte("0.1"),
			expectedType: client.FLOAT,
		},
		{
			description:  "float64 0.12345678901234567890123456",
			input:        interface{}(float64(0.12345678901234567890123456)),
			expectedByte: []byte("0.12345678901234568"),
			expectedType: client.DOUBLE,
		},
	}
	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			actualByte, actualType := IotdbFormat(c.input)
			require.EqualValues(t, c.expectedByte, actualByte)
			require.EqualValues(t, c.expectedType, actualType)
		})
	}

}

func TestSerialize_normal(t *testing.T) {
	cases := []struct {
		description string
		inputPoint  *data.Point
		expected    string
	}{
		{
			description: "a regular point ",
			inputPoint:  serialize.TestPointDefault(),
			expected:    "deviceID,timestamp,usage_guest_nice\nroot.cpu.host_0,1451606400000000000,38.24311829\ndatatype,4\ntags,region='eu-west-1',datacenter='eu-west-1b'\n",
		},
		{
			description: "a regular Point using int as value",
			inputPoint:  serialize.TestPointInt(),
			expected:    "deviceID,timestamp,usage_guest\nroot.cpu.host_0,1451606400000000000,38\ndatatype,2\ntags,region='eu-west-1',datacenter='eu-west-1b'\n",
		},
		{
			description: "a regular Point with multiple fields",
			inputPoint:  serialize.TestPointMultiField(),
			expected:    "deviceID,timestamp,big_usage_guest,usage_guest,usage_guest_nice\nroot.cpu.host_0,1451606400000000000,5000000000,38,38.24311829\ndatatype,2,2,4\ntags,region='eu-west-1',datacenter='eu-west-1b'\n",
		},
		{
			description: "a Point with no tags",
			inputPoint:  serialize.TestPointNoTags(),
			expected:    "deviceID,timestamp,usage_guest_nice\nroot.cpu.unknown,1451606400000000000,38.24311829\ndatatype,4\ntags\n",
		},
	}
	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			ps := &Serializer{}
			b := new(bytes.Buffer)
			err := ps.Serialize(c.inputPoint, b)
			require.NoError(t, err)
			actual := b.String()

			require.EqualValues(t, c.expected, actual)
		})
	}

}

func TestSerialize_nonDefaultBasicPath(t *testing.T) {
	cases := []struct {
		description    string
		inputPoint     *data.Point
		BasicPath      string
		BasicPathLevel int32
		expected       string
	}{
		{
			description:    "a regular point ",
			inputPoint:     serialize.TestPointDefault(),
			BasicPath:      "root.sg",
			BasicPathLevel: 1,
			expected:       "deviceID,timestamp,usage_guest_nice\nroot.sg.cpu.host_0,1451606400000000000,38.24311829\ndatatype,4\ntags,region='eu-west-1',datacenter='eu-west-1b'\n",
		},
		{
			description:    "a regular Point using int as value",
			inputPoint:     serialize.TestPointInt(),
			BasicPath:      "root.ln",
			BasicPathLevel: 1,
			expected:       "deviceID,timestamp,usage_guest\nroot.ln.cpu.host_0,1451606400000000000,38\ndatatype,2\ntags,region='eu-west-1',datacenter='eu-west-1b'\n",
		},
		{
			description:    "a regular Point with multiple fields",
			inputPoint:     serialize.TestPointMultiField(),
			BasicPath:      "root.db.abc",
			BasicPathLevel: 2,
			expected:       "deviceID,timestamp,big_usage_guest,usage_guest,usage_guest_nice\nroot.db.abc.cpu.host_0,1451606400000000000,5000000000,38,38.24311829\ndatatype,2,2,4\ntags,region='eu-west-1',datacenter='eu-west-1b'\n",
		},
		{
			description:    "a Point with no tags",
			inputPoint:     serialize.TestPointNoTags(),
			BasicPath:      "root",
			BasicPathLevel: 0,
			expected:       "deviceID,timestamp,usage_guest_nice\nroot.cpu.unknown,1451606400000000000,38.24311829\ndatatype,4\ntags\n",
		},
	}
	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			ps := &Serializer{
				BasicPath:      c.BasicPath,
				BasicPathLevel: c.BasicPathLevel,
			}
			b := new(bytes.Buffer)
			err := ps.Serialize(c.inputPoint, b)
			require.NoError(t, err)
			actual := b.String()

			require.EqualValues(t, c.expected, actual)
		})
	}

}
