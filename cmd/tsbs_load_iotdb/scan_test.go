package main

import (
	"testing"

	"github.com/apache/iotdb-client-go/client"
	"github.com/stretchr/testify/require"
)

func TestGenerateInsertStatement(t *testing.T) {
	cases := []struct {
		description string
		lines       []string
		expected    iotdbPoint
	}{
		{
			description: "one point",
			lines: []string{
				"deviceID,timestamp,value",
				"root.cpu.host_9,1451606400000000000,3.1415926",
				"datatype,4",
				"tags",
			},
			expected: iotdbPoint{
				deviceID:     "root.cpu.host_9",
				timestamp:    1451606400000,
				measurements: []string{"value"},
				values:       []interface{}{float64(3.1415926)},
				dataTypes:    []client.TSDataType{client.DOUBLE},
				fieldsCnt:    1,
			},
		},
		{
			description: "one point with different dataTypes",
			lines: []string{
				"deviceID,timestamp,floatV,strV,int64V,int32V,boolV",
				"root.cpu.host_0,1451606400000000000,3.1415926,hello,123,123,true",
				"datatype,4,5,2,1,0",
				"tags",
			},
			expected: iotdbPoint{
				deviceID:     "root.cpu.host_0",
				timestamp:    1451606400000,
				measurements: []string{"floatV", "strV", "int64V", "int32V", "boolV"},
				values:       []interface{}{float64(3.1415926), string("hello"), int64(123), int32(123), true},
				dataTypes:    []client.TSDataType{client.DOUBLE, client.TEXT, client.INT64, client.INT32, client.BOOLEAN},
				fieldsCnt:    5,
			},
		},
	}

	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			require.True(t, len(c.lines) == 3)
			actual := parseFourLines(c.lines[0], c.lines[1], c.lines[2], c.lines[3])
			require.EqualValues(t, &c.expected, actual.Data.(*iotdbPoint))
		})
	}
}
