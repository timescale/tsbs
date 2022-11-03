package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateInsertStatement(t *testing.T) {
	cases := []struct {
		description string
		point       iotdbPoint
		expected    string
	}{
		{
			description: "one point(1)",
			point: iotdbPoint{
				deviceID:      "root.cpu.host_0",
				fieldKeyStr:   "timestamp,value,str",
				fieldValueStr: "123456,999,'abc'",
				fieldsCnt:     3,
			},
			expected: "INSERT INTO root.cpu.host_0(timestamp,value,str) VALUES(123456,999,'abc')",
		},
		{
			description: "one point(2)",
			point: iotdbPoint{
				deviceID:      "root.cpu.host_9",
				fieldKeyStr:   "timestamp,floatValue,str,intValue",
				fieldValueStr: "123456,4321.9,'abc',45621",
				fieldsCnt:     4,
			},
			expected: "INSERT INTO root.cpu.host_9(timestamp,floatValue,str,intValue) VALUES(123456,4321.9,'abc',45621)",
		},
	}

	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			actual := c.point.generateInsertStatement()
			require.EqualValues(t, c.expected, actual)
		})
	}
}
