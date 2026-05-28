package utils

import (
	"strings"
	"testing"
)

func isEquals(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i, value := range left {
		if value != right[i] {
			return false
		}
	}
	return true
}

func formatStrings(values []string) string {
	if len(values) == 0 {
		return "[]"
	} else {
		return "[\"" + strings.Join(values, "\", \"") + "\"]"
	}
}

func TestInfluxLineParsing(t *testing.T) {
	cases := []struct {
		desc     string
		input    string
		expected []string
	}{
		{
			desc:  "simple line",
			input: "measurement,tag1_name=tag1_value,tag2_name=tag2_value field1_name=field1_value,field1_name=field2_value 1556813561098000000",
			expected: []string{
				"measurement,tag1_name=tag1_value,tag2_name=tag2_value",
				"field1_name=field1_value,field1_name=field2_value",
				"1556813561098000000",
			},
		},
		{
			desc:  "line with single quoted tag",
			input: "measurement,\"tag1 name=tag1_value,tag2_name=tag2 value\" field1_name=field1_value,field1_name=field2_value 1556813561098000000",
			expected: []string{
				"measurement,\"tag1 name=tag1_value,tag2_name=tag2 value\"",
				"field1_name=field1_value,field1_name=field2_value",
				"1556813561098000000",
			},
		},
		{
			desc:  "line with multiple quoted tags",
			input: "measurement,\"tag1 name\"=tag1_value,tag2_name=\"tag2 value\" field1_name=field1_value,field1_name=field2_value 1556813561098000000",
			expected: []string{
				"measurement,\"tag1 name\"=tag1_value,tag2_name=\"tag2 value\"",
				"field1_name=field1_value,field1_name=field2_value",
				"1556813561098000000",
			},
		},
		{
			desc:  "line with single quoted field",
			input: "measurement,tag1_name=tag1_value,tag2_name=tag2_value \"field1 name=field1_value,field1_name=field2 value\" 1556813561098000000",
			expected: []string{
				"measurement,tag1_name=tag1_value,tag2_name=tag2_value",
				"\"field1 name=field1_value,field1_name=field2 value\"",
				"1556813561098000000",
			},
		},
		{
			desc:  "line with multiple quoted fields",
			input: "measurement,tag1_name=tag1_value,tag2_name=tag2_value \"field1 name\"=field1_value,field1_name=\"field2 value\" 1556813561098000000",
			expected: []string{
				"measurement,tag1_name=tag1_value,tag2_name=tag2_value",
				"\"field1 name\"=field1_value,field1_name=\"field2 value\"",
				"1556813561098000000",
			},
		},
		{
			desc:  "line with escaped quotas",
			input: "measurement,tag1_name=tag1_value,tag2_name=\\\"tag2_value field1_name\\\"=field1_value,field1_name=field2_value 1556813561098000000",
			expected: []string{
				"measurement,tag1_name=tag1_value,tag2_name=\\\"tag2_value",
				"field1_name\\\"=field1_value,field1_name=field2_value",
				"1556813561098000000",
			},
		},
	}

	delim := ' '
	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			actual := SplitLine(c.input, delim)
			if !isEquals(actual, c.expected) {
				t.Errorf("%s: expected '%s', actual: '%s'", c.desc, formatStrings(c.expected), formatStrings(actual))
			}
		})
	}
}

func TestInfluxFieldsParsing(t *testing.T) {
	cases := []struct {
		desc     string
		input    string
		expected []string
	}{
		{
			desc:  "simple line",
			input: "measurement,tag1_name=tag1_value,tag2_name=tag2_value",
			expected: []string{
				"measurement",
				"tag1_name=tag1_value",
				"tag2_name=tag2_value",
			},
		},
		{
			desc:  "line with quoted value with delimiter",
			input: "measurement,tag1_name=\"tag1_value,tag2_name=tag2_value\"",
			expected: []string{
				"measurement",
				"tag1_name=\"tag1_value,tag2_name=tag2_value\"",
			},
		},
		{
			desc:  "line with quoted value without delimiter",
			input: "measurement,tag1_name=\"tag1_value\",tag2_name=tag2_value",
			expected: []string{
				"measurement",
				"tag1_name=\"tag1_value\"",
				"tag2_name=tag2_value",
			},
		},
	}

	delim := ','
	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			actual := SplitLine(c.input, delim)
			if !isEquals(actual, c.expected) {
				t.Errorf("%s: expected '%s', actual: '%s'", c.desc, formatStrings(c.expected), formatStrings(actual))
			}
		})
	}
}
