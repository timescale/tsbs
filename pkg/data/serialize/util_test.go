package serialize

import (
	"testing"
)

func TestFastFormatAppend(t *testing.T) {
	cases := []struct {
		desc        string
		inputString []byte
		input       interface{}
		output      []byte
		shouldPanic bool
	}{
		{
			desc:        "fastFormatAppend should properly append a float64 to a given byte string",
			inputString: []byte("values,"),
			input:       float64(29.37),
			output:      []byte("values,29.37"),
			shouldPanic: false,
		},
		{
			desc:        "fastFormatAppend should properly append a float32 to a given byte string",
			inputString: []byte("values,"),
			input:       float32(29.37),
			output:      []byte("values,29.37"),
			shouldPanic: false,
		},
		{
			desc:        "fastFormatAppend should properly append an int to a given byte string",
			inputString: []byte("values,"),
			input:       int(29),
			output:      []byte("values,29"),
			shouldPanic: false,
		},
		{
			desc:        "fastFormatAppend should properly append an int to a given byte string",
			inputString: []byte("values,"),
			input:       int64(5000000000),
			output:      []byte("values,5000000000"),
			shouldPanic: false,
		},
		{
			desc:        "fastFormatAppend should properly append a byte string to a given byte string",
			inputString: []byte("values,"),
			input:       []byte("bytestring"),
			output:      []byte("values,bytestring"),
			shouldPanic: false,
		},
		{
			desc:        "fastFormatAppend should properly append a string to a given byte string",
			inputString: []byte("values,"),
			input:       "string",
			output:      []byte("values,string"),
			shouldPanic: false,
		},
		{
			desc:        "fastFormatAppend should properly append a boolean to a given byte string",
			inputString: []byte("values,"),
			input:       true,
			output:      []byte("values,true"),
			shouldPanic: false,
		},
		{
			desc:        "fastFormatAppend should panic when given an unsupported type",
			inputString: []byte("values,"),
			input:       []int{},
			output:      []byte("values,true"),
			shouldPanic: true,
		},
	}

	testPanic := func(input interface{}, inputString []byte, desc string) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("%s: did not panic when should", desc)
			}
		}()
		FastFormatAppend(input, inputString)
	}

	for _, c := range cases {
		if c.shouldPanic == true {
			testPanic(c.input, c.inputString, c.desc)
		} else {
			got := FastFormatAppend(c.input, c.inputString)
			if string(got) != string(c.output) {
				t.Errorf("%s \nOutput incorrect: Want: %s Got: %s", c.desc, c.output, got)
			}
		}
	}
}
