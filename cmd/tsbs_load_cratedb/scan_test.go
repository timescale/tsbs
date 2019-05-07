package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/timescale/tsbs/load"
)

func TestEventsBatch(t *testing.T) {
	f := &factory{}
	eb := f.New().(*eventsBatch)
	if eb.Len() != 0 {
		t.Errorf("eventBatch must be empty")
	}
	points := []*load.Point{
		{
			Data: &point{
				table: "type1",
				row: row{[]byte(
					"{\"t1\":\"v1\",\"t2\":\"v2\"}"),
					time.Unix(0, 1451606400000000000),
					55.0,
				},
			},
		},
		{
			Data: &point{
				table: "type2",
				row: row{
					[]byte("{\"t1\":\"v1\",\"t2\":\"v2\"}"),
					time.Unix(0, 1451606400000000000),
					55,
				},
			},
		},
		{
			Data: &point{
				table: "type2",
				row: row{
					[]byte("{\"t1\":\"v1\",\"t2\":\"v2\"}"),
					time.Unix(0, 1451606400000000000),
					0, 55.0, 1,
				},
			},
		},
		{
			Data: &point{
				table: "type2",
				row: row{
					[]byte("{\"t1\":\"v1\",\"t2\":\"v2\"}"),
					time.Unix(0, 1557147988108),
					11, 55.0,
				},
			},
		},
	}
	for _, p := range points {
		eb.Append(p)
	}
	if eb.Len() != len(points) {
		t.Errorf(fmt.Sprintf("eventsBatch must have %d points", len(points)))
	}
	if len(eb.batches) != 2 {
		t.Errorf("eventsBatch must have two measurement types")
	}
}

func TestDecode(t *testing.T) {
	cases := []struct {
		desc           string
		input          string
		expectedTable  string
		expectedRow    row
		expectedToFail bool
	}{
		{
			desc:          "correct input",
			input:         "cpu\t{\"hostname\":\"host_0\"}\t1454608400000000000\t38.243",
			expectedTable: "cpu",
			expectedRow: row{
				[]byte("{\"hostname\":\"host_0\"}"),
				time.Unix(0, 1454608400000000000),
				38.243, 1,
			},
		},
		{
			desc:          "correct input: empty tags",
			input:         "mem\tnull\t1454608400000000000\t38.24311829",
			expectedTable: "mem",
			expectedRow: row{
				[]byte("null"),
				time.Unix(0, 1454608400000000000),
				38.24311829,
			},
		},
		{
			desc:           "incorrect input:, missing timestamp",
			input:          "mem\tnull\t\t38.24311829",
			expectedToFail: true,
		},
		{
			desc:           "incorrect input: missing metrics",
			input:          "mem\tnull\t1454608400000000000",
			expectedToFail: true,
		},
		{
			desc:           "incorrect input: malformed",
			input:          "..",
			expectedToFail: true,
		},
	}
	for _, c := range cases {
		br := bufio.NewReader(bytes.NewReader([]byte(c.input)))
		decoder := &decoder{scanner: bufio.NewScanner(br)}
		if c.expectedToFail {
			fmt.Println(c.desc)
			isCalled := false
			fatal = func(fmt string, args ...interface{}) {
				isCalled = true
				log.Printf(fmt, args...)
			}
			_ = decoder.Decode(br)
			if !isCalled {
				t.Errorf("%s: did not call fatal when it should", c.desc)
			}
		} else {
			p := decoder.Decode(br)
			data := p.Data.(*point)
			if data.table != c.expectedTable {
				t.Errorf(
					"%s: incorrect prefix: got %s want %s",
					c.desc,
					data.table,
					c.expectedTable,
				)
			}
			for i, value := range data.row {
				if !reflect.DeepEqual(value, c.expectedRow[i]) {
					t.Errorf(
						"%s: incorrect fields: got %s want %s",
						c.desc,
						value,
						c.expectedRow[i],
					)
				}
			}
		}
	}
}

func TestDecodeEOF(t *testing.T) {
	input := []byte("cpu\t{\"hostname\":\"host_0\"}\t1454608400000000000\t38.24311829\n")
	br := bufio.NewReader(bytes.NewReader([]byte(input)))
	decoder := &decoder{scanner: bufio.NewScanner(br)}
	_ = decoder.Decode(br)
	// nothing left, should be EOF
	p := decoder.Decode(br)
	if p != nil {
		t.Errorf("expected p to be nil, got %v", p)
	}
}
