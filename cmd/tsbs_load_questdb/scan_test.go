package main

import (
	"bufio"
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/timescale/tsbs/pkg/data"
)

func TestBatch(t *testing.T) {
	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}
	f := &factory{}
	b := f.New().(*batch)
	if b.Len() != 0 {
		t.Errorf("batch not initialized with count 0")
	}
	p := data.LoadedPoint{
		Data: []byte("tag1=tag1val,tag2=tag2val col1=0.0,col2=0.0 140"),
	}
	b.Append(p)
	if b.Len() != 1 {
		t.Errorf("batch count is not 1 after first append")
	}
	if b.rows != 1 {
		t.Errorf("batch row count is not 1 after first append")
	}
	if b.metrics != 2 {
		t.Errorf("batch metric count is not 2 after first append")
	}
	if b.metricsPerRow != 2 {
		t.Errorf("batch metric per row count is not 2 after first append")
	}

	p = data.LoadedPoint{
		Data: []byte("tag1=tag1val,tag2=tag2val col1=1.0,col2=1.0 190"),
	}
	b.Append(p)
	if b.Len() != 2 {
		t.Errorf("batch count is not 1 after second append")
	}
	if b.rows != 2 {
		t.Errorf("batch row count is not 1 after second append")
	}
	if b.metrics != 4 {
		t.Errorf("batch metric count is not 2 after second append")
	}
	if b.metricsPerRow != 2 {
		t.Errorf("batch metric per row count is not 2 after second append")
	}
}

func TestBatchMalformedRow(t *testing.T) {
	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
		},
	}
	f := &factory{}
	b := f.New().(*batch)
	if b.Len() != 0 {
		t.Errorf("batch not initialized with count 0")
	}

	p := data.LoadedPoint{
		Data: []byte("bad_point"),
	}
	errMsg := ""
	fatal = func(f string, args ...interface{}) {
		errMsg = fmt.Sprintf(f, args...)
	}
	b.Append(p)
	if errMsg == "" {
		t.Errorf("batch append did not error with ill-formed point")
	}
}

func TestFileDataSourceNextItem(t *testing.T) {
	cases := []struct {
		desc        string
		input       string
		result      []byte
		shouldFatal bool
	}{
		{
			desc:   "correct input",
			input:  "cpu,tag1=tag1text,tag2=tag2text col1=0.0,col2=0.0 140\n",
			result: []byte("cpu,tag1=tag1text,tag2=tag2text col1=0.0,col2=0.0 140"),
		},
		{
			desc:   "correct input with extra",
			input:  "cpu,tag1=tag1text,tag2=tag2text col1=0.0,col2=0.0 140\nextra_is_ignored",
			result: []byte("cpu,tag1=tag1text,tag2=tag2text col1=0.0,col2=0.0 140"),
		},
	}

	for _, c := range cases {
		br := bufio.NewReader(bytes.NewReader([]byte(c.input)))
		ds := &fileDataSource{scanner: bufio.NewScanner(br)}
		p := ds.NextItem()
		data := p.Data.([]byte)
		if !bytes.Equal(data, c.result) {
			t.Errorf("%s: incorrect result: got\n%v\nwant\n%v", c.desc, data, c.result)
		}
	}
}

func TestDecodeEOF(t *testing.T) {
	input := []byte("cpu,tag1=tag1text,tag2=tag2text col1=0.0,col2=0.0 140")
	br := bufio.NewReader(bytes.NewReader([]byte(input)))
	ds := &fileDataSource{scanner: bufio.NewScanner(br)}
	_ = ds.NextItem()
	// nothing left, should be EOF
	p := ds.NextItem()
	if p.Data != nil {
		t.Errorf("expected p to be nil, got %v", p)
	}
}
