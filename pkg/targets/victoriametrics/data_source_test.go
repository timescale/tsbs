package victoriametrics

import (
	"bufio"
	"bytes"
	"github.com/timescale/tsbs/pkg/data"
	"sync"
	"testing"
)

func TestBatch(t *testing.T) {
	f := &factory{bufPool: &sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 16*1024*1024))
		},
	}}
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

	p = data.LoadedPoint{
		Data: []byte("tag1=tag1val,tag2=tag2val col1=1.0,col2=1.0 190"),
	}
	b.Append(p)
	if b.Len() != 2 {
		t.Errorf("batch count is not 1 after first append")
	}
	if b.rows != 2 {
		t.Errorf("batch row count is not 1 after first append")
	}
	if b.metrics != 4 {
		t.Errorf("batch metric count is not 2 after first append")
	}
}

func TestDecode(t *testing.T) {
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
		decoder := &fileDataSource{scanner: bufio.NewScanner(br)}
		p := decoder.NextItem()
		dataBytes := p.Data.([]byte)
		if !bytes.Equal(dataBytes, c.result) {
			t.Errorf("%s: incorrect result: got\n%v\nwant\n%v", c.desc, dataBytes, c.result)
		}
	}
}

func TestDecodeEOF(t *testing.T) {
	input := []byte("cpu,tag1=tag1text,tag2=tag2text col1=0.0,col2=0.0 140")
	br := bufio.NewReader(bytes.NewReader(input))
	decoder := &fileDataSource{scanner: bufio.NewScanner(br)}
	_ = decoder.NextItem()
	// nothing left, should be EOF
	p := decoder.NextItem()
	if p.Data != nil {
		t.Errorf("expected p.Data to be nil, got %v", p.Data)
	}
}
