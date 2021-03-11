package main

import (
	"testing"

	"github.com/timescale/tsbs/pkg/data"
)

func TestBatch(t *testing.T) {
	f := &factory{}
	b := f.New().(*batch)
	if b.Len() != 0 {
		t.Errorf("batch not initialized with count 0")
	}
	p := data.LoadedPoint{
		Data: &point{
			data: map[string][]byte{
				"measurementName|tag1=val1,tag2=val2|fieldKey1": []byte{1, 2},
				"measurementName|tag1=val1,tag2=val2|fieldKey2": []byte{2, 3},
			},
			dataCnt: 2,
		},
	}
	b.Append(p)
	if b.Len() != 1 {
		t.Errorf("batch count is not 1 after first append")
	}
	if b.metricCnt != 2 {
		t.Errorf("batch metric count is not 2 after first append")
	}

	p = data.LoadedPoint{
		Data: &point{
			data: map[string][]byte{
				"measurementName|tag1=val1,tag2=val2|fieldKey3": []byte{3, 4},
				"measurementName|tag1=val1,tag2=val2|fieldKey4": []byte{4, 5},
			},
			dataCnt: 2,
		},
	}
	b.Append(p)
	if b.Len() != 2 {
		t.Errorf("batch count is not 1 after first append")
	}
	if b.metricCnt != 4 {
		t.Errorf("batch metric count is not 2 after first append")
	}
}
