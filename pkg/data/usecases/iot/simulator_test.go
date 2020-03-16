package iot

import (
	"bytes"
	"fmt"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"reflect"
	"testing"
	"time"
)

var (
	fieldCount = 5
	tagCount   = 5
	pointCount = 14
	buf        = &bytes.Buffer{}
)

type mockBaseSimulator struct {
	pending []*data.Point
	fields  map[string][]string
	tagKeys []string
	current int
	now     *time.Time
}

func (m *mockBaseSimulator) Finished() bool {
	return m.current >= len(m.pending)
}

func (m *mockBaseSimulator) Next(p *data.Point) bool {
	if m.Finished() {
		return false
	}
	p.Copy(m.pending[m.current])
	m.current++

	return true
}

func (m *mockBaseSimulator) Fields() map[string][]string {
	return m.fields
}

func (m *mockBaseSimulator) TagKeys() []string {
	return m.tagKeys
}

func (m *mockBaseSimulator) TagTypes() []string {
	return nil
}

func (m *mockBaseSimulator) Headers() *common.GeneratedDataHeaders {
	return &common.GeneratedDataHeaders{
		TagTypes:  m.TagTypes(),
		TagKeys:   m.TagKeys(),
		FieldKeys: m.Fields(),
	}
}
func newMockBaseSimulator() *mockBaseSimulator {
	fields := make(map[string][][]byte, fieldCount)
	fieldKeys := make([][]byte, fieldCount)
	tagKeys := make([][]byte, tagCount)
	pending := make([]*data.Point, pointCount)

	for i := 0; i < fieldCount; i++ {
		fieldKeys[i] = []byte(fmt.Sprintf("field_key_%d", i))
	}

	for i := 0; i < fieldCount; i++ {
		fields[fmt.Sprintf("measurement_%d", i)] = fieldKeys
	}

	for i := 0; i < tagCount; i++ {
		tagKeys[i] = []byte(fmt.Sprintf("tag_key_%d", i))
	}

	now := time.Now()

	for i := 0; i < pointCount; i++ {
		pending[i] = data.NewPoint()
		pending[i].SetTimestamp(&now)
		pending[i].SetMeasurementName([]byte(fmt.Sprintf("measurement_%d", i%fieldCount)))

		for j := 0; j < tagCount; j++ {
			pending[i].AppendTag(tagKeys[j], []byte(fmt.Sprintf("tag_value_%d_%d", i, j)))
		}

		fieldKey := fields[fmt.Sprintf("measurement_%d", i%fieldCount)]

		for j := 0; j < fieldCount; j++ {
			pending[i].AppendField(fieldKey[j], fmt.Sprintf("field_value_%d_%d", i, j))
		}
	}

	fieldsAsStr := make(map[string][]string, fieldCount)
	for k := range fields {
		fieldValsAsBytes := fields[k]
		fieldValsAsStr := make([]string, len(fieldValsAsBytes))
		for i, x := range fieldValsAsBytes {
			fieldValsAsStr[i] = string(x)
		}
		fieldsAsStr[k] = fieldValsAsStr
	}
	tagKeysAsStr := make([]string, tagCount)
	for i, tagKey := range tagKeys {
		tagKeysAsStr[i] = string(tagKey)
	}
	return &mockBaseSimulator{
		pending: pending,
		fields:  fieldsAsStr,
		tagKeys: tagKeysAsStr,
		now:     &now,
	}
}

func checkResults(initial []*data.Point, results []*data.Point, expectedOrder []int) (int, bool) {
	for i, expected := range expectedOrder {
		if results[i] == nil {
			return i, false
		}
		if initial[expected] == nil {
			return i, false
		}

		if !pointsEqual(initial[expected], results[i]) {
			return i, false
		}
	}

	return 0, true
}
func pointsEqual(one *data.Point, two *data.Point) bool {
	if !bytes.Equal(one.MeasurementName(), two.MeasurementName()) {
		return false
	}
	if !one.Timestamp().Equal(*two.Timestamp()) {
		return false
	}
	if len(one.TagKeys()) != len(two.TagKeys()) {
		return false
	}
	for i, tagKey := range one.TagKeys() {
		if string(tagKey) != string(two.TagKeys()[i]) {
			return false
		}
		x := one.GetTagValue(tagKey)
		y := one.GetTagValue(tagKey)
		if x == nil && y == nil {
			continue
		} else if x == nil {
			return false
		} else if y == nil {
			return false
		}
		if string(one.GetTagValue(tagKey).([]byte)) != string(two.GetTagValue(tagKey).([]byte)) {
			return false
		}
	}
	if len(one.FieldKeys()) != len(two.FieldKeys()) {
		return false
	}
	for i, fieldKey := range one.FieldKeys() {
		if string(fieldKey) != string(two.FieldKeys()[i]) {
			return false
		}
		x := one.GetFieldValue(fieldKey)
		y := one.GetFieldValue(fieldKey)
		if x == nil && y == nil {
			continue
		} else if x == nil {
			return false
		} else if y == nil {
			return false
		}
		if x.(string) != y.(string) {
			return false
		}
	}
	return true
}

func TestSimulatorNext(t *testing.T) {
	cases := []struct {
		desc                string
		config              func(batchSize int) func(int, int, int, int) *batchConfig
		resultsPerBatchSize map[int][]int
		zeroFieldsResults   map[int][]int
		zeroTagsResults     map[int][]int
	}{
		{
			desc: "no config",
			config: func(batchSize int) func(i, j, k, z int) *batchConfig {
				return func(i, j, k, z int) *batchConfig {
					return &batchConfig{}
				}
			},
			resultsPerBatchSize: map[int][]int{
				0:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				1:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				3:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				5:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				10: {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
			},
		},
		{
			desc: "all batches missing",
			config: func(batchSize int) func(i, j, k, z int) *batchConfig {
				return func(i, j, k, z int) *batchConfig {
					return &batchConfig{
						Missing: true,
					}
				}
			},
			resultsPerBatchSize: map[int][]int{
				0:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				1:  {},
				3:  {},
				5:  {},
				10: {},
			},
		},
		{
			// Since we append all out of order stuff at the end, should have
			// same results as no config.
			desc: "all batches out of order",
			config: func(batchSize int) func(i, j, k, z int) *batchConfig {
				return func(i, j, k, z int) *batchConfig {
					return &batchConfig{
						OutOfOrder: true,
					}
				}
			},
			resultsPerBatchSize: map[int][]int{
				0:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				1:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				3:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				5:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				10: {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
			},
		},
		{
			desc: "first entry of every batch missing",
			config: func(batchSize int) func(i, j, k, z int) *batchConfig {
				return func(i, j, k, z int) *batchConfig {
					return &batchConfig{
						MissingEntries: map[int]bool{0: true},
					}
				}
			},
			resultsPerBatchSize: map[int][]int{
				0:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				1:  {1, 3, 5, 7, 9, 11, 13},
				3:  {1, 2, 3, 5, 6, 7, 9, 10, 11, 13},
				5:  {1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 13},
				10: {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13},
			},
		},
		{
			desc: "last entry of every batch missing",
			config: func(batchSize int) func(i, j, k, z int) *batchConfig {
				return func(i, j, k, z int) *batchConfig {
					return &batchConfig{
						MissingEntries: map[int]bool{batchSize - 1: true},
					}
				}
			},
			resultsPerBatchSize: map[int][]int{
				0:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				1:  {1, 3, 5, 7, 9, 11, 13},
				3:  {0, 1, 3, 4, 5, 7, 8, 9, 11, 12, 13},
				5:  {0, 1, 2, 3, 5, 6, 7, 8, 9, 11, 12, 13},
				10: {0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13},
			},
		},
		{
			desc: "first entry of every batch out of order",
			config: func(batchSize int) func(i, j, k, z int) *batchConfig {
				return func(i, j, k, z int) *batchConfig {
					return &batchConfig{
						OutOfOrderEntries: map[int]bool{0: true},
					}
				}
			},
			resultsPerBatchSize: map[int][]int{
				0:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				1:  {1, 3, 5, 7, 9, 11, 13, 0, 2, 4, 6, 8, 10, 12},
				3:  {1, 2, 3, 5, 6, 7, 9, 10, 11, 13, 0, 4, 8, 12},
				5:  {1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 13, 0, 6, 12},
				10: {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 0, 11},
			},
		},
		{
			desc: "last entry of every batch out of order",
			config: func(batchSize int) func(i, j, k, z int) *batchConfig {
				return func(i, j, k, z int) *batchConfig {
					return &batchConfig{
						OutOfOrderEntries: map[int]bool{batchSize - 1: true},
					}
				}
			},
			resultsPerBatchSize: map[int][]int{
				0:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				1:  {1, 3, 5, 7, 9, 11, 13, 0, 2, 4, 6, 8, 10, 12},
				3:  {0, 1, 3, 4, 5, 7, 8, 9, 11, 12, 13, 2, 6, 10},
				5:  {0, 1, 2, 3, 5, 6, 7, 8, 9, 11, 12, 13, 4, 10},
				10: {0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 9},
			},
		},
		{
			desc: "insert first batch at the end",
			config: func(batchSize int) func(i, j, k, z int) *batchConfig {
				return func(i, j, k, z int) *batchConfig {
					return &batchConfig{
						OutOfOrder: i == 0,
					}
				}
			},
			resultsPerBatchSize: map[int][]int{
				0:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				1:  {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0},
				3:  {3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0, 1, 2},
				5:  {5, 6, 7, 8, 9, 10, 11, 12, 13, 0, 1, 2, 3, 4},
				10: {10, 11, 12, 13, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			},
		},
		{
			desc: "make every batch out of order and insert right away",
			config: func(batchSize int) func(i, j, k, z int) *batchConfig {
				return func(i, j, k, z int) *batchConfig {
					return &batchConfig{
						OutOfOrder:     true,
						InsertPrevious: i > 0,
					}
				}
			},
			resultsPerBatchSize: map[int][]int{
				0:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				1:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				3:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				5:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				10: {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
			},
		},
		{
			desc: "insert last entry of previous batch as last entry of next batch",
			config: func(batchSize int) func(i, j, k, z int) *batchConfig {
				return func(i, j, k, z int) *batchConfig {
					insertPreviousEntry := make(map[int]bool)
					if j > 0 {
						insertPreviousEntry[batchSize-1] = true
					}
					return &batchConfig{
						OutOfOrderEntries:   map[int]bool{batchSize - 1: true},
						InsertPreviousEntry: insertPreviousEntry,
					}
				}
			},
			resultsPerBatchSize: map[int][]int{
				0:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				1:  {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0},
				3:  {0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 2},
				5:  {0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 4},
				10: {0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 9},
			},
		},
		{
			desc: "insert first entry of previous batch as last entry of next batch",
			config: func(batchSize int) func(i, j, k, z int) *batchConfig {
				return func(i, j, k, z int) *batchConfig {
					insertPreviousEntry := make(map[int]bool)
					if j > 0 {
						insertPreviousEntry[batchSize-1] = true
					}
					return &batchConfig{
						OutOfOrderEntries:   map[int]bool{0: true},
						InsertPreviousEntry: insertPreviousEntry,
					}
				}
			},
			resultsPerBatchSize: map[int][]int{
				0:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				1:  {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0},
				3:  {1, 2, 3, 5, 0, 6, 8, 4, 9, 11, 7, 12, 10, 13},
				5:  {1, 2, 3, 4, 5, 7, 8, 9, 0, 10, 12, 13, 6, 11},
				10: {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 0, 11},
			},
		},
		{
			desc: "insert multiple out of order entries sequentially",
			config: func(batchSize int) func(i, j, k, z int) *batchConfig {
				return func(i, j, k, z int) *batchConfig {
					insertPreviousEntry := make(map[int]bool)
					if j > 0 {
						for index := 0; index < j; index++ {
							insertPreviousEntry[index] = true
						}
					}
					return &batchConfig{
						OutOfOrderEntries:   map[int]bool{0: true, 1: true},
						InsertPreviousEntry: insertPreviousEntry,
					}
				}
			},
			resultsPerBatchSize: map[int][]int{
				0:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				1:  {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0, 1},
				3:  {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0, 1},
				5:  {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0, 1},
				10: {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 0, 1},
			},
		},
		{
			desc: "zero first field of the first entry for all batches",
			config: func(batchSize int) func(i, j, k, z int) *batchConfig {
				return func(i, j, k, z int) *batchConfig {
					return &batchConfig{
						ZeroFields: map[int]int{0: 0},
					}
				}
			},
			resultsPerBatchSize: map[int][]int{
				0:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				1:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				3:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				5:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				10: {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
			},
			zeroFieldsResults: map[int][]int{
				0:  {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1},
				1:  {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				3:  {0, -1, -1, 0, -1, -1, 0, -1, -1, 0, -1, -1, 0, -1},
				5:  {0, -1, -1, -1, -1, 0, -1, -1, -1, -1, 0, -1, -1, -1},
				10: {0, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0, -1, -1, -1},
			},
		},
		{
			desc: "zero 3rd tag of the last entry for all batches",
			config: func(batchSize int) func(i, j, k, z int) *batchConfig {
				return func(i, j, k, z int) *batchConfig {
					return &batchConfig{
						ZeroTags: map[int]int{batchSize - 1: 3},
					}
				}
			},
			resultsPerBatchSize: map[int][]int{
				0:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				1:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				3:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				5:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				10: {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
			},
			zeroTagsResults: map[int][]int{
				0:  {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1},
				1:  {3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3},
				3:  {-1, -1, 3, -1, -1, 3, -1, -1, 3, -1, -1, 3, -1, -1},
				5:  {-1, -1, -1, -1, 3, -1, -1, -1, -1, 3, -1, -1, -1, -1},
				10: {-1, -1, -1, -1, -1, -1, -1, -1, -1, 3, -1, -1, -1, -1},
			},
		},
		{
			desc: "combine both zero field and zero tag",
			config: func(batchSize int) func(i, j, k, z int) *batchConfig {
				return func(i, j, k, z int) *batchConfig {
					return &batchConfig{
						ZeroFields: map[int]int{0: 0},
						ZeroTags:   map[int]int{batchSize - 1: 3},
					}
				}
			},
			resultsPerBatchSize: map[int][]int{
				0:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				1:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				3:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				5:  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
				10: {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
			},
			zeroFieldsResults: map[int][]int{
				0:  {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1},
				1:  {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				3:  {0, -1, -1, 0, -1, -1, 0, -1, -1, 0, -1, -1, 0, -1},
				5:  {0, -1, -1, -1, -1, 0, -1, -1, -1, -1, 0, -1, -1, -1},
				10: {0, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0, -1, -1, -1},
			},
			zeroTagsResults: map[int][]int{
				0:  {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1},
				1:  {3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3},
				3:  {-1, -1, 3, -1, -1, 3, -1, -1, 3, -1, -1, 3, -1, -1},
				5:  {-1, -1, -1, -1, 3, -1, -1, -1, -1, 3, -1, -1, -1, -1},
				10: {-1, -1, -1, -1, -1, -1, -1, -1, -1, 3, -1, -1, -1, -1},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			for batchSize, result := range c.resultsPerBatchSize {
				t.Run(fmt.Sprintf("batch size %d", batchSize), func(t *testing.T) {
					m := newMockBaseSimulator()
					s := &Simulator{
						base:            m,
						batchSize:       uint(batchSize),
						configGenerator: c.config(batchSize),
					}

					results := make([]*data.Point, 0)

					for i := 0; i < pointCount; i++ {
						point := data.NewPoint()
						valid := s.Next(point)
						if !valid {
							break
						}
						results = append(results, point)
					}

					if !s.Finished() {
						t.Errorf("simulator not finished, should be done")
					}

					if len(result) != len(results) {
						t.Fatalf("simulator didn't return correct number of points, got %d want %d", len(results), len(result))
					}

					// If we are checking zeros, we cannot check for equality since
					// a zero field or a zero tag will create a difference.
					if c.zeroFieldsResults[batchSize] != nil || c.zeroTagsResults[batchSize] != nil {
						for i := range results {
							got := m.pending[result[i]]
							fieldKeys := got.FieldKeys()
							tagKeys := got.TagKeys()
							zeroFields := c.zeroFieldsResults[batchSize]
							zeroTags := c.zeroTagsResults[batchSize]
							if zeroFields != nil && i < len(zeroFields) && zeroFields[i] >= 0 {
								got.ClearFieldValue(fieldKeys[zeroFields[i]])
							}

							if zeroTags != nil && i < len(zeroTags) && zeroTags[i] >= 0 {
								got.ClearTagValue(tagKeys[zeroTags[i]])
							}

							if !pointsEqual(got, results[i]) {
								t.Errorf("result entry at index %d has wrong zero field and/or zero tag:\ngot\n%v\nwant\n%v", i, got, results[i])
							}
						}

					} else {
						if i, ok := checkResults(m.pending, results, result); !ok {
							t.Errorf("results not as expected at index %d:\ngot\n%v\nwant\n%v", i, results[i], m.pending[result[i]])
						}
					}
				})
			}
		})
	}

}

func TestSimulatorTagTypes(t *testing.T) {
	sc := &SimulatorConfig{
		Start: time.Now(),
		End:   time.Now(),

		InitGeneratorScale:   1,
		GeneratorScale:       1,
		GeneratorConstructor: NewTruck,
	}
	s := sc.NewSimulator(time.Second, 1).(*Simulator)
	p := data.NewPoint()
	s.Next(p)
	tagTypes := s.TagTypes()
	for i, pointTagKey := range p.TagKeys() {
		value := p.GetTagValue(pointTagKey)
		tagType := reflect.TypeOf(value)
		if tagType.String() != tagTypes[i] {
			t.Errorf("incorrect tag type. expected %v, got %v", tagTypes[i], tagType)
		}
	}
}
