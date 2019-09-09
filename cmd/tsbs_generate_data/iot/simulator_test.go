package iot

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_data/serialize"
)

var (
	fieldCount = 5
	tagCount   = 5
	pointCount = 14
	serializer = serialize.TimescaleDBSerializer{}
	buf        = &bytes.Buffer{}
)

type mockBaseSimulator struct {
	pending []*serialize.Point
	fields  map[string][][]byte
	tagKeys [][]byte
	current int
	now     *time.Time
}

func (m *mockBaseSimulator) Finished() bool {
	return m.current >= len(m.pending)
}

func (m *mockBaseSimulator) Next(p *serialize.Point) bool {
	if m.Finished() {
		return false
	}
	p.Copy(m.pending[m.current])
	m.current++

	return true
}

func (m *mockBaseSimulator) Fields() map[string][][]byte {
	return m.fields
}

func (m *mockBaseSimulator) TagKeys() [][]byte {
	return m.tagKeys
}

func (m *mockBaseSimulator) TagTypes() []reflect.Type {
	return nil
}

func newMockBaseSimulator() *mockBaseSimulator {
	fields := make(map[string][][]byte, fieldCount)
	fieldKeys := make([][]byte, fieldCount)
	tagKeys := make([][]byte, tagCount)
	pending := make([]*serialize.Point, pointCount)

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
		pending[i] = serialize.NewPoint()
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

	return &mockBaseSimulator{
		pending: pending,
		fields:  fields,
		tagKeys: tagKeys,
		now:     &now,
	}
}

func checkResults(initial []*serialize.Point, results []*serialize.Point, expectedOrder []int) (int, bool) {
	for i, expected := range expectedOrder {
		if results[i] == nil {
			return i, false
		}
		if initial[expected] == nil {
			return i, false
		}
		want := toString(initial[expected])
		got := toString(results[i])

		if got != want {
			return i, false
		}
	}

	return 0, true
}

func toString(p *serialize.Point) string {
	buf.Reset()
	serializer.Serialize(p, buf)
	return buf.String()
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

					results := make([]*serialize.Point, 0)

					for i := 0; i < pointCount; i++ {
						point := serialize.NewPoint()
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
							resultString := toString(results[i])
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

							if toString(got) != resultString {
								t.Errorf("result entry at index %d has wrong zero field and/or zero tag:\ngot\n%s\nwant\n%s", i, resultString, toString(got))
							}
						}

					} else {
						if i, ok := checkResults(m.pending, results, result); !ok {
							t.Errorf("results not as expected at index %d:\ngot\n%s\nwant\n%s", i, toString(results[i]), toString(m.pending[result[i]]))
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
	p := serialize.NewPoint()
	s.Next(p)
	tagTypes := s.TagTypes()
	for i, pointTagKey := range p.TagKeys() {
		value := p.GetTagValue(pointTagKey)
		tagType := reflect.TypeOf(value)
		if tagType != tagTypes[i] {
			t.Errorf("incorrect tag type. expected %v, got %v", tagTypes[i], tagType)
		}
	}
}
