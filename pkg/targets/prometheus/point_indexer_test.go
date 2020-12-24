package prometheus

import (
	"math/rand"
	"testing"

	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/tsbs/pkg/data"
)

func TestRandomPointIndexer(t *testing.T) {
	rand.Seed(0)
	var expected []uint
	pointsToIndex := 10
	numPartitions := 100
	for i := 0; i < pointsToIndex; i++ {
		expected = append(expected, uint(rand.Intn(numPartitions)))
	}
	rand.Seed(0)
	indexer := randomPointIndexer{numPartitions}
	for i := 0; i < pointsToIndex; i++ {
		index := indexer.GetIndex(data.LoadedPoint{})
		if index != expected[i] {
			t.Errorf("expected: %d, got: %d", expected[i], index)
		}
	}
}

func TestSeriesIDPointIndexer(t *testing.T) {
	oneLabel := []prompb.Label{{
		Name:  "l1",
		Value: "l1_v",
	}}
	twoLabels := []prompb.Label{{
		Name:  "l1",
		Value: "l1_v",
	}, {
		Name:  "l2",
		Value: "l2_v",
	}}

	var t1, t2 *prompb.TimeSeries
	pi := newSeriesIDPointIndexer(1000)

	t1 = &prompb.TimeSeries{
		Labels: oneLabel,
	}
	t2 = &prompb.TimeSeries{
		Labels: twoLabels,
	}

	t1Index := pi.GetIndex(data.NewLoadedPoint(t1))

	// test cache is properly filled
	piCasted := pi.(*seriesIDPointIndexer)
	t1CacheKey := nilDelimitedLabelsToStr(t1.Labels)
	cachedIndex, exists := piCasted.indexCache.Load(t1CacheKey)
	if !exists {
		t.Fatalf("cache not properly populated, val for %s is missing", t1CacheKey)
	}
	if cachedIndex.(uint) != t1Index {
		t.Fatalf("cache not properly populated, expected val %d, got %d", t1Index, cachedIndex.(int))
	}

	// test cache is used
	//// will change cached value, if changed value is returned, then it means
	//// the correct value is not calculated and stored
	modifiedIndex := cachedIndex.(uint) + 1
	piCasted.indexCache.Store(t1CacheKey, modifiedIndex)
	if modifiedIndex != pi.GetIndex(data.NewLoadedPoint(t1)) {
		t.Fatal("cached index value not used")
	}

	// test diff values for different label sets
	piCasted.indexCache.Delete(t1CacheKey)
	t1Index = piCasted.GetIndex(data.NewLoadedPoint(t1))
	t2Index := piCasted.GetIndex(data.NewLoadedPoint(t2))
	t2CacheKey := nilDelimitedLabelsToStr(t2.Labels)
	if cachedT2Index, exists := piCasted.indexCache.Load(t2CacheKey); !exists {
		t.Fatal("index of t2 not cached")
	} else if cachedT2Index != t2Index {
		t.Fatalf("for t2 cached index %d, returned %d", cachedT2Index, t2Index)
	}
	// for the selected label set and maxIndex indexes shouldn't be the same
	if t1Index == t2Index {
		t.Fatalf("two diff labelsets got same index")
	}
}

func TestNilDelimitedLabelsToStr(t *testing.T) {
	testCases := []struct {
		desc string
		in   []prompb.Label
		out  string
	}{
		{desc: "empty label set", in: []prompb.Label{}, out: ""},
		{
			desc: "one label",
			in: []prompb.Label{{
				Name:  "l1",
				Value: "l1_v",
			}},
			out: "l1\x00l1_v",
		}, {
			desc: "two labels",
			in: []prompb.Label{{
				Name:  "l1",
				Value: "l1_v",
			}, {
				Name:  "l2",
				Value: "l2_v",
			}},
			out: "l1\x00l1_v\x00\x00l2\x00l2_v",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			got := nilDelimitedLabelsToStr(tc.in)
			if got != tc.out {
				t.Errorf("expected: %s; got: %s", tc.out, got)
			}
		})
	}
}
