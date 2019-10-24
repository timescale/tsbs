package query

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"github.com/filipecosta90/hdrhistogram"
)

// Stat represents one statistical measurement, typically used to store the
// latency of a query (or part of query).
type Stat struct {
	label     []byte
	value     float64
	isWarm    bool
	isPartial bool
}

var statPool = &sync.Pool{
	New: func() interface{} {
		return &Stat{
			label: make([]byte, 0, 1024),
			value: 0.0,
		}
	},
}

// GetStat returns a Stat for use from a pool
func GetStat() *Stat {
	return statPool.Get().(*Stat).reset()
}

// GetPartialStat returns a partial Stat for use from a pool
func GetPartialStat() *Stat {
	s := GetStat()
	s.isPartial = true
	return s
}

// Init safely initializes a Stat while minimizing heap allocations.
func (s *Stat) Init(label []byte, value float64) *Stat {
	s.label = s.label[:0] // clear
	s.label = append(s.label, label...)
	s.value = value
	s.isWarm = false
	return s
}

func (s *Stat) reset() *Stat {
	s.label = s.label[:0]
	s.value = 0.0
	s.isWarm = false
	s.isPartial = false
	return s
}

// statGroup collects simple streaming statistics.
type statGroup struct {
	latencyHDRHistogram *hdrhistogram.Histogram
	sum    float64
	values []float64
	count int64
}

// newStatGroup returns a new StatGroup with an initial size
func newStatGroup(size uint64) *statGroup {
	// This latency Histogram could be used to track and analyze the counts of
	// observed integer values between 0 us and 30000000 us ( 30 secs )
	// while maintaining a value precision of 4 significant digits across that range,
	// translating to a value resolution of :
	//   - 1 microsecond up to 10 milliseconds,
	//   - 100 microsecond (or better) from 10 milliseconds up to 10 seconds,
	//   - 300 microsecond (or better) from 10 seconds up to 30 seconds,
	lH := hdrhistogram.New(1, 30000000, 4)
	return &statGroup{
		values: make([]float64, size),
		count:  0,
		latencyHDRHistogram: lH,
	}
}

// median returns the median value of the StatGroup in milliseconds
func (s *statGroup) median() float64 {
	return float64(s.latencyHDRHistogram.ValueAtQuantile(50.0))/10e2
}

// push updates a StatGroup with a new value.
func (s *statGroup) push(n float64) {
	s.latencyHDRHistogram.RecordValue(int64(n * 10e2))
	if s.count == 0 {
		s.count = 1
		s.sum = n

		if len(s.values) > 0 {
			s.values[0] = n
		} else {
			s.values = append(s.values, n)
		}
		return
	}

	s.sum += n
	s.count++
}

// string makes a simple description of a statGroup.
func (s *statGroup) string() string {
	return fmt.Sprintf("min: %8.2fms, med: %8.2fms, mean: %8.2fms, max: %7.2fms, stddev: %8.2fms, sum: %5.1fsec, count: %d",
		float64(s.latencyHDRHistogram.Min())/10e2,
		s.median(),
		float64(s.latencyHDRHistogram.Mean())/10e2,
		float64(s.latencyHDRHistogram.Max())/10e2,
		float64(s.latencyHDRHistogram.StdDev())/10e2,
		s.sum/1e3,
		s.count)
}

func (s *statGroup) write(w io.Writer) error {
	_, err := fmt.Fprintln(w, s.string())
	return err
}

func (s *statGroup) Mean() float64 {
	return float64(s.latencyHDRHistogram.Mean())/10e2
}

func (s *statGroup) Max() float64 {
	return float64(s.latencyHDRHistogram.Max())/10e2
}

func (s *statGroup) Min() float64 {
	return float64(s.latencyHDRHistogram.Min())/10e2
}

// writeStatGroupMap writes a map of StatGroups in an ordered fashion by
// key that they are stored by
func writeStatGroupMap(w io.Writer, statGroups map[string]*statGroup) error {
	maxKeyLength := 0
	keys := make([]string, 0, len(statGroups))
	for k := range statGroups {
		if len(k) > maxKeyLength {
			maxKeyLength = len(k)
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := statGroups[k]
		paddedKey := k
		for len(paddedKey) < maxKeyLength {
			paddedKey += " "
		}

		_, err := fmt.Fprintf(w, "%s:\n", paddedKey)
		if err != nil {
			return err
		}

		err = v.write(w)
		if err != nil {
			return err
		}
	}
	return nil
}
