package query

import (
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/HdrHistogram/hdrhistogram-go"
)

var (
	hdrScaleFactor = 1e3
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
	sum                 float64
	count               int64
	intervalHistogram   *hdrhistogram.Histogram
	intervalSum         float64
	intervalCount       int64
}

// newStatGroup returns a new StatGroup with an initial size
func newStatGroup(size uint64) *statGroup {
	// This latency Histogram could be used to track and analyze the counts of
	// observed integer values between 0 us and 3600000000 us ( 3600 secs )
	// while maintaining a value precision of 3 significant digits across that range,
	// translating to a value resolution of :
	//   - 1 microsecond up to 10 millisecond,
	//   - 10 millisecond (or better) from 10 millisecond up to 10 seconds,
	//   - 1 second (or better) from 10 second up to 3600 seconds,
	lH := hdrhistogram.New(1, 3600000000, 4)
	iH := hdrhistogram.New(1, 3600000000, 4)
	return &statGroup{
		latencyHDRHistogram: lH,
		sum:                 0,
		count:               0,
		intervalHistogram:   iH,
		intervalSum:         0,
		intervalCount:       0,
	}
}

// push updates a StatGroup with a new value.
func (s *statGroup) push(n float64) {
	s.latencyHDRHistogram.RecordValue(int64(n * hdrScaleFactor))
	s.sum += n
	s.count++
	s.intervalHistogram.RecordValue(int64(n * hdrScaleFactor))
	s.intervalSum += n
	s.intervalCount++
}

// string makes a simple description of a statGroup.
func (s *statGroup) string() string {
	vl := fmt.Sprintf("min: %8.2fms, med: %8.2fms, mean: %8.2fms, max: %7.2fms, stddev: %8.2fms, sum: %5.1fsec, count: %d",
		s.Min(),
		s.Median(),
		s.Mean(),
		s.Max(),
		s.StdDev(),
		s.sum/hdrScaleFactor,
		s.count)
	vi := ""
	if s.intervalCount != 0 {
		vi = fmt.Sprintf("\nmin: %8.2fms, med: %8.2fms, mean: %8.2fms, max: %7.2fms, stddev: %8.2fms, sum: %5.1fsec, count: %d",
			s.MinInterval(),
			s.MedianInterval(),
			s.MeanInterval(),
			s.MaxInterval(),
			s.StdDevInterval(),
			s.intervalSum/hdrScaleFactor,
			s.intervalCount)
	}
	s.intervalSum = 0
	s.intervalCount = 0
	s.intervalHistogram.Reset()
	return vl + vi
}

func (s *statGroup) write(w io.Writer) error {
	_, err := fmt.Fprintln(w, s.string())
	return err
}

// Median returns the Median value of the StatGroup in milliseconds
func (s *statGroup) Median() float64 {
	return float64(s.latencyHDRHistogram.ValueAtQuantile(50.0)) / hdrScaleFactor
}

// Mean returns the Mean value of the StatGroup in milliseconds
func (s *statGroup) Mean() float64 {
	return float64(s.latencyHDRHistogram.Mean()) / hdrScaleFactor
}

// Max returns the Max value of the StatGroup in milliseconds
func (s *statGroup) Max() float64 {
	return float64(s.latencyHDRHistogram.Max()) / hdrScaleFactor
}

// Min returns the Min value of the StatGroup in milliseconds
func (s *statGroup) Min() float64 {
	return float64(s.latencyHDRHistogram.Min()) / hdrScaleFactor
}

// StdDev returns the StdDev value of the StatGroup in milliseconds
func (s *statGroup) StdDev() float64 {
	return float64(s.latencyHDRHistogram.StdDev()) / hdrScaleFactor
}

func (s *statGroup) MedianInterval() float64 {
	return float64(s.intervalHistogram.ValueAtQuantile(50.0)) / hdrScaleFactor
}

func (s *statGroup) MeanInterval() float64 {
	return float64(s.intervalHistogram.Mean()) / hdrScaleFactor
}

func (s *statGroup) MaxInterval() float64 {
	return float64(s.intervalHistogram.Max()) / hdrScaleFactor
}

func (s *statGroup) MinInterval() float64 {
	return float64(s.intervalHistogram.Min()) / hdrScaleFactor
}

func (s *statGroup) StdDevInterval() float64 {
	return float64(s.intervalHistogram.StdDev()) / hdrScaleFactor
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
