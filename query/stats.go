package query

import (
	"fmt"
	"io"
	"log"
	"math"
	"sort"
	"sync"
)

// Stat represents one statistical measurement.
type Stat struct {
	Label     []byte
	Value     float64
	IsWarm    bool
	IsPartial bool
}

var statPool = &sync.Pool{
	New: func() interface{} {
		return &Stat{
			Label: make([]byte, 0, 1024),
			Value: 0.0,
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
	s.IsPartial = true
	return s
}

// Init safely initializes a (cold) Stat while minimizing heap allocations.
func (s *Stat) Init(label []byte, value float64) *Stat {
	s.Label = s.Label[:0] // clear
	s.Label = append(s.Label, label...)
	s.Value = value
	s.IsWarm = false
	return s
}

func (s *Stat) reset() *Stat {
	s.Label = s.Label[:0]
	s.Value = 0.0
	s.IsWarm = false
	s.IsPartial = false
	return s
}

// StatGroup collects simple streaming statistics.
type StatGroup struct {
	Min    float64
	Max    float64
	Mean   float64
	Sum    float64
	Values []float64

	// used for stddev calculations
	m      float64
	s      float64
	StdDev float64

	Count int64
}

// NewStatGroup returns a new StatGroup with an initial size
func NewStatGroup(size uint64) *StatGroup {
	return &StatGroup{
		Values: make([]float64, size),
		Count:  0,
	}
}

// Median returns the median value of the StatGroup
func (s *StatGroup) Median() float64 {
	sort.Float64s(s.Values[:s.Count])
	if s.Count == 0 {
		return 0
	} else if s.Count%2 == 0 {
		idx := s.Count / 2
		return (s.Values[idx] + s.Values[idx-1]) / 2.0
	} else {
		return s.Values[s.Count/2]
	}
}

// Push updates a StatGroup with a new value.
func (s *StatGroup) Push(n float64) {
	if s.Count == 0 {
		s.Min = n
		s.Max = n
		s.Mean = n
		s.Count = 1
		s.Sum = n

		s.m = n
		s.s = 0.0
		s.StdDev = 0.0
		if len(s.Values) > 0 {
			s.Values[0] = n
		} else {
			s.Values = append(s.Values, n)
		}
		return
	}

	if n < s.Min {
		s.Min = n
	}
	if n > s.Max {
		s.Max = n
	}

	s.Sum += n

	// constant-space mean update:
	sum := s.Mean*float64(s.Count) + n
	s.Mean = sum / float64(s.Count+1)
	if int(s.Count) == len(s.Values) {
		s.Values = append(s.Values, n)
	} else {
		s.Values[s.Count] = n
	}

	s.Count++

	oldM := s.m
	s.m += (n - oldM) / float64(s.Count)
	s.s += (n - oldM) * (n - s.m)
	s.StdDev = math.Sqrt(s.s / (float64(s.Count) - 1.0))
}

// String makes a simple description of a StatGroup.
func (s *StatGroup) String() string {
	return fmt.Sprintf("min: %8.2fms, med: %8.2fms, mean: %8.2fms, max: %7.2fms, stddev: %8.2fms, sum: %5.1fsec, count: %d", s.Min, s.Median(), s.Mean, s.Max, s.StdDev, s.Sum/1e3, s.Count)
}

func (s *StatGroup) Write(w io.Writer) error {
	_, err := fmt.Fprintf(w, "%s\n", s.String())
	return err
}

// WriteStatGroupMap writes a map of StatGroups in an ordered fashion by
// key that they are stored by
func WriteStatGroupMap(w io.Writer, statGroups map[string]*StatGroup) {
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
		paddedKey := fmt.Sprintf("%s", k)
		for len(paddedKey) < maxKeyLength {
			paddedKey += " "
		}

		_, err := fmt.Fprintf(w, "%s:\n", paddedKey)
		if err != nil {
			log.Fatal(err)
		}

		err = v.Write(w)
		if err != nil {
			log.Fatal(err)
		}
	}
}
