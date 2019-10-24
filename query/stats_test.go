package query

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestGetPartialStat(t *testing.T) {
	s := GetPartialStat()

	if !s.isPartial {
		t.Errorf("GetPartialStat() failed - isPartial = false")
	}
	if s.isWarm {
		t.Errorf("GetPartialStat() failed - isWarm = true")
	}
	if len(s.label) > 0 {
		t.Errorf("GetPartialStat() failed - label has non-0 length")
	}
	if s.value != 0.0 {
		t.Errorf("GetPartialStat() failed - value is not 0.0")
	}
}

func TestStatInit(t *testing.T) {
	s := GetStat()
	s.Init([]byte("foo"), 11.0)

	if s.isPartial {
		t.Errorf("Init() failed - isPartial = true")
	}
	if s.isWarm {
		t.Errorf("Init() failed - isWarm = true")
	}
	if len(s.label) == 0 || string(s.label) != "foo" {
		t.Errorf("Init() failed - label is incorrect")
	}
	if s.value != 11.0 {
		t.Errorf("Init() failed - value is not 11.0")
	}
}

func TestStatReset(t *testing.T) {
	s := GetStat()
	s.isPartial = true
	s.isWarm = true
	s.label = []byte("foo")
	s.value = 100.0
	s.reset()
	if s.isPartial {
		t.Errorf("reset() failed - isPartial = true")
	}
	if s.isWarm {
		t.Errorf("reset() failed - isWarm = true")
	}
	if len(s.label) > 0 {
		t.Errorf("reset() failed - label has non-0 length")
	}
	if s.value != 0.0 {
		t.Errorf("reset() failed - value is not 0.0")
	}
}

func TestStateGroupMedian(t *testing.T) {
	cases := []struct {
		len  uint64
		want float64
	}{
		{
			len:  0,
			want: 0.0,
		},
		{
			len:  1,
			want: 1.0,
		},
		{
			len:  5,
			want: 5.0,
		},
		{
			len:  99,
			want: 99.0,
		},
		{
			len:  999,
			want: 999.0,
		},
		{
			len:  9999,
			want: 9999.0,
		},
	}
	errorMargin := 0.0001
	for _, c := range cases {
		sg := newStatGroup(c.len)
		for i := uint64(0); i < c.len; i++ {
			sg.push(1 + float64(i)*2)
		}
		lowerLimit := c.want - (c.want*errorMargin)
		upperLimit := c.want + (c.want*errorMargin)
		if got := sg.median(); ( (lowerLimit > got) && ( got > upperLimit )  && got != 0 ) || got == 0 && got!=c.want {
			t.Errorf("got: %v want C [ %v,%v ]\n", got, lowerLimit, upperLimit)
		}
	}
}

func TestStatGroupMedian0InitialSize(t *testing.T) {
	errorMargin := 0.0001
	cases := []struct {
		len  uint64
		want float64
	}{
		{
			len:  0,
			want: 0.0,
		},
		{
			len:  1,
			want: 1.0,
		},
		{
			len:  5,
			want: 5.0,
		},
		{
			len:  99,
			want: 99.0,
		},
		{
			len:  999,
			want: 999.0,
		},
		{
			len:  9999,
			want: 9999.0,
		},
	}

	for _, c := range cases {
		sg := newStatGroup(0)
		for i := uint64(0); i < c.len; i++ {
			sg.push(1 + float64(i)*2)
		}
		lowerLimit := c.want - (c.want*errorMargin)
		upperLimit := c.want + (c.want*errorMargin)
		if got := sg.median(); ( (lowerLimit > got) && ( got > upperLimit )  && got != 0 ) || got == 0 && got!=c.want {
			t.Errorf("got: %v want C [ %v,%v ]\n", got, lowerLimit, upperLimit)
		}
	}
}

func TestStatGroupPush(t *testing.T) {
	cases := []struct {
		desc      string
		vals      []float64
		wantMin   float64
		wantMax   float64
		wantMean  float64
		wantCount int64
		wantSum   float64
	}{
		{
			desc:      "ordered smallest to largest",
			vals:      []float64{0.0, 1.0, 2.0},
			wantMin:   0.0,
			wantMax:   2.0,
			wantMean:  1.0,
			wantCount: 3,
			wantSum:   3.0,
		},
		{
			desc:      "ordered largest to smallest",
			vals:      []float64{2.0, 1.0, 0.0},
			wantMin:   0.0,
			wantMax:   2.0,
			wantMean:  1.0,
			wantCount: 3,
			wantSum:   3.0,
		},
		{
			desc:      "out of order",
			vals:      []float64{17.0, 10.0, 12.0},
			wantMin:   10.0,
			wantMax:   17.0,
			wantMean:  13.0,
			wantCount: 3,
			wantSum:   39.0,
		},
	}

	for _, c := range cases {
		sg := newStatGroup(0)
		for _, val := range c.vals {
			sg.push(val)
		}
		if got := sg.Min(); got != c.wantMin {
			t.Errorf("%s: incorrect min: got %f want %f", c.desc, got, c.wantMin)
		}
		if got := sg.Max(); got != c.wantMax {
			t.Errorf("%s: incorrect max: got %f want %f", c.desc, got, c.wantMin)
		}
		if got := sg.Mean(); got != c.wantMean {
			t.Errorf("%s: incorrect mean: got %f want %f", c.desc, got, c.wantMin)
		}
		if got := sg.count; got != c.wantCount {
			t.Errorf("%s: incorrect count: got %d want %d", c.desc, got, c.wantCount)
		}
		if got := sg.sum; got != c.wantSum {
			t.Errorf("%s: incorrect sum: got %f want %f", c.desc, got, c.wantMin)
		}
	}
}

const (
	errWriterNormal  = "could not write"
	errWriterSkipOne = "could not write after once"
)

type errWriter struct {
	skipOne bool
	writes  int
}

func (w *errWriter) Write(p []byte) (int, error) {
	if w.skipOne {
		if w.writes > 0 {
			return 0, fmt.Errorf(errWriterSkipOne)
		}
		w.writes++
		return 0, nil
	}
	return 0, fmt.Errorf(errWriterNormal)
}

func TestWrite(t *testing.T) {
	var buf bytes.Buffer
	sg := newStatGroup(0)
	err := sg.write(&buf)
	if err != nil {
		t.Errorf("unexpected error for write: %v", err)
	}
	bArr := buf.Bytes()
	lastCharIdx := len(bArr) - 1
	if got := string(bArr[lastCharIdx:]); got != "\n" {
		t.Errorf("did not end write with a newline: got %v", got)
	}

	// Test error case
	err = sg.write(&errWriter{})
	if err == nil {
		t.Errorf("expected error but did not get one")
	}
}

func TestWriteStatGroupMap(t *testing.T) {
	cases := []struct {
		desc           string
		numGroups      int
		shouldErrLabel bool
		shouldErrStats bool
	}{
		{
			desc:      "no labels",
			numGroups: 0,
		},
		{
			desc:      "one label",
			numGroups: 1,
		},
		{
			desc:      "two labels",
			numGroups: 2,
		},
		{
			desc:      "ten labels",
			numGroups: 10,
		},
		{
			desc:           "err on label",
			numGroups:      1,
			shouldErrLabel: true,
		},
		{
			desc:           "err on stats",
			numGroups:      1,
			shouldErrStats: true,
		},
	}

	for _, c := range cases {
		m := map[string]*statGroup{}
		orderedKeys := []string{}
		for i := 0; i < c.numGroups; i++ {
			sg := newStatGroup(uint64(i))
			label := ""
			for j := 0; j < (i + 1); j++ {
				label += "a"
			}
			m[label] = sg

			// we are generating labels in order
			orderedKeys = append(orderedKeys, label)
		}
		shouldErr := c.shouldErrLabel || c.shouldErrStats

		var w io.Writer
		if c.shouldErrLabel {
			w = &errWriter{}
		} else if c.shouldErrStats {
			w = &errWriter{skipOne: true}
		} else {
			w = bytes.NewBuffer([]byte{})
		}
		err := writeStatGroupMap(w, m)
		if shouldErr {
			ew := w.(*errWriter)
			if err == nil {
				t.Errorf("%s: did not error when it should", c.desc)
			}

			check := func(ew *errWriter, wantWrites int, wantErr string) {
				if ew.writes != wantWrites {
					t.Errorf("%s: too many writes for error case: got %d want %d", c.desc, ew.writes, wantWrites)
				}
				if got := err.Error(); got != wantErr {
					t.Errorf("%s: unexpected err msg: got %s want %s", c.desc, got, wantErr)
				}
			}

			if c.shouldErrLabel {
				check(ew, 0, errWriterNormal)
			} else if c.shouldErrStats {
				check(ew, 1, errWriterSkipOne)
			} else {
				t.Errorf("%s: unexpected condition reached", c.desc)
			}
		} else {
			if err != nil {
				t.Errorf("%s: unexpected error: %v", c.desc, err)
			}
			buf := w.(*bytes.Buffer)
			text := string(buf.Bytes())

			labelIndexes := []int{}
			for _, l := range orderedKeys {
				labelIndexes = append(labelIndexes, strings.Index(text, l))
			}
			// check labels are in order by checking indexes
			prev := -1
			for _, i := range labelIndexes {
				if prev > i {
					t.Errorf("%s: labels not alphabetical: got\n%s", c.desc, text)
				}
				prev = i
			}

			// check that labels are padded correctly
			lines := strings.Split(text, "\n")
			wantLen := c.numGroups*2 + 1 // two per group -- label & metrics -- plus newline
			if got := len(lines); got != wantLen {
				t.Errorf("%s: text is incorrect length: got %d want %d", c.desc, got, wantLen)
			}
			lines = lines[:len(lines)-1] // remove trailing new line
			for i, line := range lines {
				// label lines are every other one
				if i%2 == 0 {
					args := strings.Split(line, ":")
					if got := len(args); got != 2 {
						t.Errorf("%s: invalid label line, more than 2 parts: got %s", c.desc, line)
					}
					if got := len(args[0]); got != c.numGroups {
						t.Errorf("%s: invalid label, not padded: '%s' is only len %d, not %d", c.desc, args[0], len(args[0]), c.numGroups)
					}
				}
			}
		}
	}
}
