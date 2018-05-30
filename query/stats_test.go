package query

import "testing"

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
			len:  2,
			want: 2.0,
		},
		{
			len:  4,
			want: 4.0,
		},
		{
			len:  5,
			want: 5.0,
		},
		{
			len:  1000,
			want: 1000,
		},
	}

	for _, c := range cases {
		sg := newStatGroup(c.len)
		for i := uint64(0); i < c.len; i++ {
			sg.push(1 + float64(i)*2)
		}
		if got := sg.median(); c.want != got {
			t.Errorf("got: %v want: %v\n", got, c.want)
		}
	}
}

func TestStatGroupMedian0InitialSize(t *testing.T) {
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
			len:  2,
			want: 2.0,
		},
		{
			len:  4,
			want: 4.0,
		},
		{
			len:  5,
			want: 5.0,
		},
		{
			len:  1000,
			want: 1000,
		},
	}

	for _, c := range cases {
		sg := newStatGroup(0)
		for i := uint64(0); i < c.len; i++ {
			sg.push(1 + float64(i)*2)
		}
		if got := sg.median(); c.want != got {
			t.Errorf("got: %v want: %v\n", got, c.want)
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
		if got := sg.min; got != c.wantMin {
			t.Errorf("%s: incorrect min: got %f want %f", c.desc, got, c.wantMin)
		}
		if got := sg.max; got != c.wantMax {
			t.Errorf("%s: incorrect max: got %f want %f", c.desc, got, c.wantMin)
		}
		if got := sg.mean; got != c.wantMean {
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
