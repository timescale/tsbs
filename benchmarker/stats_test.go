package benchmarker

import "testing"

func TestMedian(t *testing.T) {
	cases := []struct {
		len  uint64
		want float64
	}{
		{
			len:  0,
			want: 0.0,
		},
		{
			len:  4,
			want: 3.0,
		},
		{
			len:  5,
			want: 4.0,
		},
		{
			len:  1000,
			want: 999,
		},
	}

	for _, c := range cases {
		sg := NewStatGroup(c.len)
		for i := uint64(0); i < c.len; i++ {
			sg.Push(float64(i) * 2)
		}
		if got := sg.Median(); c.want != got {
			t.Errorf("got: %v want: %v\n", got, c.want)
		}
	}
}

func TestMedian0InitialSize(t *testing.T) {
	cases := []struct {
		len  uint64
		want float64
	}{
		{
			len:  0,
			want: 0.0,
		},
		{
			len:  4,
			want: 3.0,
		},
		{
			len:  5,
			want: 4.0,
		},
		{
			len:  1000,
			want: 999,
		},
	}

	for _, c := range cases {
		sg := NewStatGroup(0)
		for i := uint64(0); i < c.len; i++ {
			sg.Push(float64(i) * 2)
		}
		if got := sg.Median(); c.want != got {
			t.Errorf("got: %v want: %v\n", got, c.want)
		}
	}
}
