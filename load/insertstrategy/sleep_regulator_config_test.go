package insertstrategy

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func TestMakeRandsForWorkers(t *testing.T) {
	initialRand1 := rand.New(rand.NewSource(1))
	rands1 := makeRandsForWorkers(1, initialRand1)
	if len(rands1) != 1 {
		t.Errorf("Expected %d rands, got %d", 1, len(rands1))
	}
	initialRand1 = rand.New(rand.NewSource(1))
	rands2 := makeRandsForWorkers(2, initialRand1)
	if len(rands2) != 2 {
		t.Errorf("Expected %d rands, got %d", 1, len(rands1))
	}

	if x, y := rands1[0].Int63(), rands2[0].Int63(); x != y {
		t.Errorf("Expected rands to generate the same numbers, got: %d and %d", x, y)
	}
}

func TestSplitIntervalString(t *testing.T) {
	testCases := []struct {
		in  string
		out []string
	}{
		{in: "", out: []string{"0"}},
		{in: "1,2,asd,3,1-2", out: []string{"1", "2", "asd", "3", "1-2"}},
	}

	for _, tc := range testCases {
		res := splitIntervalString(tc.in)
		if !reflect.DeepEqual(tc.out, res) {
			t.Errorf("expected: %v; got: %v", tc.out, res)
		}
	}
}

func TestNewConstantSleepTimeGenerator(t *testing.T) {
	testCases := []struct {
		maxSeconds       int
		expectedDuration time.Duration
	}{
		{maxSeconds: 1, expectedDuration: time.Second},
		{maxSeconds: 2, expectedDuration: 2 * time.Second},
		{maxSeconds: 123, expectedDuration: 123 * time.Second},
	}

	for _, tc := range testCases {
		genFn := newConstantSleepTimeGenerator(tc.maxSeconds)
		first := genFn()
		second := genFn()
		if first != second {
			t.Errorf("generator is not constant. First: %v; Second: %v", first, second)
			continue
		}
		if first != tc.expectedDuration {
			t.Errorf("expected: %v; got: %v", tc.expectedDuration, first)
		}
	}
}

func TestNewRangeSleepTimeGenerator(t *testing.T) {
	testCases := []struct {
		minSeconds int
		maxSeconds int
	}{
		{maxSeconds: 1, minSeconds: 0},
		{maxSeconds: 4, minSeconds: 1},
		{maxSeconds: 123, minSeconds: 50},
	}

	rnd := rand.New(rand.NewSource(1))
	for _, tc := range testCases {
		desc := fmt.Sprintf("test range generator between %d and %d", tc.minSeconds, tc.maxSeconds)
		t.Run(desc, func(t *testing.T) {
			genFn := newRangeSleepTimeGenerator(tc.minSeconds, tc.maxSeconds, rnd)
			minDur := time.Second * time.Duration(tc.minSeconds)
			maxDur := time.Second * time.Duration(tc.maxSeconds)
			for i := 0; i < 1000; i++ {
				res := genFn()
				if res < minDur || res >= maxDur {
					t.Errorf("unexpected value. must be in [%v,%v) got: %v", minDur, maxDur, res)
					break
				}
			}
		})

	}
}

func TestAttemptRangeParse(t *testing.T) {
	testCases := []struct {
		desc      string
		in        string
		expectErr bool
		out       []int
	}{
		{
			desc:      "more than one -",
			in:        "1-2-3",
			expectErr: true,
		}, {
			desc:      "lower limit not an int",
			in:        "a-1",
			expectErr: true,
		}, {
			desc:      "upper limit not an int",
			in:        "1-1.2",
			expectErr: true,
		}, {
			desc:      "upper limit < lower",
			in:        "2-1",
			expectErr: true,
		}, {
			desc: "all good",
			in:   "123-143",
			out:  []int{123, 143},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			got, err := attemptRangeParse(tc.in)
			if err != nil && !tc.expectErr {
				t.Errorf("unexpected error: \n%v", err)
			} else if err == nil && tc.expectErr {
				t.Error("unexpected lack of error")
			}
			if tc.expectErr {
				return
			}
			if !reflect.DeepEqual(got, tc.out) {
				t.Errorf("expected %v, got %v", tc.out, got)
			}
		})
	}
}

func TestParseSingleIntervalString(t *testing.T) {
	seedToUse := int64(1)
	randForExpected := rand.New(rand.NewSource(seedToUse))
	firstExpected := time.Duration(2+randForExpected.Intn(8)) * time.Second
	secondExpected := time.Duration(2+randForExpected.Intn(8)) * time.Second
	testCases := []struct {
		desc      string
		in        string
		out       []time.Duration
		expectErr bool
	}{
		{
			desc:      "error on parsing non-integer constant",
			in:        "a",
			expectErr: true,
		}, {
			desc:      "error on parsing non-integer range",
			in:        "1-a",
			expectErr: true,
		}, {
			desc: "return same constant for two invocations",
			in:   "1",
			out:  []time.Duration{time.Second, time.Second},
		}, {
			desc: "return two expected values from random bounded interval",
			in:   "2-10",
			out:  []time.Duration{firstExpected, secondExpected},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			randForWorker := rand.New(rand.NewSource(seedToUse))
			res, err := parseSingleIntervalString(tc.in, randForWorker)
			if tc.expectErr && err == nil {
				t.Error("unexpected lack of error")
			} else if !tc.expectErr && err != nil {
				t.Errorf("unexpected error:\n%v", err)
			}

			if tc.expectErr {
				return
			}

			gotFirst := res()
			gotSecond := res()
			got := []time.Duration{gotFirst, gotSecond}
			if !reflect.DeepEqual(got, tc.out) {
				t.Errorf("expected: %v; got: %v", tc.out, got)
			}
		})
	}
}
