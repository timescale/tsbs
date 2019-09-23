package common

import (
	"math"
	"testing"
)

type mockDistribution struct {
	AdvanceCalled bool
	ReturnValue   float64
}

func (m *mockDistribution) Advance() {
	m.AdvanceCalled = true
}

func (m *mockDistribution) Get() float64 {
	return m.ReturnValue
}

func TestFloatPrecisionAdvance(t *testing.T) {

	dist := &mockDistribution{}

	fp := FP(dist, 1)

	fp.Advance()

	if !dist.AdvanceCalled {
		t.Errorf("FloatPrecision Advance call did not call underlying distribution Advance method")
	}
}

func TestFloatPrecisionGet(t *testing.T) {
	testCases := []struct {
		value   float64
		results map[int]float64
	}{
		{
			value: 1.234567890,
			results: map[int]float64{
				-1: 1,
				0:  1,
				1:  1.2,
				2:  1.23,
				3:  1.234,
				4:  1.2345,
				5:  1.23456,
				6:  1.23456,
				7:  1.23456,
				8:  1.23456,
				9:  1.23456,
			},
		},
		{
			value: 1.0,
			results: map[int]float64{
				-1: 1,
				0:  1,
				1:  1.0,
				2:  1.00,
				3:  1.000,
				4:  1.0000,
				5:  1.00000,
				6:  1.00000,
				7:  1.00000,
				8:  1.00000,
				9:  1.00000,
			},
		},
		{
			value: 0.0,
			results: map[int]float64{
				-1: 0,
				0:  0,
				1:  0.0,
				2:  0.00,
				3:  0.000,
				4:  0.0000,
				5:  0.00000,
				6:  0.00000,
				7:  0.00000,
				8:  0.00000,
				9:  0.00000,
			},
		},
	}

	for _, testCase := range testCases {
		for precision, want := range testCase.results {
			fp := FP(&mockDistribution{ReturnValue: testCase.value}, precision)

			if got := fp.Get(); got != want {
				t.Errorf("wrong result for value %f and precision %d, got %f want %f (diff %f)", testCase.value, precision, got, want, math.Abs(got-want))
			}
		}
	}
}
