package common

import (
	"math"
	"math/rand"
)

// Distribution provides an interface to model a statistical distribution.
type Distribution interface {
	Advance()
	Get() float64 // should be idempotent
}

// NormalDistribution models a normal distribution (stateless).
type NormalDistribution struct {
	Mean   float64
	StdDev float64

	value float64
}

// ND creates a new normal distribution with the given mean/stddev
func ND(mean, stddev float64) *NormalDistribution {
	return &NormalDistribution{
		Mean:   mean,
		StdDev: stddev,
	}
}

// Advance advances this distribution. Since the distribution is
// stateless, this just overwrites the internal cache value.
func (d *NormalDistribution) Advance() {
	d.value = rand.NormFloat64()*d.StdDev + d.Mean
}

// Get returns the last computed value for this distribution.
func (d *NormalDistribution) Get() float64 {
	return d.value
}

// UniformDistribution models a uniform distribution (stateless).
type UniformDistribution struct {
	Low  float64
	High float64

	value float64
}

// UD creates a new uniform distribution with the given range
func UD(low, high float64) *UniformDistribution {
	return &UniformDistribution{
		Low:  low,
		High: high,
	}
}

// Advance advances this distribution. Since the distribution is
// stateless, this just overwrites the internal cache value.
func (d *UniformDistribution) Advance() {
	x := rand.Float64() // uniform
	x *= d.High - d.Low
	x += d.Low
	d.value = x
}

// Get returns the last computed value for this distribution.
func (d *UniformDistribution) Get() float64 {
	return d.value
}

// RandomWalkDistribution is a stateful random walk. Initialize it with an
// underlying distribution, which is used to compute the new step value.
type RandomWalkDistribution struct {
	Step Distribution

	State float64 // optional
}

// WD creates a new RandomWalkDistribution based on a given distribution and starting state
func WD(step Distribution, state float64) *RandomWalkDistribution {
	return &RandomWalkDistribution{
		Step:  step,
		State: state,
	}
}

// Advance computes the next value of this distribution and stores it.
func (d *RandomWalkDistribution) Advance() {
	d.Step.Advance()
	d.State += d.Step.Get()
}

// Get returns the last computed value for this distribution.
func (d *RandomWalkDistribution) Get() float64 {
	return d.State
}

// ClampedRandomWalkDistribution is a stateful random walk, with minimum and
// maximum bounds. Initialize it with a Min, Max, and an underlying
// distribution, which is used to compute the new step value.
type ClampedRandomWalkDistribution struct {
	Step Distribution
	Min  float64
	Max  float64

	State float64 // optional
}

// CWD returns a new ClampedRandomWalkDistribution based on a given distribution and optional starting state
func CWD(step Distribution, min, max, state float64) *ClampedRandomWalkDistribution {
	return &ClampedRandomWalkDistribution{
		Step: step,
		Min:  min,
		Max:  max,

		State: state,
	}
}

// Advance computes the next value of this distribution and stores it.
func (d *ClampedRandomWalkDistribution) Advance() {
	d.Step.Advance()
	d.State += d.Step.Get()
	if d.State > d.Max {
		d.State = d.Max
	}
	if d.State < d.Min {
		d.State = d.Min
	}
}

// Get returns the last computed value for this distribution.
func (d *ClampedRandomWalkDistribution) Get() float64 {
	return d.State
}

// MonotonicRandomWalkDistribution is a stateful random walk that only
// increases. Initialize it with a Start and an underlying distribution,
// which is used to compute the new step value. The sign of any value of the
// u.d. is always made positive.
type MonotonicRandomWalkDistribution struct {
	Step  Distribution
	State float64
}

// Advance computes the next value of this distribution and stores it.
func (d *MonotonicRandomWalkDistribution) Advance() {
	d.Step.Advance()
	d.State += math.Abs(d.Step.Get())
}

// Get returns the last computed value for this distribution.
func (d *MonotonicRandomWalkDistribution) Get() float64 {
	return d.State
}

// MWD creates a new MonotonicRandomWalkDistribution with a given distribution and initial state
func MWD(step Distribution, state float64) *MonotonicRandomWalkDistribution {
	return &MonotonicRandomWalkDistribution{
		Step:  step,
		State: state,
	}
}

// ConstantDistribution is a stateful distribution that always returns the same value
type ConstantDistribution struct {
	State float64
}

// Advance does nothing in a constant distribution
func (d *ConstantDistribution) Advance() {
}

// Get returns the last computed value for this distribution.
func (d *ConstantDistribution) Get() float64 {
	return d.State
}

// FloatPrecision is a distribution wrapper which specifies the float value precision of the underlying distribution.
type FloatPrecision struct {
	step      Distribution
	precision float64
}

// Advance calls the underlying distribution Advance method.
func (f *FloatPrecision) Advance() {
	f.step.Advance()
}

// Get returns the value from the underlying distribution with adjusted float value precision.
func (f *FloatPrecision) Get() float64 {
	return float64(int(f.step.Get()*f.precision)) / f.precision
}

// FP creates a new FloatPrecision distribution wrapper with a given distribution and precision value.
// Precision value is clamped to [0,5] to avoid floating point calculation errors.
func FP(step Distribution, precision int) *FloatPrecision {
	// Clamping the precision value to spec.
	if precision < 0 {
		precision = 0
	} else if precision > 5 {
		precision = 5
	}
	return &FloatPrecision{
		step:      step,
		precision: math.Pow(10, float64(precision)),
	}
}
