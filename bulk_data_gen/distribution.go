package main

import (
	"math"
	"math/rand"
)

// Distribution provides an interface to model a statistical distribution.
type Distribution interface {
	Advance()
	Get() float64 // should be idempotent
}

// NormalDistribution models a normal distribution.
type NormalDistribution struct {
	Mean   float64
	StdDev float64

	value float64
}

// Advance advances this distribution. Since a normal distribution is
// stateless, this is just overwrites the internal cache value.
func (d *NormalDistribution) Advance() {
	d.value = rand.NormFloat64()*d.StdDev + d.Mean
}

// Get returns the last computed value for this distribution.
func (d *NormalDistribution) Get() float64 {
	return d.value
}

// UniformDistribution models a uniform distribution.
type UniformDistribution struct {
	Low  float64
	High float64

	value float64
}

// Advance advances this distribution. Since a uniform distribution is
// stateless, this is just overwrites the internal cache value.
func (d *UniformDistribution) Advance() {
	x := rand.Float64() // uniform
	x *= d.High - d.Low
	x += d.Low
	d.value = x
}

// Get computes and returns the next value in the distribution.
func (d *UniformDistribution) Get() float64 {
	return d.value
}

// RandomWalkDistribution is a stateful random walk. Initialize it with an
// underlying distribution, which is used to compute the new step value.
type RandomWalkDistribution struct {
	Step Distribution

	State float64 // optional
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

func (d *MonotonicRandomWalkDistribution) Get() float64 {
	return d.State
}

type ConstantDistribution struct {
	State float64
}

func (d *ConstantDistribution) Advacne() {
}

func (d *ConstantDistribution) Get() float64 {
	return d.State
}
