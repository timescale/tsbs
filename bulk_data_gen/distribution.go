package main

import "math/rand"

// Distribution provides an interface to model a statistical distribution.
type Distribution interface {
	Advance()
	Get() float64
}

// NormalDistribution models a normal distribution.
type NormalDistribution struct {
	Mean   float64
	StdDev float64
}

// Advance advances this distribution. Since a normal distribution is
// stateless, this is a no-op.
func (d *NormalDistribution) Advance() {
	return
}

func (d *NormalDistribution) Get() float64 {
	return rand.NormFloat64()*d.StdDev + d.Mean
}

// UniformDistribution models a uniform distribution.
type UniformDistribution struct {
	Low  float64
	High float64
}

// Advance advances this distribution. Since a uniform distribution is
// stateless, this is a no-op.
func (d *UniformDistribution) Advance() {
	return
}

func (d *UniformDistribution) Get() float64 {
	x := rand.Float64() // uniform
	x *= d.High - d.Low
	x += d.Low
	return x
}

type RandomWalkDistribution struct {
	State float64
	Step  Distribution
}

func (d *RandomWalkDistribution) Advance() {
	d.Step.Advance()
	d.State += d.Step.Get()
}

func (d *RandomWalkDistribution) Get() float64 {
	return d.State
}

type ClampedRandomWalkDistribution struct {
	State float64
	Step  Distribution
	Min   float64
	Max   float64
}

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

func (d *ClampedRandomWalkDistribution) Get() float64 {
	return d.State
}
